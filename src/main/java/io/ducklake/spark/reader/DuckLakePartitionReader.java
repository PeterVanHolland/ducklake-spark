package io.ducklake.spark.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.*;

/**
 * Reads Parquet data files for a single DuckLake partition.
 * Handles schema evolution by mapping columns via field_id (not name/position):
 * <ul>
 *   <li>Column renames: matched by field_id embedded in Parquet metadata</li>
 *   <li>Added columns: filled with initialDefault from catalog</li>
 *   <li>Dropped columns: ignored (only current schema columns are read)</li>
 * </ul>
 */
public class DuckLakePartitionReader implements PartitionReader<InternalRow> {

    private final DuckLakeInputPartition partition;
    private final StructType requiredSchema;
    private final StructType fullSchema;

    private ParquetFileReader fileReader;
    private MessageType fileSchema;
    private long currentRowIndex = 0;
    private final Queue<InternalRow> rowBuffer = new LinkedList<>();
    private InternalRow currentRow;
    private final Set<Long> deletedRowIds;

    // Schema evolution: per-output-column mappings (built once at init)
    private int[] outputToFileIndex;    // requiredSchema col index -> file column index (-1 = not in file)
    private Object[] defaultValues;     // requiredSchema col index -> default value for missing columns

    public DuckLakePartitionReader(DuckLakeInputPartition partition,
                                    StructType requiredSchema,
                                    StructType fullSchema) {
        this.partition = partition;
        this.requiredSchema = requiredSchema;
        this.fullSchema = fullSchema;
        this.deletedRowIds = loadDeleteFiles(partition.getDeleteFilePaths());
        initReader();
    }

    private void initReader() {
        try {
            Configuration conf = new Configuration();
            Path filePath = new Path(partition.getFilePath());
            fileReader = ParquetFileReader.open(conf, filePath);
            fileSchema = fileReader.getFooter().getFileMetaData().getSchema();

            buildColumnMapping();
        } catch (IOException e) {
            throw new RuntimeException("Failed to open Parquet file: " + partition.getFilePath(), e);
        }
    }

    /**
     * Build the mapping from output columns (requiredSchema) to file columns
     * using field_id-based matching. Falls back to name-based matching when
     * field_ids are not present in the Parquet file.
     */
    private void buildColumnMapping() {
        int outputCount = requiredSchema.fields().length;
        outputToFileIndex = new int[outputCount];
        defaultValues = new Object[outputCount];

        Map<String, Long> nameToColumnId = partition.getNameToColumnId();
        Map<Long, String> columnDefaults = partition.getColumnDefaults();
        Map<Long, String> nameMapping = partition.getNameMapping();

        // Build field_id -> file column index from Parquet file schema
        Map<Long, Integer> fileFieldIdToIndex = new HashMap<>();
        Map<String, Integer> fileNameToIndex = new HashMap<>();
        for (int i = 0; i < fileSchema.getFieldCount(); i++) {
            org.apache.parquet.schema.Type fieldType = fileSchema.getType(i);
            fileNameToIndex.put(fieldType.getName(), i);
            if (fieldType.getId() != null) {
                fileFieldIdToIndex.put((long) fieldType.getId().intValue(), i);
            }
        }

        for (int i = 0; i < outputCount; i++) {
            String colName = requiredSchema.fields()[i].name();
            DataType sparkType = requiredSchema.fields()[i].dataType();
            outputToFileIndex[i] = -1;  // default: not found in file

            Long columnId = (nameToColumnId != null) ? nameToColumnId.get(colName) : null;

            if (columnId != null) {
                // Strategy 1: Match by field_id (handles renames correctly)
                if (fileFieldIdToIndex.containsKey(columnId)) {
                    outputToFileIndex[i] = fileFieldIdToIndex.get(columnId);
                }
                // Strategy 2: Fall back to name mapping (for files with mapping_id)
                else if (nameMapping != null && nameMapping.containsKey(columnId)) {
                    String physicalName = nameMapping.get(columnId);
                    if (fileNameToIndex.containsKey(physicalName)) {
                        outputToFileIndex[i] = fileNameToIndex.get(physicalName);
                    }
                }
                // Strategy 3: Fall back to name-based match
                else if (fileNameToIndex.containsKey(colName)) {
                    outputToFileIndex[i] = fileNameToIndex.get(colName);
                }

                // If still not found, set the default value
                if (outputToFileIndex[i] == -1) {
                    String defaultStr = (columnDefaults != null) ? columnDefaults.get(columnId) : null;
                    defaultValues[i] = parseDefault(defaultStr, sparkType);
                }
            } else {
                // No column info -- try direct name match
                if (fileNameToIndex.containsKey(colName)) {
                    outputToFileIndex[i] = fileNameToIndex.get(colName);
                }
            }
        }
    }

    @Override
    public boolean next() throws IOException {
        while (true) {
            if (!rowBuffer.isEmpty()) {
                currentRow = rowBuffer.poll();
                return true;
            }

            PageReadStore pages = fileReader.readNextRowGroup();
            if (pages == null) {
                return false;
            }

            long rowCount = pages.getRowCount();

            ColumnIOFactory factory = new ColumnIOFactory();
            MessageColumnIO columnIO = factory.getColumnIO(fileSchema);
            RecordReader<Group> recordReader =
                    columnIO.getRecordReader(pages, new GroupRecordConverter(fileSchema));

            for (long i = 0; i < rowCount; i++) {
                Group group = recordReader.read();
                long rowId = currentRowIndex++;

                if (deletedRowIds.contains(rowId)) {
                    continue;
                }

                InternalRow row = groupToInternalRow(group);
                rowBuffer.add(row);
            }

            if (!rowBuffer.isEmpty()) {
                currentRow = rowBuffer.poll();
                return true;
            }
        }
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        if (fileReader != null) {
            fileReader.close();
        }
    }

    // ---------------------------------------------------------------
    // Delete file handling
    // ---------------------------------------------------------------

    private Set<Long> loadDeleteFiles(String[] deleteFilePaths) {
        Set<Long> deleted = new HashSet<>();
        if (deleteFilePaths == null || deleteFilePaths.length == 0) {
            return deleted;
        }
        for (String path : deleteFilePaths) {
            try {
                Configuration conf = new Configuration();
                ParquetFileReader reader = ParquetFileReader.open(conf, new Path(path));
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    ColumnIOFactory factory = new ColumnIOFactory();
                    MessageColumnIO columnIO = factory.getColumnIO(schema);
                    RecordReader<Group> recordReader =
                            columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (long i = 0; i < pages.getRowCount(); i++) {
                        Group group = recordReader.read();
                        int fieldIndex = schema.getFieldIndex("row_id");
                        long rowId = group.getLong(fieldIndex, 0);
                        deleted.add(rowId);
                    }
                }
                reader.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to read delete file: " + path, e);
            }
        }
        return deleted;
    }

    // ---------------------------------------------------------------
    // Parquet Group -> InternalRow (field_id-based mapping)
    // ---------------------------------------------------------------

    private InternalRow groupToInternalRow(Group group) {
        Object[] values = new Object[requiredSchema.fields().length];
        for (int i = 0; i < requiredSchema.fields().length; i++) {
            int fileIndex = outputToFileIndex[i];
            DataType sparkType = requiredSchema.fields()[i].dataType();

            if (fileIndex < 0) {
                // Column not in file -- use default value
                values[i] = defaultValues[i];
            } else {
                try {
                    if (group.getFieldRepetitionCount(fileIndex) == 0) {
                        values[i] = null;
                    } else {
                        values[i] = readValue(group, fileIndex, sparkType);
                    }
                } catch (Exception e) {
                    values[i] = null;
                }
            }
        }
        return new GenericInternalRow(values);
    }

    private Object readValue(Group group, int fieldIndex, DataType sparkType) {
        if (sparkType instanceof BooleanType) {
            return group.getBoolean(fieldIndex, 0);
        } else if (sparkType instanceof ByteType) {
            return (byte) group.getInteger(fieldIndex, 0);
        } else if (sparkType instanceof ShortType) {
            return (short) group.getInteger(fieldIndex, 0);
        } else if (sparkType instanceof IntegerType) {
            return group.getInteger(fieldIndex, 0);
        } else if (sparkType instanceof LongType) {
            return group.getLong(fieldIndex, 0);
        } else if (sparkType instanceof FloatType) {
            return group.getFloat(fieldIndex, 0);
        } else if (sparkType instanceof DoubleType) {
            return group.getDouble(fieldIndex, 0);
        } else if (sparkType instanceof StringType) {
            return UTF8String.fromString(group.getString(fieldIndex, 0));
        } else if (sparkType instanceof BinaryType) {
            return group.getBinary(fieldIndex, 0).getBytes();
        } else {
            return UTF8String.fromString(group.getValueToString(fieldIndex, 0));
        }
    }

    // ---------------------------------------------------------------
    // Default value parsing
    // ---------------------------------------------------------------

    /**
     * Parse a SQL default value literal into a Java object suitable for InternalRow.
     * Handles common types: integers, floats, booleans, strings, NULL.
     */
    static Object parseDefault(String defaultStr, DataType sparkType) {
        if (defaultStr == null) {
            return null;
        }
        String trimmed = defaultStr.trim();
        if (trimmed.isEmpty() || trimmed.equalsIgnoreCase("NULL")) {
            return null;
        }

        try {
            if (sparkType instanceof BooleanType) {
                return Boolean.parseBoolean(trimmed);
            } else if (sparkType instanceof ByteType) {
                return Byte.parseByte(trimmed);
            } else if (sparkType instanceof ShortType) {
                return Short.parseShort(trimmed);
            } else if (sparkType instanceof IntegerType) {
                return Integer.parseInt(trimmed);
            } else if (sparkType instanceof LongType) {
                return Long.parseLong(trimmed);
            } else if (sparkType instanceof FloatType) {
                return Float.parseFloat(trimmed);
            } else if (sparkType instanceof DoubleType) {
                return Double.parseDouble(trimmed);
            } else if (sparkType instanceof StringType) {
                // Strip SQL string quotes: 'hello' -> hello
                if (trimmed.startsWith("'") && trimmed.endsWith("'") && trimmed.length() >= 2) {
                    return UTF8String.fromString(trimmed.substring(1, trimmed.length() - 1));
                }
                return UTF8String.fromString(trimmed);
            } else if (sparkType instanceof BinaryType) {
                return trimmed.getBytes();
            }
        } catch (NumberFormatException e) {
            // Fall through to null
        }
        return null;
    }
}
