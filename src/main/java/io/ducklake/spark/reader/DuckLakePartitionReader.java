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
 * Reads Parquet data files for a single DuckLake partition,
 * applying delete files and column rename mappings.
 */
public class DuckLakePartitionReader implements PartitionReader<InternalRow> {

    private final DuckLakeInputPartition partition;
    private final StructType requiredSchema;
    private final StructType fullSchema;

    private ParquetFileReader fileReader;
    private long currentRowIndex = 0;
    private final Queue<InternalRow> rowBuffer = new LinkedList<>();
    private InternalRow currentRow;
    private final Set<Long> deletedRowIds;

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
        } catch (IOException e) {
            throw new RuntimeException("Failed to open Parquet file: " + partition.getFilePath(), e);
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
            MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();

            ColumnIOFactory factory = new ColumnIOFactory();
            MessageColumnIO columnIO = factory.getColumnIO(fileSchema);
            RecordReader<Group> recordReader =
                    columnIO.getRecordReader(pages, new GroupRecordConverter(fileSchema));

            Map<String, String> physicalToLogical = buildPhysicalToLogicalMapping();

            for (long i = 0; i < rowCount; i++) {
                Group group = recordReader.read();
                long rowId = currentRowIndex++;

                if (deletedRowIds.contains(rowId)) {
                    continue;
                }

                InternalRow row = groupToInternalRow(group, fileSchema, physicalToLogical);
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
    // Column rename mapping
    // ---------------------------------------------------------------

    private Map<String, String> buildPhysicalToLogicalMapping() {
        Map<String, String> mapping = new HashMap<>();
        Map<Long, String> nameMap = partition.getNameMapping();
        Map<Long, String> colIdToName = partition.getColIdToName();

        if (nameMap != null && colIdToName != null) {
            for (Map.Entry<Long, String> entry : nameMap.entrySet()) {
                long fieldId = entry.getKey();
                String physicalName = entry.getValue();
                String logicalName = colIdToName.get(fieldId);
                if (logicalName != null && !physicalName.equals(logicalName)) {
                    mapping.put(physicalName, logicalName);
                }
            }
        }
        return mapping;
    }

    // ---------------------------------------------------------------
    // Parquet Group → InternalRow conversion
    // ---------------------------------------------------------------

    private InternalRow groupToInternalRow(Group group, MessageType fileSchema,
                                            Map<String, String> physicalToLogical) {
        Object[] values = new Object[requiredSchema.fields().length];
        for (int i = 0; i < requiredSchema.fields().length; i++) {
            String sparkColName = requiredSchema.fields()[i].name();
            DataType sparkType = requiredSchema.fields()[i].dataType();

            String physicalName = sparkColName;
            for (Map.Entry<String, String> entry : physicalToLogical.entrySet()) {
                if (entry.getValue().equals(sparkColName)) {
                    physicalName = entry.getKey();
                    break;
                }
            }

            try {
                int fieldIndex = fileSchema.getFieldIndex(physicalName);
                if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                    values[i] = null;
                } else {
                    values[i] = readValue(group, fieldIndex, sparkType);
                }
            } catch (Exception e) {
                values[i] = null;
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
}
