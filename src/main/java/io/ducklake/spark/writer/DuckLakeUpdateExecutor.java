package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.util.DuckLakeTypeMapping;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.*;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

/**
 * Executes row-level UPDATE operations on DuckLake tables.
 *
 * For each matching row:
 *   1. Records its position in a delete file (marks old version as deleted)
 *   2. Writes updated row to a new data file (inserts new version)
 *
 * This is the standard copy-on-write approach for DuckLake updates.
 */
public class DuckLakeUpdateExecutor {

    private final String catalogPath;
    private final String dataPathOption;
    private final String tableName;
    private final String schemaName;

    public DuckLakeUpdateExecutor(String catalogPath, String dataPathOption,
                                   String tableName, String schemaName) {
        this.catalogPath = catalogPath;
        this.dataPathOption = dataPathOption;
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    /**
     * Update rows matching the filters, setting column values from the updates map.
     *
     * @param filters WHERE clause predicates
     * @param updates Map from column name to new value (Java objects matching Spark types)
     */
    public void updateWhere(Filter[] filters, Map<String, Object> updates) {
        try (DuckLakeMetadataBackend backend = createBackend()) {
            backend.beginTransaction();
            try {
                long currentSnap = backend.getCurrentSnapshotId();

                TableInfo table = backend.getTable(tableName, schemaName, currentSnap);
                if (table == null) {
                    throw new RuntimeException("Table not found: " + schemaName + "." + tableName);
                }

                String dataPath = backend.getDataPath();
                if (!dataPath.endsWith("/")) dataPath += "/";

                List<DataFileInfo> dataFiles = backend.getDataFiles(table.tableId, currentSnap);
                List<ColumnInfo> columns = backend.getColumns(table.tableId, currentSnap);

                StructType sparkSchema = DuckLakeTypeMapping.buildSchema(columns);

                Map<Long, String> colIdToName = new HashMap<>();
                for (ColumnInfo col : columns) {
                    colIdToName.put(col.columnId, col.name);
                }

                // Map column name -> column id
                Map<String, Long> nameToColId = new HashMap<>();
                for (ColumnInfo col : columns) {
                    nameToColId.put(col.name, col.columnId);
                }

                long[] columnIds = new long[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    columnIds[i] = columns.get(i).columnId;
                }

                CatalogState snapInfo = backend.getSnapshotInfo(currentSnap);
                long nextFileId = snapInfo.nextFileId;

                List<DuckLakeDeleteExecutor.DeleteFileResult> deleteResults = new ArrayList<>();
                List<UpdateDataFileResult> newDataFiles = new ArrayList<>();

                for (DataFileInfo dataFile : dataFiles) {
                    String filePath = dataFile.pathIsRelative ? dataPath + dataFile.path : dataFile.path;

                    Set<Long> existingDeletes = loadExistingDeletes(
                            backend, table.tableId, dataFile.dataFileId, currentSnap, dataPath);

                    Map<String, String> logicalToPhysical = buildLogicalToPhysical(
                            backend, dataFile.mappingId, colIdToName);

                    // Scan file: collect delete positions and updated rows
                    ScanResult scanResult = scanAndCollectUpdates(
                            filePath, sparkSchema, filters, existingDeletes,
                            logicalToPhysical, updates, columnIds);

                    if (!scanResult.deletePositions.isEmpty()) {
                        // Write delete file
                        String deleteFileName = "delete_" + UUID.randomUUID() + ".parquet";
                        String deleteRelPath = table.path + deleteFileName;
                        String deleteAbsPath = dataPath + deleteRelPath;

                        DuckLakeDeleteExecutor.writeDeleteFile(deleteAbsPath, scanResult.deletePositions);

                        long deleteFileSize = new File(deleteAbsPath).length();
                        deleteResults.add(new DuckLakeDeleteExecutor.DeleteFileResult(
                                dataFile.dataFileId, deleteRelPath,
                                scanResult.deletePositions.size(), deleteFileSize));

                        // Write new data file with updated rows
                        String newFileName = "ducklake-" + UUID.randomUUID() + ".parquet";
                        String newRelPath = table.path + newFileName;
                        String newAbsPath = dataPath + newRelPath;

                        writeUpdatedRows(newAbsPath, sparkSchema, columnIds,
                                scanResult.updatedRows);

                        long newFileSize = new File(newAbsPath).length();
                        newDataFiles.add(new UpdateDataFileResult(
                                newRelPath, scanResult.updatedRows.size(), newFileSize,
                                scanResult.columnStats));
                    }
                }

                if (deleteResults.isEmpty()) {
                    backend.rollbackTransaction();
                    return;
                }

                // Create new snapshot
                long newSnap = currentSnap + 1;
                long totalNewFiles = deleteResults.size() + newDataFiles.size();
                long newNextFileId = nextFileId + totalNewFiles;
                backend.createSnapshot(newSnap, snapInfo.schemaVersion,
                        snapInfo.nextCatalogId, newNextFileId);

                // Register delete files
                long fileId = nextFileId;
                long totalDeleteCount = 0;
                for (DuckLakeDeleteExecutor.DeleteFileResult result : deleteResults) {
                    backend.insertDeleteFile(fileId, table.tableId, newSnap,
                            result.dataFileId, result.relativePath, result.deleteCount, result.fileSize);
                    totalDeleteCount += result.deleteCount;
                    fileId++;
                }

                // Register new data files
                TableStats stats = backend.getTableStats(table.tableId);
                long rowIdStart = stats.nextRowId;
                long totalNewRecords = 0;
                long totalNewFileSize = 0;
                int fileOrder = 0;

                for (UpdateDataFileResult ndf : newDataFiles) {
                    backend.insertDataFile(fileId, table.tableId, newSnap, fileOrder,
                            ndf.relativePath, ndf.recordCount, ndf.fileSize, rowIdStart);

                    // Insert column stats
                    for (DuckLakeWriterCommitMessage.ColumnStats cs : ndf.columnStats) {
                        backend.insertColumnStats(fileId, table.tableId, cs.columnId,
                                cs.valueCount, cs.nullCount, cs.minValue, cs.maxValue);
                    }

                    rowIdStart += ndf.recordCount;
                    totalNewRecords += ndf.recordCount;
                    totalNewFileSize += ndf.fileSize;
                    fileId++;
                    fileOrder++;
                }

                // Update table stats: subtract deleted, add new
                long newRecordCount = stats.recordCount - totalDeleteCount + totalNewRecords;
                backend.updateTableStats(table.tableId, newRecordCount, rowIdStart,
                        stats.fileSizeBytes + totalNewFileSize);

                backend.insertSnapshotChanges(newSnap,
                        "deleted_from_table:" + table.tableId + ",inserted_into_table:" + table.tableId,
                        "ducklake-spark", "Spark update");

                backend.commitTransaction();
            } catch (Exception e) {
                try { backend.rollbackTransaction(); } catch (SQLException ex) { e.addSuppressed(ex); }
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException("Failed to execute update", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute update", e);
        }
    }

    // ---------------------------------------------------------------
    // Scanning, updating, and writing
    // ---------------------------------------------------------------

    private ScanResult scanAndCollectUpdates(String filePath, StructType sparkSchema,
                                              Filter[] filters, Set<Long> existingDeletes,
                                              Map<String, String> logicalToPhysical,
                                              Map<String, Object> updates,
                                              long[] columnIds) {
        List<Long> deletePositions = new ArrayList<>();
        List<Object[]> updatedRows = new ArrayList<>();

        // Initialize stats accumulators
        StructField[] fields = sparkSchema.fields();
        Comparable<?>[] mins = new Comparable<?>[fields.length];
        Comparable<?>[] maxs = new Comparable<?>[fields.length];
        long[] nullCounts = new long[fields.length];
        long[] valueCounts = new long[fields.length];

        try (ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(filePath))) {
            MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
            DuckLakeRowFilterEvaluator evaluator =
                    new DuckLakeRowFilterEvaluator(sparkSchema, fileSchema, logicalToPhysical);

            long rowPosition = 0;
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rowCount = pages.getRowCount();
                ColumnIOFactory factory = new ColumnIOFactory();
                MessageColumnIO columnIO = factory.getColumnIO(fileSchema);
                RecordReader<Group> recordReader =
                        columnIO.getRecordReader(pages, new GroupRecordConverter(fileSchema));

                for (long i = 0; i < rowCount; i++) {
                    Group group = recordReader.read();
                    long pos = rowPosition++;

                    if (existingDeletes.contains(pos)) {
                        continue;
                    }

                    if (evaluator.matches(group, filters)) {
                        deletePositions.add(pos);

                        // Build updated row
                        Object[] rowValues = new Object[fields.length];
                        for (int c = 0; c < fields.length; c++) {
                            String colName = fields[c].name();
                            if (updates.containsKey(colName)) {
                                rowValues[c] = updates.get(colName);
                            } else {
                                rowValues[c] = readGroupValue(group, fileSchema,
                                        logicalToPhysical, colName, fields[c].dataType());
                            }

                            // Track stats
                            trackStats(rowValues[c], c, mins, maxs, nullCounts, valueCounts);
                        }
                        updatedRows.add(rowValues);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan file for update: " + filePath, e);
        }

        // Build column stats
        List<DuckLakeWriterCommitMessage.ColumnStats> colStats = new ArrayList<>();
        for (int c = 0; c < fields.length; c++) {
            String minStr = mins[c] != null ? mins[c].toString() : null;
            String maxStr = maxs[c] != null ? maxs[c].toString() : null;
            colStats.add(new DuckLakeWriterCommitMessage.ColumnStats(
                    columnIds[c], minStr, maxStr, nullCounts[c], valueCounts[c]));
        }

        return new ScanResult(deletePositions, updatedRows, colStats);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void trackStats(Object value, int colIdx,
                             Comparable<?>[] mins, Comparable<?>[] maxs,
                             long[] nullCounts, long[] valueCounts) {
        if (value == null) {
            nullCounts[colIdx]++;
            return;
        }
        valueCounts[colIdx]++;
        if (value instanceof Comparable) {
            Comparable comp = (Comparable) value;
            if (mins[colIdx] == null || comp.compareTo(mins[colIdx]) < 0) {
                mins[colIdx] = comp;
            }
            if (maxs[colIdx] == null || comp.compareTo(maxs[colIdx]) > 0) {
                maxs[colIdx] = comp;
            }
        }
    }

    private Object readGroupValue(Group group, MessageType fileSchema,
                                   Map<String, String> logicalToPhysical,
                                   String logicalName, DataType sparkType) {
        String physicalName = logicalToPhysical.getOrDefault(logicalName, logicalName);
        int fieldIndex;
        try {
            fieldIndex = fileSchema.getFieldIndex(physicalName);
        } catch (Exception e) {
            return null;
        }
        if (group.getFieldRepetitionCount(fieldIndex) == 0) {
            return null;
        }
        try {
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
                return group.getString(fieldIndex, 0);
            } else {
                return group.getValueToString(fieldIndex, 0);
            }
        } catch (Exception e) {
            return null;
        }
    }

    private void writeUpdatedRows(String absolutePath, StructType sparkSchema,
                                   long[] columnIds, List<Object[]> rows) {
        MessageType parquetSchema = buildParquetSchema(sparkSchema, columnIds);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(parquetSchema);

        File parentDir = new File(absolutePath).getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(absolutePath))
                .withType(parquetSchema)
                .withConf(new Configuration())
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            StructField[] fields = sparkSchema.fields();
            for (Object[] row : rows) {
                Group group = groupFactory.newGroup();
                for (int i = 0; i < fields.length; i++) {
                    if (row[i] != null) {
                        writeField(group, i, row[i], fields[i].dataType());
                    }
                }
                writer.write(group);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write updated data file: " + absolutePath, e);
        }
    }

    private void writeField(Group group, int fieldIndex, Object value, DataType type) {
        if (type instanceof BooleanType) {
            group.add(fieldIndex, (Boolean) value);
        } else if (type instanceof ByteType) {
            group.add(fieldIndex, (int) ((Byte) value));
        } else if (type instanceof ShortType) {
            group.add(fieldIndex, (int) ((Short) value));
        } else if (type instanceof IntegerType) {
            group.add(fieldIndex, (Integer) value);
        } else if (type instanceof LongType) {
            group.add(fieldIndex, (Long) value);
        } else if (type instanceof FloatType) {
            group.add(fieldIndex, (Float) value);
        } else if (type instanceof DoubleType) {
            group.add(fieldIndex, (Double) value);
        } else if (type instanceof StringType) {
            group.add(fieldIndex, value.toString());
        } else {
            group.add(fieldIndex, value.toString());
        }
    }

    private static MessageType buildParquetSchema(StructType sparkSchema, long[] columnIds) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < sparkSchema.fields().length; i++) {
            StructField field = sparkSchema.fields()[i];
            int fieldId = (int) columnIds[i];
            builder.addField(sparkTypeToParquetType(field.name(), field.dataType(), field.nullable(), fieldId));
        }
        return builder.named("spark_schema");
    }

    private static org.apache.parquet.schema.Type sparkTypeToParquetType(
            String name, DataType type, boolean nullable, int fieldId) {
        PrimitiveType.PrimitiveTypeName typeName;
        LogicalTypeAnnotation logicalType = null;

        if (type instanceof BooleanType) {
            typeName = PrimitiveType.PrimitiveTypeName.BOOLEAN;
        } else if (type instanceof ByteType || type instanceof ShortType || type instanceof IntegerType) {
            typeName = PrimitiveType.PrimitiveTypeName.INT32;
        } else if (type instanceof LongType) {
            typeName = PrimitiveType.PrimitiveTypeName.INT64;
        } else if (type instanceof FloatType) {
            typeName = PrimitiveType.PrimitiveTypeName.FLOAT;
        } else if (type instanceof DoubleType) {
            typeName = PrimitiveType.PrimitiveTypeName.DOUBLE;
        } else {
            typeName = PrimitiveType.PrimitiveTypeName.BINARY;
            logicalType = LogicalTypeAnnotation.stringType();
        }

        if (logicalType != null) {
            return nullable
                    ? Types.optional(typeName).as(logicalType).id(fieldId).named(name)
                    : Types.required(typeName).as(logicalType).id(fieldId).named(name);
        } else {
            return nullable
                    ? Types.optional(typeName).id(fieldId).named(name)
                    : Types.required(typeName).id(fieldId).named(name);
        }
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private Set<Long> loadExistingDeletes(DuckLakeMetadataBackend backend, long tableId,
                                           long dataFileId, long snapshotId,
                                           String dataPath) throws SQLException {
        Set<Long> deleted = new HashSet<>();
        List<DeleteFileInfo> deleteFiles = backend.getDeleteFiles(tableId, dataFileId, snapshotId);
        for (DeleteFileInfo df : deleteFiles) {
            String path = df.pathIsRelative ? dataPath + df.path : df.path;
            try (ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(path))) {
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    ColumnIOFactory factory = new ColumnIOFactory();
                    MessageColumnIO columnIO = factory.getColumnIO(schema);
                    RecordReader<Group> recordReader =
                            columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (long i = 0; i < pages.getRowCount(); i++) {
                        Group group = recordReader.read();
                        int fieldIdx = schema.getFieldIndex("row_id");
                        deleted.add(group.getLong(fieldIdx, 0));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to read delete file: " + path, e);
            }
        }
        return deleted;
    }

    private Map<String, String> buildLogicalToPhysical(DuckLakeMetadataBackend backend,
                                                         long mappingId,
                                                         Map<Long, String> colIdToName) throws SQLException {
        Map<String, String> logicalToPhysical = new HashMap<>();
        if (mappingId >= 0) {
            Map<Long, String> nameMapping = backend.getNameMapping(mappingId);
            for (Map.Entry<Long, String> entry : nameMapping.entrySet()) {
                long fieldId = entry.getKey();
                String physicalName = entry.getValue();
                String logicalName = colIdToName.get(fieldId);
                if (logicalName != null && !physicalName.equals(logicalName)) {
                    logicalToPhysical.put(logicalName, physicalName);
                }
            }
        }
        return logicalToPhysical;
    }

    private DuckLakeMetadataBackend createBackend() {
        return new DuckLakeMetadataBackend(catalogPath, dataPathOption);
    }

    // ---------------------------------------------------------------
    // Result data classes
    // ---------------------------------------------------------------

    private static class ScanResult {
        final List<Long> deletePositions;
        final List<Object[]> updatedRows;
        final List<DuckLakeWriterCommitMessage.ColumnStats> columnStats;

        ScanResult(List<Long> deletePositions, List<Object[]> updatedRows,
                   List<DuckLakeWriterCommitMessage.ColumnStats> columnStats) {
            this.deletePositions = deletePositions;
            this.updatedRows = updatedRows;
            this.columnStats = columnStats;
        }
    }

    private static class UpdateDataFileResult {
        final String relativePath;
        final long recordCount;
        final long fileSize;
        final List<DuckLakeWriterCommitMessage.ColumnStats> columnStats;

        UpdateDataFileResult(String relativePath, long recordCount, long fileSize,
                              List<DuckLakeWriterCommitMessage.ColumnStats> columnStats) {
            this.relativePath = relativePath;
            this.recordCount = recordCount;
            this.fileSize = fileSize;
            this.columnStats = columnStats;
        }
    }
}
