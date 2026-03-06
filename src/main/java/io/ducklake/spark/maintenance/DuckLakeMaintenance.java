package io.ducklake.spark.maintenance;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.SparkSession;

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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Maintenance utilities for DuckLake tables managed through Spark.
 *
 * <p>Provides three operations for table lifecycle management:</p>
 * <ul>
 *   <li>{@link #rewriteDataFiles} - Compact small files and apply deletion vectors</li>
 *   <li>{@link #expireSnapshots} - Remove old snapshot metadata</li>
 *   <li>{@link #vacuum} - Physically delete orphaned data files</li>
 * </ul>
 *
 * <p>Usage:</p>
 * <pre>
 *   DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, "my_table", "main");
 *   DuckLakeMaintenance.expireSnapshots(spark, catalogPath, 7);
 *   DuckLakeMaintenance.vacuum(spark, catalogPath);
 * </pre>
 */
public class DuckLakeMaintenance {

    private DuckLakeMaintenance() {} // utility class

    /**
     * Rewrite (compact) data files for a table. Merges multiple small files into
     * a single file and applies any pending deletion vectors, producing a clean
     * data file with no associated delete files.
     *
     * <p>This is a no-op if the table already has at most one data file
     * and no delete files.</p>
     *
     * @param spark       active SparkSession (used for cluster configuration)
     * @param catalogPath path to the .ducklake catalog file
     * @param tableName   name of the table to compact
     * @param schemaName  schema (namespace) containing the table
     */
    public static void rewriteDataFiles(SparkSession spark, String catalogPath,
                                         String tableName, String schemaName) {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, null)) {
            String dataPath = backend.getDataPath();
            if (!dataPath.endsWith("/")) dataPath += "/";

            long currentSnap = backend.getCurrentSnapshotId();
            TableInfo table = backend.getTable(tableName, schemaName, currentSnap);
            if (table == null) {
                throw new RuntimeException("Table not found: " + schemaName + "." + tableName);
            }

            long tableId = table.tableId;
            List<DataFileInfo> dataFiles = backend.getDataFiles(tableId, currentSnap);
            List<ColumnInfo> columns = backend.getColumns(tableId, currentSnap);

            // Check if compaction is needed
            boolean hasDeletes = false;
            for (DataFileInfo f : dataFiles) {
                if (!backend.getDeleteFiles(tableId, f.dataFileId, currentSnap).isEmpty()) {
                    hasDeletes = true;
                    break;
                }
            }

            if (dataFiles.size() <= 1 && !hasDeletes) {
                return; // Already compact
            }

            // Build output Parquet schema from current columns
            MessageType outputSchema = buildParquetSchema(columns);
            SimpleGroupFactory outputFactory = new SimpleGroupFactory(outputSchema);

            // Build column mappings
            Map<Long, String> colIdToName = new HashMap<>();
            for (ColumnInfo col : columns) {
                colIdToName.put(col.columnId, col.name);
            }

            // Prepare output file
            String fileName = "ducklake-compacted-" + UUID.randomUUID() + ".parquet";
            String relPath = table.path + fileName;
            String absPath = dataPath + relPath;
            new File(absPath).getParentFile().mkdirs();

            // Initialize stats accumulators
            ColumnStatsAcc[] statsAccs = new ColumnStatsAcc[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                statsAccs[i] = new ColumnStatsAcc();
            }

            long recordCount = 0;

            try {
                // Stream rows from all input files through to output
                ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(absPath))
                        .withType(outputSchema)
                        .withConf(new Configuration())
                        .withWriteMode(ParquetFileWriter.Mode.CREATE)
                        .build();

                try {
                    for (DataFileInfo dataFile : dataFiles) {
                        String filePath = dataFile.pathIsRelative
                                ? dataPath + dataFile.path : dataFile.path;

                        // Load delete positions for this file
                        Set<Long> deletes = loadDeletes(
                                backend, tableId, dataFile.dataFileId, currentSnap, dataPath);

                        // Get name mapping if present (for column renames)
                        Map<Long, String> nameMapping = null;
                        if (dataFile.mappingId >= 0) {
                            nameMapping = backend.getNameMapping(dataFile.mappingId);
                        }

                        // Read and compact this file
                        try (ParquetFileReader reader = ParquetFileReader.open(
                                new Configuration(), new Path(filePath))) {
                            MessageType fileSchema = reader.getFooter()
                                    .getFileMetaData().getSchema();

                            // Build column mapping: output index -> file index
                            int[] outputToFile = buildOutputToFileMapping(
                                    outputSchema, fileSchema, columns, nameMapping);

                            long rowPos = 0;
                            PageReadStore pages;
                            while ((pages = reader.readNextRowGroup()) != null) {
                                long rowCount = pages.getRowCount();
                                ColumnIOFactory factory = new ColumnIOFactory();
                                MessageColumnIO columnIO = factory.getColumnIO(fileSchema);
                                RecordReader<Group> recordReader = columnIO.getRecordReader(
                                        pages, new GroupRecordConverter(fileSchema));

                                for (long i = 0; i < rowCount; i++) {
                                    Group inputGroup = recordReader.read();
                                    long pos = rowPos++;

                                    if (deletes.contains(pos)) {
                                        continue;
                                    }

                                    Group outputGroup = mapGroup(
                                            inputGroup, outputFactory, outputSchema,
                                            fileSchema, outputToFile, statsAccs);
                                    writer.write(outputGroup);
                                    recordCount++;
                                }
                            }
                        }
                    }
                } finally {
                    writer.close();
                }

                // Handle empty table (all rows deleted)
                if (recordCount == 0) {
                    new File(absPath).delete();
                }
            } catch (Exception e) {
                new File(absPath).delete();
                throw new RuntimeException("Failed to write compacted file", e);
            }

            long fileSize = recordCount > 0 ? new File(absPath).length() : 0;

            // Update catalog in a single transaction
            backend.beginTransaction();
            try {
                CatalogState snapInfo = backend.getSnapshotInfo(currentSnap);
                long newSnap = currentSnap + 1;
                long newFileId = snapInfo.nextFileId;
                long nextFileId = recordCount > 0 ? newFileId + 1 : newFileId;

                backend.createSnapshot(newSnap, snapInfo.schemaVersion,
                        snapInfo.nextCatalogId, nextFileId);

                // Mark old data files and delete files as logically deleted
                backend.markDataFilesDeleted(tableId, newSnap);
                backend.markDeleteFilesDeleted(tableId, newSnap);

                if (recordCount > 0) {
                    // Register new compacted data file
                    backend.insertDataFile(newFileId, tableId, newSnap, 0,
                            relPath, recordCount, fileSize, 0);

                    // Register column statistics
                    for (int i = 0; i < columns.size(); i++) {
                        backend.insertColumnStats(newFileId, tableId,
                                columns.get(i).columnId,
                                statsAccs[i].valueCount, statsAccs[i].nullCount,
                                statsAccs[i].getMinString(), statsAccs[i].getMaxString());
                    }
                }

                // Update table stats
                backend.updateTableStats(tableId, recordCount, recordCount, fileSize);

                backend.insertSnapshotChanges(newSnap,
                        "compacted_table:" + tableId,
                        "ducklake-spark", "Compact data files");

                backend.commitTransaction();
            } catch (Exception e) {
                try { backend.rollbackTransaction(); } catch (SQLException ex) { e.addSuppressed(ex); }
                if (recordCount > 0) new File(absPath).delete();
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException("Failed to commit compaction", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to compact table: " + schemaName + "." + tableName, e);
        }
    }

    /**
     * Expire snapshots older than the specified number of days.
     * The latest snapshot is always retained regardless of age.
     *
     * <p>Expired snapshots have their metadata records removed from the catalog.
     * Their associated data files are not deleted until {@link #vacuum} is called.</p>
     *
     * @param spark         active SparkSession
     * @param catalogPath   path to the .ducklake catalog file
     * @param olderThanDays remove snapshots older than this many days (0 = all except latest)
     */
    public static void expireSnapshots(SparkSession spark, String catalogPath,
                                        int olderThanDays) {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, null)) {
            List<SnapshotInfo> snapshots = backend.listSnapshots();
            if (snapshots.size() <= 1) {
                return; // Always keep at least one snapshot
            }

            long latestId = snapshots.get(snapshots.size() - 1).snapshotId;
            LocalDateTime cutoff = LocalDateTime.now(ZoneOffset.UTC).minusDays(olderThanDays);

            backend.beginTransaction();
            try {
                for (SnapshotInfo snap : snapshots) {
                    if (snap.snapshotId == latestId) {
                        continue; // Never expire the latest snapshot
                    }
                    if (isOlderThan(snap.snapshotTime, cutoff)) {
                        backend.deleteSnapshotRecord(snap.snapshotId);
                    }
                }
                backend.commitTransaction();
            } catch (Exception e) {
                try { backend.rollbackTransaction(); } catch (SQLException ex) { e.addSuppressed(ex); }
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException("Failed to expire snapshots", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to expire snapshots", e);
        }
    }

    /**
     * Remove orphaned data files that are no longer referenced by any snapshot.
     *
     * <p>A file is considered orphaned if it has been logically deleted
     * (end_snapshot is set) and no remaining snapshot falls within its
     * visibility range [begin_snapshot, end_snapshot).</p>
     *
     * <p>This should typically be called after {@link #expireSnapshots} to
     * reclaim storage for files that were only visible in now-expired snapshots.</p>
     *
     * @param spark       active SparkSession
     * @param catalogPath path to the .ducklake catalog file
     */
    public static void vacuum(SparkSession spark, String catalogPath) {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, null)) {
            String dataPath = backend.getDataPath();
            if (!dataPath.endsWith("/")) dataPath += "/";

            // Get all remaining snapshot IDs
            List<SnapshotInfo> snapshots = backend.listSnapshots();
            Set<Long> remainingSnapIds = new HashSet<>();
            for (SnapshotInfo s : snapshots) {
                remainingSnapIds.add(s.snapshotId);
            }

            if (remainingSnapIds.isEmpty()) {
                return;
            }

            backend.beginTransaction();
            try {
                // Clean up orphaned data files
                List<EndedFileInfo> endedDataFiles = backend.getEndedDataFiles();
                for (EndedFileInfo f : endedDataFiles) {
                    if (!isVisibleInAnySnapshot(f, remainingSnapIds)) {
                        // Physically delete the Parquet file
                        String path = f.pathIsRelative ? dataPath + f.path : f.path;
                        new File(path).delete();

                        // Remove catalog records
                        backend.removeColumnStatsForFile(f.fileId, f.tableId);
                        backend.removeDataFileRecord(f.fileId);
                    }
                }

                // Clean up orphaned delete files
                List<EndedFileInfo> endedDeleteFiles = backend.getEndedDeleteFiles();
                for (EndedFileInfo f : endedDeleteFiles) {
                    if (!isVisibleInAnySnapshot(f, remainingSnapIds)) {
                        String path = f.pathIsRelative ? dataPath + f.path : f.path;
                        new File(path).delete();
                        backend.removeDeleteFileRecord(f.fileId);
                    }
                }

                backend.commitTransaction();
            } catch (Exception e) {
                try { backend.rollbackTransaction(); } catch (SQLException ex) { e.addSuppressed(ex); }
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException("Failed to vacuum", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to vacuum catalog", e);
        }
    }

    // ---------------------------------------------------------------
    // Parquet reading helpers
    // ---------------------------------------------------------------

    /**
     * Load deletion positions from all active delete files for a given data file.
     */
    private static Set<Long> loadDeletes(DuckLakeMetadataBackend backend, long tableId,
                                          long dataFileId, long snapshotId,
                                          String dataPath) throws SQLException {
        Set<Long> deleted = new HashSet<>();
        List<DeleteFileInfo> deleteFiles = backend.getDeleteFiles(tableId, dataFileId, snapshotId);

        for (DeleteFileInfo df : deleteFiles) {
            String path = df.pathIsRelative ? dataPath + df.path : df.path;
            try (ParquetFileReader reader = ParquetFileReader.open(
                    new Configuration(), new Path(path))) {
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    ColumnIOFactory factory = new ColumnIOFactory();
                    MessageColumnIO columnIO = factory.getColumnIO(schema);
                    RecordReader<Group> recordReader = columnIO.getRecordReader(
                            pages, new GroupRecordConverter(schema));
                    for (long i = 0; i < pages.getRowCount(); i++) {
                        Group group = recordReader.read();
                        int fieldIndex = schema.getFieldIndex("row_id");
                        deleted.add(group.getLong(fieldIndex, 0));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to read delete file: " + path, e);
            }
        }
        return deleted;
    }

    // ---------------------------------------------------------------
    // Column mapping (schema evolution support)
    // ---------------------------------------------------------------

    /**
     * Build a mapping from output schema field positions to file schema field positions.
     * Uses field_id matching first, then name mapping, then direct name match.
     * Returns -1 for columns not present in the file.
     */
    private static int[] buildOutputToFileMapping(MessageType outputSchema,
                                                   MessageType fileSchema,
                                                   List<ColumnInfo> columns,
                                                   Map<Long, String> nameMapping) {
        int outputCount = outputSchema.getFieldCount();
        int[] mapping = new int[outputCount];
        Arrays.fill(mapping, -1);

        // Build file schema indexes
        Map<Long, Integer> fileFieldIdToIndex = new HashMap<>();
        Map<String, Integer> fileNameToIndex = new HashMap<>();
        for (int i = 0; i < fileSchema.getFieldCount(); i++) {
            Type fieldType = fileSchema.getType(i);
            fileNameToIndex.put(fieldType.getName(), i);
            if (fieldType.getId() != null) {
                fileFieldIdToIndex.put((long) fieldType.getId().intValue(), i);
            }
        }

        for (int i = 0; i < outputCount; i++) {
            ColumnInfo col = columns.get(i);

            // Strategy 1: Match by field_id
            if (fileFieldIdToIndex.containsKey(col.columnId)) {
                mapping[i] = fileFieldIdToIndex.get(col.columnId);
            }
            // Strategy 2: Name mapping (for files with mapping_id)
            else if (nameMapping != null && nameMapping.containsKey(col.columnId)) {
                String physName = nameMapping.get(col.columnId);
                if (fileNameToIndex.containsKey(physName)) {
                    mapping[i] = fileNameToIndex.get(physName);
                }
            }
            // Strategy 3: Direct name match
            else if (fileNameToIndex.containsKey(col.name)) {
                mapping[i] = fileNameToIndex.get(col.name);
            }
        }

        return mapping;
    }

    // ---------------------------------------------------------------
    // Parquet Group mapping
    // ---------------------------------------------------------------

    /**
     * Map a Group from the file schema to the output schema using the column mapping.
     */
    private static Group mapGroup(Group input, SimpleGroupFactory outputFactory,
                                   MessageType outputSchema, MessageType fileSchema,
                                   int[] outputToFile, ColumnStatsAcc[] stats) {
        Group output = outputFactory.newGroup();

        for (int i = 0; i < outputSchema.getFieldCount(); i++) {
            int fileIdx = outputToFile[i];

            if (fileIdx < 0 || input.getFieldRepetitionCount(fileIdx) == 0) {
                // Column not in file or null value
                stats[i].addNull();
                continue;
            }

            Type inType = fileSchema.getType(fileIdx);
            copyField(input, fileIdx, output, i, inType, stats[i]);
        }

        return output;
    }

    /**
     * Copy a single field value from input Group to output Group.
     */
    private static void copyField(Group input, int inIdx, Group output, int outIdx,
                                   Type inType, ColumnStatsAcc stats) {
        if (!inType.isPrimitive()) {
            stats.addValue(null);
            return;
        }

        PrimitiveType.PrimitiveTypeName typeName = inType.asPrimitiveType().getPrimitiveTypeName();
        switch (typeName) {
            case BOOLEAN: {
                boolean v = input.getBoolean(inIdx, 0);
                output.add(outIdx, v);
                stats.addValue(v);
                break;
            }
            case INT32: {
                int v = input.getInteger(inIdx, 0);
                output.add(outIdx, v);
                stats.addValue(v);
                break;
            }
            case INT64: {
                long v = input.getLong(inIdx, 0);
                output.add(outIdx, v);
                stats.addValue(v);
                break;
            }
            case FLOAT: {
                float v = input.getFloat(inIdx, 0);
                output.add(outIdx, v);
                stats.addValue(v);
                break;
            }
            case DOUBLE: {
                double v = input.getDouble(inIdx, 0);
                output.add(outIdx, v);
                stats.addValue(v);
                break;
            }
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY: {
                Binary bin = input.getBinary(inIdx, 0);
                output.add(outIdx, bin);
                LogicalTypeAnnotation logType = inType.getLogicalTypeAnnotation();
                if (logType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                    stats.addValue(bin.toStringUsingUTF8());
                } else {
                    stats.addValue(null); // skip stats for non-string binary
                }
                break;
            }
            default:
                stats.addValue(null);
                break;
        }
    }

    // ---------------------------------------------------------------
    // Parquet schema construction
    // ---------------------------------------------------------------

    /**
     * Build a Parquet MessageType from DuckLake column definitions.
     * Sets field_id on each column for schema evolution support.
     */
    private static MessageType buildParquetSchema(List<ColumnInfo> columns) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (ColumnInfo col : columns) {
            builder.addField(duckTypeToParquetType(
                    col.name, col.type, col.nullable, (int) col.columnId));
        }
        return builder.named("compacted_schema");
    }

    private static Type duckTypeToParquetType(String name, String duckType,
                                               boolean nullable, int fieldId) {
        String t = duckType.toUpperCase().trim();

        if (t.equals("BOOLEAN") || t.equals("BOOL")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.BOOLEAN, null, fieldId, name);
        }
        if (t.equals("TINYINT") || t.equals("SMALLINT") || t.equals("INTEGER") || t.equals("INT")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.INT32, null, fieldId, name);
        }
        if (t.equals("BIGINT") || t.equals("LONG")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.INT64, null, fieldId, name);
        }
        if (t.equals("FLOAT") || t.equals("REAL")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.FLOAT, null, fieldId, name);
        }
        if (t.equals("DOUBLE")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.DOUBLE, null, fieldId, name);
        }
        if (t.equals("VARCHAR") || t.equals("TEXT") || t.equals("STRING")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.BINARY,
                    LogicalTypeAnnotation.stringType(), fieldId, name);
        }
        if (t.equals("BLOB") || t.equals("BYTEA")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.BINARY, null, fieldId, name);
        }
        if (t.equals("DATE")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.INT32,
                    LogicalTypeAnnotation.dateType(), fieldId, name);
        }
        if (t.startsWith("TIMESTAMP")) {
            return prim(nullable, PrimitiveType.PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(
                            true, LogicalTypeAnnotation.TimeUnit.MICROS),
                    fieldId, name);
        }
        if (t.startsWith("DECIMAL") || t.startsWith("NUMERIC")) {
            int p = 18, s = 0;
            int lparen = t.indexOf('(');
            if (lparen >= 0) {
                String inner = t.substring(lparen + 1, t.indexOf(')'));
                String[] parts = inner.split(",");
                p = Integer.parseInt(parts[0].trim());
                s = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
            }
            if (p <= 9) {
                return prim(nullable, PrimitiveType.PrimitiveTypeName.INT32,
                        LogicalTypeAnnotation.decimalType(s, p), fieldId, name);
            } else if (p <= 18) {
                return prim(nullable, PrimitiveType.PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.decimalType(s, p), fieldId, name);
            } else {
                int byteLen = (int) Math.ceil(
                        Math.log(Math.pow(10, p)) / Math.log(2) / 8.0) + 1;
                if (nullable) {
                    return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                            .length(byteLen)
                            .as(LogicalTypeAnnotation.decimalType(s, p))
                            .id(fieldId).named(name);
                } else {
                    return Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                            .length(byteLen)
                            .as(LogicalTypeAnnotation.decimalType(s, p))
                            .id(fieldId).named(name);
                }
            }
        }

        // Fallback: store as string
        return prim(nullable, PrimitiveType.PrimitiveTypeName.BINARY,
                LogicalTypeAnnotation.stringType(), fieldId, name);
    }

    private static PrimitiveType prim(boolean nullable,
                                       PrimitiveType.PrimitiveTypeName typeName,
                                       LogicalTypeAnnotation logType,
                                       int fieldId, String name) {
        if (logType != null) {
            return nullable
                    ? Types.optional(typeName).as(logType).id(fieldId).named(name)
                    : Types.required(typeName).as(logType).id(fieldId).named(name);
        } else {
            return nullable
                    ? Types.optional(typeName).id(fieldId).named(name)
                    : Types.required(typeName).id(fieldId).named(name);
        }
    }

    // ---------------------------------------------------------------
    // Snapshot time helpers
    // ---------------------------------------------------------------

    private static final DateTimeFormatter[] TIME_FORMATS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
    };

    private static boolean isOlderThan(String snapshotTime, LocalDateTime cutoff) {
        if (snapshotTime == null) return false;
        String normalized = snapshotTime.trim();
        for (DateTimeFormatter fmt : TIME_FORMATS) {
            try {
                LocalDateTime snapTime = LocalDateTime.parse(normalized, fmt);
                return snapTime.isBefore(cutoff);
            } catch (Exception ignored) {
                // try next format
            }
        }
        // If parsing fails, try replacing 'T' with space and vice versa
        try {
            LocalDateTime snapTime = LocalDateTime.parse(normalized.replace(' ', 'T'));
            return snapTime.isBefore(cutoff);
        } catch (Exception e) {
            return false; // Can't parse -- don't expire
        }
    }

    // ---------------------------------------------------------------
    // Visibility check for vacuum
    // ---------------------------------------------------------------

    /**
     * Check whether a logically-deleted file is still visible in any remaining snapshot.
     * A file is visible at snapshot S if: begin_snapshot <= S < end_snapshot
     */
    private static boolean isVisibleInAnySnapshot(EndedFileInfo file,
                                                   Set<Long> remainingSnapIds) {
        for (long s : remainingSnapIds) {
            if (file.beginSnapshot <= s && file.endSnapshot > s) {
                return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------
    // Column statistics accumulator
    // ---------------------------------------------------------------

    private static class ColumnStatsAcc {
        long nullCount = 0;
        long valueCount = 0;
        Comparable<?> minValue = null;
        Comparable<?> maxValue = null;

        void addNull() {
            nullCount++;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        void addValue(Object value) {
            if (value == null) {
                valueCount++;
                return;
            }
            valueCount++;
            Comparable comp = (Comparable) value;
            if (minValue == null || comp.compareTo(minValue) < 0) {
                minValue = comp;
            }
            if (maxValue == null || comp.compareTo(maxValue) > 0) {
                maxValue = comp;
            }
        }

        String getMinString() {
            return minValue == null ? null : minValue.toString();
        }

        String getMaxString() {
            return maxValue == null ? null : maxValue.toString();
        }
    }
}
