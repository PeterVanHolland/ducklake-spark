package io.ducklake.spark.catalog;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * Register existing Parquet files into a DuckLake table without copying data.
 * Files are referenced in-place. Schema validation ensures Parquet columns
 * match the DuckLake table definition.
 */
public class DuckLakeAddFiles {

    private final DuckLakeMetadataBackend backend;

    public DuckLakeAddFiles(DuckLakeMetadataBackend backend) {
        this.backend = backend;
    }

    /**
     * Register one or more Parquet files into the specified table.
     * Files are NOT copied — they are referenced in-place.
     *
     * @param tableId       Target table ID
     * @param filePaths     Absolute paths to Parquet files
     * @param columns       Current columns of the table
     * @param pathIsRelative Whether to store paths as relative to data_path
     * @param dataPath      The data_path for resolving relative paths
     * @return The new snapshot ID
     */
    public long addFiles(long tableId, String[] filePaths,
                         List<ColumnInfo> columns, boolean pathIsRelative,
                         String dataPath) throws SQLException, IOException {
        if (filePaths == null || filePaths.length == 0) {
            throw new IllegalArgumentException("filePaths must not be empty");
        }

        // Validate schema from the first file
        validateParquetSchema(filePaths[0], columns);

        backend.beginTransaction();
        try {
            long currentSnap = backend.getCurrentSnapshotId();
            CatalogState meta = backend.getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            long nextFileId = meta.nextFileId;
            long newNextFileId = nextFileId + filePaths.length;

            backend.createSnapshot(newSnap, meta.schemaVersion, meta.nextCatalogId, newNextFileId);

            // Get current table stats
            TableStats tableStats = backend.getTableStats(tableId);
            long rowIdStart = tableStats.nextRowId;
            long totalRecordCount = tableStats.recordCount;
            long totalFileSize = tableStats.fileSizeBytes;

            long fileId = nextFileId;
            int fileOrder = 0;
            for (String filePath : filePaths) {
                // Read Parquet metadata for record count and file size
                ParquetMetadata parquetMeta = readParquetMetadata(filePath);
                long recordCount = getRecordCount(parquetMeta);
                long fileSize = new File(filePath).length();

                // Determine stored path
                String storedPath;
                if (pathIsRelative && dataPath != null && filePath.startsWith(dataPath)) {
                    storedPath = filePath.substring(dataPath.length());
                } else {
                    storedPath = filePath;
                }

                // Use insertDataFile with path_is_relative set appropriately
                backend.insertDataFileAbsolute(fileId, tableId, newSnap, fileOrder,
                        storedPath, pathIsRelative, recordCount, fileSize, rowIdStart);

                // Compute and store column stats from Parquet footer
                storeColumnStatsFromFooter(fileId, tableId, parquetMeta, columns);

                rowIdStart += recordCount;
                totalRecordCount += recordCount;
                totalFileSize += fileSize;
                fileId++;
                fileOrder++;
            }

            backend.updateTableStats(tableId, totalRecordCount, rowIdStart, totalFileSize);

            backend.insertSnapshotChanges(newSnap,
                    "add_files:" + filePaths.length + " files to table:" + tableId,
                    "ducklake-spark", "Add " + filePaths.length + " external file(s)");

            backend.commitTransaction();
            return newSnap;
        } catch (Exception e) {
            backend.rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e :
                  e instanceof IOException ? (IOException) e :
                  new SQLException(e);
        }
    }

    /**
     * Validate that a Parquet file's schema matches the DuckLake table columns.
     */
    private void validateParquetSchema(String filePath, List<ColumnInfo> columns) throws IOException {
        ParquetMetadata meta = readParquetMetadata(filePath);
        MessageType fileSchema = meta.getFileMetaData().getSchema();

        Set<String> fileColNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < fileSchema.getFieldCount(); i++) {
            fileColNames.add(fileSchema.getType(i).getName());
        }

        Set<String> catalogColNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnInfo col : columns) {
            catalogColNames.add(col.name);
        }

        if (!fileColNames.equals(catalogColNames)) {
            throw new IOException("Schema mismatch: file columns " +
                    new TreeSet<>(fileColNames) + " do not match table columns " +
                    new TreeSet<>(catalogColNames));
        }
    }

    private ParquetMetadata readParquetMetadata(String filePath) throws IOException {
        Configuration conf = new Configuration();
        try (ParquetFileReader reader = ParquetFileReader.open(conf, new Path(filePath))) {
            return reader.getFooter();
        }
    }

    private long getRecordCount(ParquetMetadata meta) {
        long total = 0;
        for (org.apache.parquet.hadoop.metadata.BlockMetaData block : meta.getBlocks()) {
            total += block.getRowCount();
        }
        return total;
    }

    /**
     * Extract min/max statistics from Parquet footer metadata and store as column stats.
     */
    private void storeColumnStatsFromFooter(long fileId, long tableId,
                                             ParquetMetadata meta,
                                             List<ColumnInfo> columns) throws SQLException {
        MessageType schema = meta.getFileMetaData().getSchema();

        // Build column name -> column_id map
        Map<String, Long> nameToColId = new HashMap<>();
        for (ColumnInfo col : columns) {
            nameToColId.put(col.name.toLowerCase(), col.columnId);
        }

        // Aggregate stats across all row groups
        Map<String, StatsAccumulator> statsMap = new HashMap<>();

        for (org.apache.parquet.hadoop.metadata.BlockMetaData block : meta.getBlocks()) {
            for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData chunk : block.getColumns()) {
                String colName = chunk.getPath().toDotString().toLowerCase();
                StatsAccumulator acc = statsMap.computeIfAbsent(colName, k -> new StatsAccumulator());
                acc.valueCount += chunk.getValueCount();

                org.apache.parquet.column.statistics.Statistics<?> stats = chunk.getStatistics();
                if (stats != null && stats.hasNonNullValue()) {
                    acc.nullCount += stats.getNumNulls();
                    String min = stats.minAsString();
                    String max = stats.maxAsString();
                    if (acc.min == null || (min != null && min.compareTo(acc.min) < 0)) {
                        acc.min = min;
                    }
                    if (acc.max == null || (max != null && max.compareTo(acc.max) > 0)) {
                        acc.max = max;
                    }
                }
            }
        }

        // Store stats
        for (Map.Entry<String, StatsAccumulator> entry : statsMap.entrySet()) {
            Long colId = nameToColId.get(entry.getKey());
            if (colId != null) {
                StatsAccumulator acc = entry.getValue();
                backend.insertColumnStats(fileId, tableId, colId,
                        acc.valueCount, acc.nullCount, acc.min, acc.max);
            }
        }
    }

    private static class StatsAccumulator {
        long valueCount = 0;
        long nullCount = 0;
        String min = null;
        String max = null;
    }
}
