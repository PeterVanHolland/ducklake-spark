package io.ducklake.spark.catalog;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import java.io.Serializable;
import java.util.*;

/**
 * Pre-loaded metadata snapshot for a DuckLake table.
 * Populated once during loadTable(), reused by DuckLakeScan
 * to eliminate redundant SQLite queries.
 *
 * Mirrors how Iceberg's SparkTable holds a live Table object
 * with manifests already parsed in memory.
 */
public class DuckLakeTableMetadataCache implements Serializable {
    private static final long serialVersionUID = 1L;

    public final long tableId;
    public final String tableName;
    public final String tablePath;
    public final boolean pathIsRelative;
    public final long snapshotId;
    public final String dataPath;
    public final List<ColumnInfo> columns;
    public final List<PartitionInfo> partitionInfos;
    public final List<DataFileInfo> dataFiles;
    public final Map<Long, List<DeleteFileInfo>> deleteFiles;
    public final Map<Long, List<FileColumnStats>> fileColumnStats;
    public final Map<Long, Map<Long, String>> nameMappings;

    public DuckLakeTableMetadataCache(
            long tableId, String tableName, String tablePath, boolean pathIsRelative,
            long snapshotId, String dataPath,
            List<ColumnInfo> columns,
            List<PartitionInfo> partitionInfos,
            List<DataFileInfo> dataFiles,
            Map<Long, List<DeleteFileInfo>> deleteFiles,
            Map<Long, List<FileColumnStats>> fileColumnStats,
            Map<Long, Map<Long, String>> nameMappings) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.tablePath = tablePath;
        this.pathIsRelative = pathIsRelative;
        this.snapshotId = snapshotId;
        this.dataPath = dataPath;
        this.columns = columns;
        this.partitionInfos = partitionInfos;
        this.dataFiles = dataFiles;
        this.deleteFiles = deleteFiles;
        this.fileColumnStats = fileColumnStats;
        this.nameMappings = nameMappings;
    }

    /**
     * Load all metadata for a table in one shot from the backend.
     * After this call, the scan needs zero SQLite queries.
     */
    public static DuckLakeTableMetadataCache load(DuckLakeMetadataBackend backend,
                                                    TableInfo tableInfo, long snapshotId)
            throws java.sql.SQLException {
        String dataPath = backend.getDataPath();
        List<ColumnInfo> columns = backend.getColumns(tableInfo.tableId, snapshotId);
        List<PartitionInfo> partitionInfos = backend.getPartitionColumns(tableInfo.tableId, snapshotId);
        List<DataFileInfo> dataFiles = backend.getDataFiles(tableInfo.tableId, snapshotId);
        Map<Long, List<DeleteFileInfo>> deleteFiles =
                backend.getAllDeleteFilesForTable(tableInfo.tableId, snapshotId);
        Map<Long, List<FileColumnStats>> fileColumnStats =
                backend.getAllFileColumnStatsForTable(tableInfo.tableId, snapshotId);
        Map<Long, Map<Long, String>> nameMappings =
                backend.getAllNameMappingsForTable(tableInfo.tableId, snapshotId);

        return new DuckLakeTableMetadataCache(
                tableInfo.tableId, tableInfo.name, tableInfo.path, tableInfo.pathIsRelative,
                snapshotId, dataPath,
                columns, partitionInfos, dataFiles,
                deleteFiles, fileColumnStats, nameMappings);
    }

    /**
     * Lightweight cache: just table info + columns + snapshot ID.
     * The scan will load file metadata using the cached table ID
     * (one connection, bulk queries, but deferred to scan time).
     */
    public static DuckLakeTableMetadataCache loadLightweight(DuckLakeMetadataBackend backend,
                                                              TableInfo tableInfo, long snapshotId,
                                                              List<ColumnInfo> columns)
            throws java.sql.SQLException {
        String dataPath = backend.getDataPath();
        List<PartitionInfo> partitionInfos = backend.getPartitionColumns(tableInfo.tableId, snapshotId);
        return new DuckLakeTableMetadataCache(
                tableInfo.tableId, tableInfo.name, tableInfo.path, tableInfo.pathIsRelative,
                snapshotId, dataPath,
                columns, partitionInfos,
                null, null, null, null); // files loaded lazily
    }

    /** Check if file metadata needs lazy loading. */
    public boolean needsFileMetadata() {
        return dataFiles == null;
    }
}
