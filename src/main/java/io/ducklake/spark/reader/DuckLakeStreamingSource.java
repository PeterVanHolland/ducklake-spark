package io.ducklake.spark.reader;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.util.DuckLakeTypeMapping;

import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.read.streaming.*;

import java.sql.SQLException;
import java.util.*;

/**
 * Spark Structured Streaming V2 micro-batch source for DuckLake tables.
 *
 * <p>Polls for new snapshots and returns data file changes (new files added
 * between offsets) as input partitions. Each micro-batch covers the snapshots
 * from the last committed offset to the latest available snapshot.</p>
 *
 * <p>The offset is simply the DuckLake snapshot ID (monotonically increasing).
 * New data files whose {@code begin_snapshot} falls in the offset range are
 * returned as the micro-batch partitions.</p>
 */
public class DuckLakeStreamingSource implements MicroBatchStream {

    private final String catalogPath;
    private final String dataPath;
    private final String tableName;
    private final String schemaName;
    private final org.apache.spark.sql.types.StructType readSchema;

    public DuckLakeStreamingSource(String catalogPath, String dataPath,
                                    String tableName, String schemaName,
                                    org.apache.spark.sql.types.StructType readSchema) {
        this.catalogPath = catalogPath;
        this.dataPath = dataPath;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.readSchema = readSchema;
    }

    @Override
    public Offset latestOffset() {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            long snap = backend.getCurrentSnapshotId();
            return new DuckLakeStreamingOffset(snap);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get latest DuckLake snapshot", e);
        }
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        long startSnap = ((DuckLakeStreamingOffset) start).getSnapshotId();
        long endSnap = ((DuckLakeStreamingOffset) end).getSnapshotId();

        if (startSnap >= endSnap) {
            return new InputPartition[0];
        }

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            TableInfo table = backend.getTable(schema.schemaId, tableName);
            String resolvedDataPath = backend.getDataPath();

            // Get data files added in the range (startSnap, endSnap]
            List<DataFileInfo> newFiles = backend.getDataFilesInRange(table.tableId, startSnap, endSnap);

            // Build column mappings for the partition reader
            List<ColumnInfo> columns = backend.getColumns(table.tableId, endSnap);
            java.util.Map<Long, String> nameMapping = new java.util.HashMap<>();
            java.util.Map<Long, String> colIdToName = new java.util.HashMap<>();
            java.util.Map<String, Long> nameToColumnId = new java.util.HashMap<>();
            java.util.Map<Long, String> columnDefaults = new java.util.HashMap<>();
            java.util.Map<Long, String> columnTypes = new java.util.HashMap<>();
            for (ColumnInfo col : columns) {
                colIdToName.put(col.columnId, col.name);
                nameToColumnId.put(col.name, col.columnId);
                columnTypes.put(col.columnId, col.type);
                if (col.defaultValue != null) {
                    columnDefaults.put(col.columnId, col.defaultValue);
                }
            }

            List<InputPartition> partitions = new ArrayList<>();
            for (DataFileInfo file : newFiles) {
                String filePath = file.pathIsRelative
                        ? java.nio.file.Paths.get(resolvedDataPath, file.path).toString()
                        : file.path;
                partitions.add(new DuckLakeInputPartition(filePath, file.recordCount,
                        new String[0], nameMapping, colIdToName, nameToColumnId,
                        columnDefaults, columnTypes));
            }
            return partitions.toArray(new InputPartition[0]);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to plan streaming input partitions", e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new DuckLakePartitionReaderFactory(readSchema, readSchema, false);
    }

    @Override
    public Offset initialOffset() {
        // Start from the current snapshot (only new data going forward)
        // Use snapshot 0 if we want to replay from the beginning
        return new DuckLakeStreamingOffset(0);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return DuckLakeStreamingOffset.fromJson(json);
    }

    @Override
    public void commit(Offset end) {
        // No-op: DuckLake snapshots are immutable
    }

    @Override
    public void stop() {
        // No resources to release
    }
}
