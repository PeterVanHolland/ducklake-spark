package io.ducklake.spark.reader;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.sources.Filter;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

/**
 * Represents a scan of a DuckLake table. Plans input partitions
 * (one per data file) and handles file pruning via column statistics.
 *
 * Supports time travel via snapshot_version / snapshot_time options.
 * Supports schema evolution: maps columns by field_id and fills
 * defaults for columns added after a file was written.
 */
public class DuckLakeScan implements Scan, Batch {

    private final StructType fullSchema;
    private final StructType requiredSchema;
    private final CaseInsensitiveStringMap options;
    private final Filter[] filters;

    public DuckLakeScan(StructType fullSchema, StructType requiredSchema,
                        CaseInsensitiveStringMap options, Filter[] filters) {
        this.fullSchema = fullSchema;
        this.requiredSchema = requiredSchema;
        this.options = options;
        this.filters = filters;
    }

    @Override
    public StructType readSchema() {
        return requiredSchema;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        try (DuckLakeMetadataBackend backend = createBackend()) {
            String tableName = options.get("table");
            String schemaName = options.getOrDefault("schema", "main");

            // Resolve snapshot for time travel
            long snapshotId = backend.resolveSnapshotId(
                    options.getOrDefault("snapshot_version", null),
                    options.getOrDefault("snapshot_time", null));

            TableInfo table = backend.getTable(tableName, schemaName, snapshotId);
            if (table == null) {
                throw new RuntimeException("Table not found: " + schemaName + "." + tableName);
            }

            String dataPath = backend.getDataPath();
            List<DataFileInfo> files = backend.getDataFiles(table.tableId, snapshotId);
            List<ColumnInfo> columns = backend.getColumns(table.tableId, snapshotId);

            // Build column ID -> name mapping for stats-based pruning
            Map<Long, String> colIdToName = new HashMap<>();
            for (ColumnInfo col : columns) {
                colIdToName.put(col.columnId, col.name);
            }

            // Build schema evolution maps from current columns
            Map<String, Long> nameToColumnId = new HashMap<>();
            Map<Long, String> columnDefaults = new HashMap<>();
            Map<Long, String> columnTypes = new HashMap<>();
            for (ColumnInfo col : columns) {
                nameToColumnId.put(col.name, col.columnId);
                if (col.initialDefault != null) {
                    columnDefaults.put(col.columnId, col.initialDefault);
                }
                columnTypes.put(col.columnId, col.type);
            }

            List<InputPartition> partitions = new ArrayList<>();
            for (DataFileInfo file : files) {
                // Resolve file path
                String filePath = file.pathIsRelative ? dataPath + file.path : file.path;

                // Get delete files for this data file at the target snapshot
                List<DeleteFileInfo> deleteFiles = backend.getDeleteFiles(
                        table.tableId, file.dataFileId, snapshotId);
                List<String> deletePaths = new ArrayList<>();
                for (DeleteFileInfo df : deleteFiles) {
                    deletePaths.add(df.pathIsRelative ? dataPath + df.path : df.path);
                }

                // Get name mapping if present (for column renames)
                Map<Long, String> nameMapping = null;
                if (file.mappingId >= 0) {
                    nameMapping = backend.getNameMapping(file.mappingId);
                }

                // Stats-based file pruning: skip files whose column stats
                // prove no row can match the pushed filters
                if (filters != null && filters.length > 0) {
                    List<FileColumnStats> fileStats = backend.getFileColumnStats(
                            table.tableId, file.dataFileId);
                    if (!fileStats.isEmpty()) {
                        Map<String, FileColumnStats> statsMap = new HashMap<>();
                        for (FileColumnStats fs : fileStats) {
                            String colName = colIdToName.get(fs.columnId);
                            if (colName != null) {
                                statsMap.put(colName, fs);
                            }
                        }
                        DuckLakeFilterEvaluator evaluator =
                                new DuckLakeFilterEvaluator(statsMap, file.recordCount);
                        boolean skip = false;
                        for (Filter f : filters) {
                            if (!evaluator.mightMatch(f)) {
                                skip = true;
                                break;
                            }
                        }
                        if (skip) {
                            continue; // skip this data file
                        }
                    }
                }

                partitions.add(new DuckLakeInputPartition(
                        filePath,
                        file.recordCount,
                        deletePaths.toArray(new String[0]),
                        nameMapping,
                        colIdToName,
                        nameToColumnId,
                        columnDefaults,
                        columnTypes));
            }

            return partitions.toArray(new InputPartition[0]);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to plan DuckLake scan", e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new DuckLakePartitionReaderFactory(requiredSchema, fullSchema);
    }

    private DuckLakeMetadataBackend createBackend() {
        String catalog = options.get("catalog");
        String dataPath = options.getOrDefault("data_path", null);
        return new DuckLakeMetadataBackend(catalog, dataPath);
    }
}
