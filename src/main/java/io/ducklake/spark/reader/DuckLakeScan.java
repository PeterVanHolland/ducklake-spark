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
            List<ColumnInfo> columns = backend.getColumns(table.tableId, snapshotId);

            // Get partition information for partition pruning
            List<DuckLakeMetadataBackend.PartitionInfo> partitionInfos = backend.getPartitionColumns(table.tableId, snapshotId);

            // Extract partition filters from pushed filters
            List<DuckLakeMetadataBackend.PartitionFilter> partitionFilters = new ArrayList<>();
            if (!partitionInfos.isEmpty() && filters != null && filters.length > 0) {
                DuckLakePartitionFilterExtractor extractor = new DuckLakePartitionFilterExtractor(partitionInfos);
                partitionFilters = extractor.extractPartitionFilters(filters);
            }

            // Get data files, using partition pruning if applicable
            List<DataFileInfo> files;
            if (!partitionFilters.isEmpty()) {
                files = backend.getDataFilesForPartition(table.tableId, snapshotId, partitionFilters);
            } else {
                files = backend.getDataFiles(table.tableId, snapshotId);
            }

            // Build column ID -> name mapping for stats-based pruning
            Map<Long, String> colIdToName = new HashMap<>();
            for (ColumnInfo col : columns) {
                colIdToName.put(col.columnId, col.name);
            }

            // Check if table is partitioned and load partition metadata for pruning
            long partitionId = backend.getActivePartitionId(table.tableId, snapshotId);
            List<PartitionColumnDef> partCols = null;
            Map<Long, Map<Integer, String>> filePartitionValues = null;
            Map<Integer, String> partKeyToColName = null;
            if (partitionId >= 0) {
                partCols = backend.getPartitionColumns(partitionId, table.tableId);
                if (!partCols.isEmpty()) {
                    filePartitionValues = backend.getFilePartitionValues(table.tableId);
                    partKeyToColName = new HashMap<>();
                    for (PartitionColumnDef pc : partCols) {
                        String colName = colIdToName.get(pc.columnId);
                        if (colName != null) {
                            partKeyToColName.put(pc.keyIndex, colName);
                        }
                    }
                }
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

                // Partition-value-based pruning: skip files whose partition values
                // don't match the pushed filters (identity partitions only)
                if (partKeyToColName != null && filePartitionValues != null
                        && filters != null && filters.length > 0) {
                    Map<Integer, String> fpv = filePartitionValues.get(file.dataFileId);
                    if (fpv != null) {
                        Map<String, String> partValuesByColName = new HashMap<>();
                        for (Map.Entry<Integer, String> entry : fpv.entrySet()) {
                            String colName = partKeyToColName.get(entry.getKey());
                            if (colName != null) {
                                partValuesByColName.put(colName, entry.getValue());
                            }
                        }
                        if (!partValuesByColName.isEmpty()) {
                            boolean partitionSkip = false;
                            for (Filter f : filters) {
                                if (!DuckLakeFilterEvaluator.partitionMightMatch(f, partValuesByColName)) {
                                    partitionSkip = true;
                                    break;
                                }
                            }
                            if (partitionSkip) {
                                continue; // skip this data file
                            }
                        }
                    }
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
