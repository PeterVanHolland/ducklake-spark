package io.ducklake.spark.reader;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.writer.DuckLakeInlineWriter;

import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.sources.Filter;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

/**
 * Represents a scan of a DuckLake table. Plans input partitions
 * and handles file pruning via column statistics.
 *
 * Performance optimizations over naive implementation:
 * <ul>
 *   <li>Bulk queries: fetches ALL delete files, column stats, and name mappings
 *       in 3 queries instead of N per-file queries (eliminates N+1 pattern)</li>
 *   <li>File bin-packing: combines small files into fewer partitions to reduce
 *       Spark task scheduling overhead</li>
 * </ul>
 */
public class DuckLakeScan implements Scan, Batch {

    /** Target partition size for bin-packing (128 MB, matches Iceberg default). */
    private static final long BIN_PACK_TARGET_SIZE = 128L * 1024 * 1024;

    /** Minimum number of partitions for parallelism. */
    private static final int MIN_PARTITIONS = 4;

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
            List<PartitionInfo> partitionInfos = backend.getPartitionColumns(table.tableId, snapshotId);

            // Extract partition filters from pushed filters
            List<PartitionFilter> partitionFilters = new ArrayList<>();
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

            // ============================================================
            // BULK QUERIES: fetch all delete files, stats, and name
            // mappings in 3 queries instead of N per-file queries.
            // ============================================================
            Map<Long, List<DeleteFileInfo>> allDeleteFiles =
                    backend.getAllDeleteFilesForTable(table.tableId, snapshotId);

            // Only fetch stats if we have filters to evaluate
            Map<Long, List<FileColumnStats>> allFileStats = null;
            if (filters != null && filters.length > 0) {
                allFileStats = backend.getAllFileColumnStatsForTable(table.tableId, snapshotId);
            }

            Map<Long, Map<Long, String>> allNameMappings =
                    backend.getAllNameMappingsForTable(table.tableId, snapshotId);

            // ============================================================
            // Build candidate partitions (with stats-based pruning)
            // ============================================================
            List<FilePartitionCandidate> candidates = new ArrayList<>();

            for (DataFileInfo file : files) {
                // Resolve file path
                String filePath = file.pathIsRelative ? dataPath + file.path : file.path;

                // Get delete files from bulk map
                List<DeleteFileInfo> deleteFileList = allDeleteFiles.getOrDefault(file.dataFileId, Collections.emptyList());
                List<String> deletePaths = new ArrayList<>();
                for (DeleteFileInfo df : deleteFileList) {
                    deletePaths.add(df.pathIsRelative ? dataPath + df.path : df.path);
                }

                // Get name mapping from bulk map
                Map<Long, String> nameMapping = null;
                if (file.mappingId >= 0) {
                    nameMapping = allNameMappings.get(file.mappingId);
                }

                // Stats-based file pruning
                if (allFileStats != null) {
                    List<FileColumnStats> fileStats = allFileStats.getOrDefault(file.dataFileId, Collections.emptyList());
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
                            continue;
                        }
                    }
                }

                candidates.add(new FilePartitionCandidate(
                        filePath, file.recordCount, file.fileSizeBytes,
                        deletePaths.toArray(new String[0]),
                        nameMapping, colIdToName, nameToColumnId,
                        columnDefaults, columnTypes));
            }

            // ============================================================
            // BIN-PACKING: combine small files into fewer partitions
            // to reduce Spark task scheduling overhead.
            // ============================================================
            List<InputPartition> partitions = binPackPartitions(candidates);

            // Add inlined data partition if any exist
            try {
                DuckLakeInlineWriter inlineWriter = new DuckLakeInlineWriter(backend);
                List<Map<String, String>> inlinedRows =
                        inlineWriter.readInlinedRows(table.tableId, snapshotId);
                if (!inlinedRows.isEmpty()) {
                    ArrayList<Map<String, String>> serializableRows = new ArrayList<>();
                    for (Map<String, String> row : inlinedRows) {
                        serializableRows.add(new LinkedHashMap<>(row));
                    }
                    partitions.add(new DuckLakeInlinedInputPartition(serializableRows));
                }
            } catch (Exception inlineEx) {
                // No inlined data or table not present - skip
            }

            return partitions.toArray(new InputPartition[0]);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to plan DuckLake scan", e);
        }
    }

    /**
     * Bin-pack file candidates into partitions. Ensures at least
     * MIN_PARTITIONS outputs for parallelism, while combining
     * truly tiny files to reduce Spark task scheduling overhead.
     */
    private List<InputPartition> binPackPartitions(List<FilePartitionCandidate> candidates) {
        if (candidates.isEmpty()) return new ArrayList<>();
        if (candidates.size() <= MIN_PARTITIONS) {
            // Few files — one partition each, no bin-packing needed
            List<InputPartition> result = new ArrayList<>();
            for (FilePartitionCandidate c : candidates) result.add(c.toInputPartition());
            return result;
        }

        // Calculate target: aim for max(MIN_PARTITIONS, size-based bins)
        long totalSize = 0;
        for (FilePartitionCandidate c : candidates) totalSize += c.fileSizeBytes;
        int sizeBins = Math.max(1, (int) ((totalSize + BIN_PACK_TARGET_SIZE - 1) / BIN_PACK_TARGET_SIZE));
        int targetBins = Math.max(MIN_PARTITIONS, sizeBins);
        long binTarget = Math.max(1, totalSize / targetBins);

        // Round-robin into target number of bins
        List<List<FilePartitionCandidate>> bins = new ArrayList<>();
        for (int i = 0; i < targetBins; i++) bins.add(new ArrayList<>());

        // Distribute files across bins
        int binIdx = 0;
        for (FilePartitionCandidate c : candidates) {
            if (c.fileSizeBytes >= BIN_PACK_TARGET_SIZE) {
                // Large file always gets its own bin
                List<FilePartitionCandidate> solo = new ArrayList<>();
                solo.add(c);
                bins.add(solo);
            } else {
                bins.get(binIdx % targetBins).add(c);
                binIdx++;
            }
        }

        List<InputPartition> result = new ArrayList<>();
        for (List<FilePartitionCandidate> bin : bins) {
            if (bin.isEmpty()) continue;
            result.add(mergeIntoBinnedPartition(bin));
        }
        return result;
    }

    /**
     * Merge multiple small file candidates into a single DuckLakeBinnedInputPartition.
     */
    private InputPartition mergeIntoBinnedPartition(List<FilePartitionCandidate> candidates) {
        if (candidates.size() == 1) {
            return candidates.get(0).toInputPartition();
        }
        List<DuckLakeInputPartition> subPartitions = new ArrayList<>();
        for (FilePartitionCandidate c : candidates) {
            subPartitions.add((DuckLakeInputPartition) c.toInputPartition());
        }
        return new DuckLakeBinnedInputPartition(subPartitions);
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

    /** Intermediate structure for file-to-partition planning. */
    private static class FilePartitionCandidate {
        final String filePath;
        final long recordCount;
        final long fileSizeBytes;
        final String[] deleteFilePaths;
        final Map<Long, String> nameMapping;
        final Map<Long, String> colIdToName;
        final Map<String, Long> nameToColumnId;
        final Map<Long, String> columnDefaults;
        final Map<Long, String> columnTypes;

        FilePartitionCandidate(String filePath, long recordCount, long fileSizeBytes,
                               String[] deleteFilePaths, Map<Long, String> nameMapping,
                               Map<Long, String> colIdToName, Map<String, Long> nameToColumnId,
                               Map<Long, String> columnDefaults, Map<Long, String> columnTypes) {
            this.filePath = filePath;
            this.recordCount = recordCount;
            this.fileSizeBytes = fileSizeBytes;
            this.deleteFilePaths = deleteFilePaths;
            this.nameMapping = nameMapping;
            this.colIdToName = colIdToName;
            this.nameToColumnId = nameToColumnId;
            this.columnDefaults = columnDefaults;
            this.columnTypes = columnTypes;
        }

        InputPartition toInputPartition() {
            return new DuckLakeInputPartition(filePath, recordCount, deleteFilePaths,
                    nameMapping, colIdToName, nameToColumnId, columnDefaults, columnTypes);
        }
    }
}
