package io.ducklake.spark.reader;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.sources.Filter;

import java.sql.SQLException;
import java.util.*;

/**
 * Represents a scan of a DuckLake table. Plans input partitions
 * (one per data file) and handles file pruning via column statistics.
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
            TableInfo table = backend.getTable(tableName, schemaName);
            if (table == null) {
                throw new RuntimeException("Table not found: " + schemaName + "." + tableName);
            }

            String dataPath = backend.getDataPath();
            List<DataFileInfo> files = backend.getDataFiles(table.tableId);
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            // Build column ID → name mapping for stats-based pruning
            Map<Long, String> colIdToName = new HashMap<>();
            for (ColumnInfo col : columns) {
                colIdToName.put(col.columnId, col.name);
            }

            List<InputPartition> partitions = new ArrayList<>();
            for (DataFileInfo file : files) {
                // Resolve file path
                String filePath = file.pathIsRelative ? dataPath + file.path : file.path;

                // Get delete files for this data file
                List<DeleteFileInfo> deleteFiles = backend.getDeleteFiles(table.tableId, file.dataFileId);
                List<String> deletePaths = new ArrayList<>();
                for (DeleteFileInfo df : deleteFiles) {
                    deletePaths.add(df.pathIsRelative ? dataPath + df.path : df.path);
                }

                // Get name mapping if present (for column renames)
                Map<Long, String> nameMapping = null;
                if (file.mappingId >= 0) {
                    nameMapping = backend.getNameMapping(file.mappingId);
                }

                // TODO: stats-based file pruning using filters and FileColumnStats

                partitions.add(new DuckLakeInputPartition(
                        filePath,
                        file.recordCount,
                        deletePaths.toArray(new String[0]),
                        nameMapping,
                        colIdToName));
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
