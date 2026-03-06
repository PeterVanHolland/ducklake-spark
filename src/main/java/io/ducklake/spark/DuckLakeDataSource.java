package io.ducklake.spark;

import io.ducklake.spark.reader.DuckLakeScanBuilder;
import io.ducklake.spark.writer.DuckLakeDeleteExecutor;
import io.ducklake.spark.writer.DuckLakeWriteBuilder;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.util.DuckLakeTypeMapping;

import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.sql.SQLException;
import java.util.*;

/**
 * Spark DataSource V2 entry point for DuckLake catalogs.
 *
 * Usage:
 *   spark.read.format("ducklake")
 *     .option("catalog", "/path/to/catalog.ducklake")
 *     .option("table", "my_table")
 *     .load()
 *
 * Time travel:
 *   .option("snapshot_version", 3)          // read at version 3
 *   .option("snapshot_time", "2026-01-01T00:00:00")  // read at timestamp
 */
public class DuckLakeDataSource implements TableProvider {


    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        try (DuckLakeMetadataBackend backend = createBackend(options)) {
            String tableName = getTableName(options);
            String schemaName = options.getOrDefault("schema", "main");

            // Resolve snapshot for time travel
            long snapshotId = backend.resolveSnapshotId(
                    options.getOrDefault("snapshot_version", null),
                    options.getOrDefault("snapshot_time", null));

            DuckLakeMetadataBackend.TableInfo table = backend.getTable(tableName, schemaName, snapshotId);
            if (table == null) {
                throw new IllegalArgumentException("Table not found: " + schemaName + "." + tableName);
            }
            var columns = backend.getColumns(table.tableId, snapshotId);
            return DuckLakeTypeMapping.buildSchema(columns);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to infer schema from DuckLake catalog", e);
        }
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning,
                          Map<String, String> properties) {
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
        return new DuckLakeTable(schema, options);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    private DuckLakeMetadataBackend createBackend(CaseInsensitiveStringMap options) {
        String catalog = options.get("catalog");
        if (catalog == null || catalog.isEmpty()) {
            throw new IllegalArgumentException("Option 'catalog' is required (path to .ducklake file or PostgreSQL DSN)");
        }
        String dataPath = options.getOrDefault("data_path", null);
        return new DuckLakeMetadataBackend(catalog, dataPath);
    }

    private String getTableName(CaseInsensitiveStringMap options) {
        String table = options.get("table");
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("Option 'table' is required");
        }
        return table;
    }

    // ---------------------------------------------------------------
    // Inner Table class implementing SupportsRead
    // ---------------------------------------------------------------

    static class DuckLakeTable implements Table, SupportsRead, SupportsWrite, SupportsDelete {
        private final StructType schema;
        private final CaseInsensitiveStringMap options;

        DuckLakeTable(StructType schema, CaseInsensitiveStringMap options) {
            this.schema = schema;
            this.options = options;
        }

        @Override
        public String name() {
            return "DuckLake:" + options.getOrDefault("table", "unknown");
        }

        @Override
        public StructType schema() {
            return schema;
        }

        @Override
        public Set<TableCapability> capabilities() {
            return new HashSet<>(Arrays.asList(
                    TableCapability.BATCH_READ,
                    TableCapability.BATCH_WRITE,
                    TableCapability.TRUNCATE
            ));
        }

        @Override
        public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
            // Merge table-level options with scan options
            Map<String, String> merged = new HashMap<>(this.options.asCaseSensitiveMap());
            merged.putAll(options.asCaseSensitiveMap());
            return new DuckLakeScanBuilder(schema, new CaseInsensitiveStringMap(merged));
        }

        @Override
        public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
            Map<String, String> merged = new HashMap<>(this.options.asCaseSensitiveMap());
            merged.putAll(info.options().asCaseSensitiveMap());
            return new DuckLakeWriteBuilder(info.schema(), new CaseInsensitiveStringMap(merged));
        }

        @Override
        public boolean canDeleteWhere(Filter[] filters) {
            return true;
        }

        @Override
        public void deleteWhere(Filter[] filters) {
            new DuckLakeDeleteExecutor(
                    options.get("catalog"),
                    options.getOrDefault("data_path", null),
                    options.get("table"),
                    options.getOrDefault("schema", "main")
            ).deleteWhere(filters);
        }
    }
}
