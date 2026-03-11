package io.ducklake.spark.reader;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.util.DuckLakeTypeMapping;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.sql.SQLException;
import java.util.*;

/**
 * DuckLake table implementation that supports streaming reads.
 * Used with {@code spark.readStream().format("ducklake-stream")}.
 */
public class DuckLakeStreamTable implements Table, SupportsRead {

    private final String catalogPath;
    private final String dataPath;
    private final String tableName;
    private final String schemaName;
    private final StructType tableSchema;

    public DuckLakeStreamTable(String catalogPath, String dataPath,
                                String tableName, String schemaName) {
        this.catalogPath = catalogPath;
        this.dataPath = dataPath;
        this.tableName = tableName;
        this.schemaName = schemaName;

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            long snap = backend.getCurrentSnapshotId();
            SchemaInfo schemaInfo = backend.getSchemaByName(schemaName);
            TableInfo table = backend.getTable(schemaInfo.schemaId, tableName);
            List<ColumnInfo> columns = backend.getColumns(table.tableId, snap);
            this.tableSchema = DuckLakeTypeMapping.buildSchema(columns);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load DuckLake table schema", e);
        }
    }

    @Override
    public String name() {
        return schemaName + "." + tableName;
    }

    @Override
    public StructType schema() {
        return tableSchema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return new HashSet<>(Arrays.asList(
                TableCapability.MICRO_BATCH_READ
        ));
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new DuckLakeStreamScanBuilder(catalogPath, dataPath, tableName, schemaName, tableSchema);
    }

    /**
     * Scan builder that produces a MicroBatchStream.
     */
    private static class DuckLakeStreamScanBuilder implements ScanBuilder, Scan {
        private final String catalogPath;
        private final String dataPath;
        private final String tableName;
        private final String schemaName;
        private final StructType schema;

        DuckLakeStreamScanBuilder(String catalogPath, String dataPath,
                                   String tableName, String schemaName, StructType schema) {
            this.catalogPath = catalogPath;
            this.dataPath = dataPath;
            this.tableName = tableName;
            this.schemaName = schemaName;
            this.schema = schema;
        }

        @Override
        public Scan build() {
            return this;
        }

        @Override
        public StructType readSchema() {
            return schema;
        }

        @Override
        public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
            return new DuckLakeStreamingSource(catalogPath, dataPath, tableName, schemaName, schema);
        }
    }
}
