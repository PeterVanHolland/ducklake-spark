package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.sql.SQLException;
import java.util.*;

/**
 * Builds write plans for DuckLake tables.
 * Supports append (default) and truncate-overwrite modes.
 */
public class DuckLakeWriteBuilder implements WriteBuilder, SupportsTruncate {

    private final StructType schema;
    private final CaseInsensitiveStringMap options;
    private boolean isOverwrite = false;

    public DuckLakeWriteBuilder(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public WriteBuilder truncate() {
        this.isOverwrite = true;
        return this;
    }

    @Override
    public Write build() {
        try (DuckLakeMetadataBackend backend = createBackend()) {
            String tableName = options.get("table");
            String schemaName = options.getOrDefault("schema", "main");

            TableInfo tableInfo = backend.getTable(tableName, schemaName);
            if (tableInfo == null) {
                throw new RuntimeException("Table not found: " + schemaName + "." + tableName);
            }

            String dataPath = backend.getDataPath();
            if (!dataPath.endsWith("/")) dataPath += "/";

            List<ColumnInfo> columns = backend.getColumns(tableInfo.tableId);

            // Resolve table path (relative to data path)
            String tablePath = tableInfo.path;
            if (tablePath != null && !tablePath.isEmpty()) {
                if (!tablePath.endsWith("/")) tablePath += "/";
            } else {
                tablePath = "";
            }

            // Map Spark schema field positions to DuckLake column IDs
            long[] columnIds = new long[schema.fields().length];
            for (int i = 0; i < schema.fields().length; i++) {
                String sparkColName = schema.fields()[i].name();
                boolean found = false;
                for (ColumnInfo col : columns) {
                    if (col.name.equalsIgnoreCase(sparkColName)) {
                        columnIds[i] = col.columnId;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new RuntimeException("Column '" + sparkColName +
                            "' not found in DuckLake table " + schemaName + "." + tableName);
                }
            }

            return new DuckLakeBatchWrite(options, schema, tableInfo, columns,
                    dataPath, tablePath, columnIds, isOverwrite);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to build DuckLake write plan", e);
        }
    }

    private DuckLakeMetadataBackend createBackend() {
        String catalog = options.get("catalog");
        String dataPath = options.getOrDefault("data_path", null);
        return new DuckLakeMetadataBackend(catalog, dataPath);
    }
}
