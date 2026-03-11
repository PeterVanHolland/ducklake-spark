package io.ducklake.spark.reader;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Registers "ducklake-stream" as a Spark data source for streaming reads.
 *
 * <p>Usage:</p>
 * <pre>
 *   spark.readStream()
 *     .format("ducklake-stream")
 *     .option("catalog", "/path/to/catalog.ducklake")
 *     .option("table", "my_table")
 *     .load()
 * </pre>
 */
public class DuckLakeStreamTableProvider implements TableProvider, DataSourceRegister {

    @Override
    public String shortName() {
        return "ducklake-stream";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return getTable(null, new Transform[0], options.asCaseSensitiveMap()).schema();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        String catalogPath = properties.get("catalog");
        String tableName = properties.get("table");
        String schemaName = properties.getOrDefault("schema", "main");
        String dataPath = properties.getOrDefault("data_path", null);

        if (catalogPath == null || tableName == null) {
            throw new IllegalArgumentException(
                    "ducklake-stream requires 'catalog' and 'table' options");
        }

        return new DuckLakeStreamTable(catalogPath, dataPath, tableName, schemaName);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return false;
    }
}
