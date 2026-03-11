package io.ducklake.spark.catalog;

/**
 * Thrown when a DuckLake table is not found.
 */
public class DuckLakeTableNotFoundException extends DuckLakeException {
    private final String schemaName;
    private final String tableName;

    public DuckLakeTableNotFoundException(String schemaName, String tableName) {
        super("Table '" + schemaName + "." + tableName + "' not found");
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
}
