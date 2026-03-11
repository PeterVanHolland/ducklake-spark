package io.ducklake.spark.catalog;

/**
 * Thrown when a DuckLake schema/namespace is not found.
 */
public class DuckLakeSchemaNotFoundException extends DuckLakeException {
    private final String schemaName;

    public DuckLakeSchemaNotFoundException(String schemaName) {
        super("Schema '" + schemaName + "' not found");
        this.schemaName = schemaName;
    }

    public String getSchemaName() { return schemaName; }
}
