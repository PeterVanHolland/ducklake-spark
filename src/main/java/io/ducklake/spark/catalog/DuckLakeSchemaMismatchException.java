package io.ducklake.spark.catalog;

import java.util.Set;

/**
 * Thrown when Parquet file schema does not match the DuckLake table schema.
 */
public class DuckLakeSchemaMismatchException extends DuckLakeException {
    private final Set<String> fileColumns;
    private final Set<String> tableColumns;

    public DuckLakeSchemaMismatchException(Set<String> fileColumns, Set<String> tableColumns) {
        super("Schema mismatch: file columns " + fileColumns + " do not match table columns " + tableColumns);
        this.fileColumns = fileColumns;
        this.tableColumns = tableColumns;
    }

    public Set<String> getFileColumns() { return fileColumns; }
    public Set<String> getTableColumns() { return tableColumns; }
}
