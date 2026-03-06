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
 * Builds scan plans for DuckLake tables. Supports column pruning
 * and filter pushdown via catalog-level file pruning.
 */
public class DuckLakeScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns,
        SupportsPushDownFilters {

    private final CaseInsensitiveStringMap options;
    private StructType schema;
    private StructType requiredSchema;
    private Filter[] pushedFilters = new Filter[0];

    public DuckLakeScanBuilder(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.requiredSchema = schema;
        this.options = options;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        // Accept all filters for file pruning; return them as post-scan filters too
        // since we do file-level pruning only (not row-level)
        this.pushedFilters = filters;
        return filters;
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public Scan build() {
        return new DuckLakeScan(schema, requiredSchema, options, pushedFilters);
    }
}
