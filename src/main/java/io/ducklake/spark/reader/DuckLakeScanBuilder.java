package io.ducklake.spark.reader;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.sources.Filter;

import io.ducklake.spark.catalog.DuckLakeTableMetadataCache;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import java.sql.SQLException;
import java.util.*;

/**
 * Builds scan plans for DuckLake tables. Supports column pruning
 * and filter pushdown via catalog-level file pruning.
 */
public class DuckLakeScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns,
        SupportsPushDownFilters, SupportsPushDownAggregates {

    private final CaseInsensitiveStringMap options;
    private final DuckLakeTableMetadataCache metadataCache;
    private StructType schema;
    private StructType requiredSchema;
    private Filter[] pushedFilters = new Filter[0];
    private Aggregation pushedAggregation = null;
    private boolean completePushDown = false;

    public DuckLakeScanBuilder(StructType schema, CaseInsensitiveStringMap options) {
        this(schema, options, null);
    }

    public DuckLakeScanBuilder(StructType schema, CaseInsensitiveStringMap options,
                                DuckLakeTableMetadataCache metadataCache) {
        this.schema = schema;
        this.requiredSchema = schema;
        this.options = options;
        this.metadataCache = metadataCache;
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
    public boolean pushAggregation(Aggregation aggregation) {
        AggregateFunc[] funcs = aggregation.aggregateExpressions();
        // Only push down simple count(*) with no GROUP BY,
        // and only if we can verify no delete files exist (otherwise count is wrong)
        if (aggregation.groupByExpressions().length == 0 &&
                funcs.length == 1 && funcs[0] instanceof CountStar) {
            // Check for delete files — can't push count if deletes exist
            if (metadataCache != null && !metadataCache.needsFileMetadata() &&
                    (metadataCache.deleteFiles == null || metadataCache.deleteFiles.isEmpty())) {
                this.pushedAggregation = aggregation;
                this.completePushDown = true;
                return true;
            }
            // No cache or has deletes — fall back to full scan
        }
        return false;
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation) {
        return completePushDown;
    }

    @Override
    public Scan build() {
        return new DuckLakeScan(schema, requiredSchema, options, pushedFilters, metadataCache, pushedAggregation);
    }
}
