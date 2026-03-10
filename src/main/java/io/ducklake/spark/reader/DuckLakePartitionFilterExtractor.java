package io.ducklake.spark.reader;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.PartitionInfo;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.PartitionFilter;

import org.apache.spark.sql.sources.*;

import java.util.*;

/**
 * Extracts partition filters from Spark Filter predicates.
 *
 * This class analyzes pushed-down Spark filters and identifies those
 * that can be used for partition pruning. Only filters on partition
 * columns are converted to PartitionFilter objects.
 */
public class DuckLakePartitionFilterExtractor {

    private final Map<String, PartitionInfo> partitionColumns;

    public DuckLakePartitionFilterExtractor(List<PartitionInfo> partitionInfos) {
        this.partitionColumns = new HashMap<>();
        for (PartitionInfo info : partitionInfos) {
            this.partitionColumns.put(info.columnName, info);
        }
    }

    /**
     * Extract partition filters from the given Spark filters.
     * Only filters on partition columns are considered.
     */
    public List<PartitionFilter> extractPartitionFilters(Filter[] filters) {
        List<PartitionFilter> result = new ArrayList<>();
        if (filters == null) {
            return result;
        }

        for (Filter filter : filters) {
            extractFromFilter(filter, result);
        }

        return result;
    }

    private void extractFromFilter(Filter filter, List<PartitionFilter> result) {
        if (filter instanceof EqualTo) {
            EqualTo eq = (EqualTo) filter;
            PartitionInfo partInfo = partitionColumns.get(eq.attribute());
            if (partInfo != null) {
                result.add(new PartitionFilter(partInfo.partitionIndex, "=", eq.value()));
            }
        } else if (filter instanceof In) {
            In in = (In) filter;
            PartitionInfo partInfo = partitionColumns.get(in.attribute());
            if (partInfo != null) {
                result.add(new PartitionFilter(partInfo.partitionIndex, "IN", Arrays.asList(in.values())));
            }
        } else if (filter instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan) filter;
            PartitionInfo partInfo = partitionColumns.get(gt.attribute());
            if (partInfo != null) {
                result.add(new PartitionFilter(partInfo.partitionIndex, ">", gt.value()));
            }
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual gte = (GreaterThanOrEqual) filter;
            PartitionInfo partInfo = partitionColumns.get(gte.attribute());
            if (partInfo != null) {
                result.add(new PartitionFilter(partInfo.partitionIndex, ">=", gte.value()));
            }
        } else if (filter instanceof LessThan) {
            LessThan lt = (LessThan) filter;
            PartitionInfo partInfo = partitionColumns.get(lt.attribute());
            if (partInfo != null) {
                result.add(new PartitionFilter(partInfo.partitionIndex, "<", lt.value()));
            }
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lte = (LessThanOrEqual) filter;
            PartitionInfo partInfo = partitionColumns.get(lte.attribute());
            if (partInfo != null) {
                result.add(new PartitionFilter(partInfo.partitionIndex, "<=", lte.value()));
            }
        } else if (filter instanceof And) {
            And and = (And) filter;
            extractFromFilter(and.left(), result);
            extractFromFilter(and.right(), result);
        } else if (filter instanceof Or) {
            // For OR filters, we can't easily combine partition filters
            // across different partition columns, so we skip OR for now
            // This is conservative - we could optimize this later
        }
        // Note: We don't handle NOT filters for partition pruning as they
        // would require more complex logic to determine which partitions to exclude
    }
}