package io.ducklake.spark.reader;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.FileColumnStats;

import org.apache.spark.sql.sources.*;

import java.util.Map;

/**
 * Evaluates Spark {@link Filter} predicates against DuckLake per-file column
 * statistics (min/max values and null counts).
 *
 * <p>This implements zone-map style pruning: a file can be skipped when its
 * statistics prove that <em>no row</em> in the file can satisfy the filter.
 * All comparisons are conservative — when in doubt, we return {@code true}
 * (the file <em>might</em> match) to avoid false negatives.
 *
 * <p>Statistics values are stored as strings in the DuckLake catalog. Numeric
 * comparisons parse them to {@code double}; string comparisons use natural
 * lexicographic order.
 */
public class DuckLakeFilterEvaluator {

    private final Map<String, FileColumnStats> statsMap;
    private final long recordCount;

    public DuckLakeFilterEvaluator(Map<String, FileColumnStats> statsMap, long recordCount) {
        this.statsMap = statsMap;
        this.recordCount = recordCount;
    }

    /**
     * Returns {@code true} if the file <em>might</em> contain rows matching
     * {@code filter}.  Returns {@code false} only when the stats guarantee
     * that the file cannot match (safe to skip).
     */
    public boolean mightMatch(Filter filter) {
        if (filter instanceof EqualTo) {
            return evalEqualTo((EqualTo) filter);
        } else if (filter instanceof GreaterThan) {
            return evalGreaterThan((GreaterThan) filter);
        } else if (filter instanceof GreaterThanOrEqual) {
            return evalGreaterThanOrEqual((GreaterThanOrEqual) filter);
        } else if (filter instanceof LessThan) {
            return evalLessThan((LessThan) filter);
        } else if (filter instanceof LessThanOrEqual) {
            return evalLessThanOrEqual((LessThanOrEqual) filter);
        } else if (filter instanceof In) {
            return evalIn((In) filter);
        } else if (filter instanceof IsNull) {
            return evalIsNull((IsNull) filter);
        } else if (filter instanceof IsNotNull) {
            return evalIsNotNull((IsNotNull) filter);
        } else if (filter instanceof And) {
            And and = (And) filter;
            return mightMatch(and.left()) && mightMatch(and.right());
        } else if (filter instanceof Or) {
            Or or = (Or) filter;
            return mightMatch(or.left()) || mightMatch(or.right());
        } else if (filter instanceof Not) {
            Not not = (Not) filter;
            Filter child = not.child();
            if (child instanceof IsNull) {
                return evalIsNotNull(new IsNotNull(((IsNull) child).attribute()));
            } else if (child instanceof IsNotNull) {
                return evalIsNull(new IsNull(((IsNotNull) child).attribute()));
            }
            return true; // conservative
        } else {
            return true; // unknown filter — can't prune
        }
    }

    // ---------------------------------------------------------------
    // Individual filter evaluators
    // ---------------------------------------------------------------

    private boolean evalEqualTo(EqualTo f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null || stats.minValue == null || stats.maxValue == null) {
            return true;
        }
        if (stats.valueCount == 0) {
            return false;
        }
        Object value = f.value();
        if (value == null) {
            return stats.nullCount > 0;
        }
        int cmpMin = compareValues(value, stats.minValue);
        int cmpMax = compareValues(value, stats.maxValue);
        return cmpMin >= 0 && cmpMax <= 0;
    }

    private boolean evalGreaterThan(GreaterThan f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null || stats.maxValue == null) {
            return true;
        }
        if (stats.valueCount == 0) {
            return false;
        }
        // col > value  →  skip if max(col) <= value
        int cmp = compareValues(f.value(), stats.maxValue);
        return cmp < 0;
    }

    private boolean evalGreaterThanOrEqual(GreaterThanOrEqual f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null || stats.maxValue == null) {
            return true;
        }
        if (stats.valueCount == 0) {
            return false;
        }
        // col >= value  →  skip if max(col) < value
        int cmp = compareValues(f.value(), stats.maxValue);
        return cmp <= 0;
    }

    private boolean evalLessThan(LessThan f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null || stats.minValue == null) {
            return true;
        }
        if (stats.valueCount == 0) {
            return false;
        }
        // col < value  →  skip if min(col) >= value
        int cmp = compareValues(f.value(), stats.minValue);
        return cmp > 0;
    }

    private boolean evalLessThanOrEqual(LessThanOrEqual f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null || stats.minValue == null) {
            return true;
        }
        if (stats.valueCount == 0) {
            return false;
        }
        // col <= value  →  skip if min(col) > value
        int cmp = compareValues(f.value(), stats.minValue);
        return cmp >= 0;
    }

    private boolean evalIn(In f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null || stats.minValue == null || stats.maxValue == null) {
            return true;
        }
        if (stats.valueCount == 0) {
            return false;
        }
        for (Object value : f.values()) {
            if (value == null) {
                if (stats.nullCount > 0) return true;
                continue;
            }
            int cmpMin = compareValues(value, stats.minValue);
            int cmpMax = compareValues(value, stats.maxValue);
            if (cmpMin >= 0 && cmpMax <= 0) {
                return true;
            }
        }
        return false;
    }

    private boolean evalIsNull(IsNull f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null) {
            return true;
        }
        return stats.nullCount > 0;
    }

    private boolean evalIsNotNull(IsNotNull f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null) {
            return true;
        }
        return stats.valueCount > 0;
    }

    // ---------------------------------------------------------------
    // Partition-value pruning (static)
    // ---------------------------------------------------------------

    /**
     * Check if a filter might match given known partition values for a file.
     * Only handles filters on partition columns (identity transforms).
     * Returns true (conservative) if the filter references non-partition columns.
     */
    public static boolean partitionMightMatch(Filter filter, Map<String, String> partValuesByColName) {
        if (filter instanceof EqualTo) {
            EqualTo f = (EqualTo) filter;
            String partVal = partValuesByColName.get(f.attribute());
            if (partVal == null) return true; // not a partition column
            return compareValues(f.value(), partVal) == 0;
        } else if (filter instanceof In) {
            In f = (In) filter;
            String partVal = partValuesByColName.get(f.attribute());
            if (partVal == null) return true;
            for (Object value : f.values()) {
                if (value != null && compareValues(value, partVal) == 0) {
                    return true;
                }
            }
            return false;
        } else if (filter instanceof GreaterThan) {
            GreaterThan f = (GreaterThan) filter;
            String partVal = partValuesByColName.get(f.attribute());
            if (partVal == null) return true;
            return compareValues(partVal, f.value().toString()) > 0;
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
            String partVal = partValuesByColName.get(f.attribute());
            if (partVal == null) return true;
            return compareValues(partVal, f.value().toString()) >= 0;
        } else if (filter instanceof LessThan) {
            LessThan f = (LessThan) filter;
            String partVal = partValuesByColName.get(f.attribute());
            if (partVal == null) return true;
            return compareValues(partVal, f.value().toString()) < 0;
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual f = (LessThanOrEqual) filter;
            String partVal = partValuesByColName.get(f.attribute());
            if (partVal == null) return true;
            return compareValues(partVal, f.value().toString()) <= 0;
        } else if (filter instanceof And) {
            And and = (And) filter;
            return partitionMightMatch(and.left(), partValuesByColName)
                    && partitionMightMatch(and.right(), partValuesByColName);
        } else if (filter instanceof Or) {
            Or or = (Or) filter;
            return partitionMightMatch(or.left(), partValuesByColName)
                    || partitionMightMatch(or.right(), partValuesByColName);
        } else if (filter instanceof Not) {
            // Conservative: can't negate partition pruning safely in general
            return true;
        } else if (filter instanceof IsNotNull) {
            String partVal = partValuesByColName.get(((IsNotNull) filter).attribute());
            if (partVal == null) return true;
            return true; // partition values are always non-null for identity
        } else if (filter instanceof IsNull) {
            String partVal = partValuesByColName.get(((IsNull) filter).attribute());
            if (partVal == null) return true;
            return false; // identity partition values are never null
        }
        return true; // conservative
    }

    // ---------------------------------------------------------------
    // Value comparison
    // ---------------------------------------------------------------

    /**
     * Compare a filter literal against a stat value (stored as String).
     * Tries numeric comparison first; falls back to lexicographic.
     */
    static int compareValues(Object filterValue, String statValue) {
        if (filterValue == null || statValue == null) {
            return 0; // conservative
        }
        String filterStr = filterValue.toString();
        try {
            double fv = Double.parseDouble(filterStr);
            double sv = Double.parseDouble(statValue);
            return Double.compare(fv, sv);
        } catch (NumberFormatException e) {
            // fall through to string compare
        }
        return filterStr.compareTo(statValue);
    }
}
