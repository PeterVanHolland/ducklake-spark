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
        } else if (filter instanceof StringStartsWith) {
            return evalStringStartsWith((StringStartsWith) filter);
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

    private boolean evalStringStartsWith(StringStartsWith f) {
        FileColumnStats stats = statsMap.get(f.attribute());
        if (stats == null || stats.minValue == null || stats.maxValue == null) {
            return true;
        }
        if (stats.valueCount == 0) {
            return false;
        }
        String prefix = f.value().toString();
        // File can be skipped if max < prefix or min starts-after prefix range
        // Conservative: if max < prefix, no values can match
        if (stats.maxValue.compareTo(prefix) < 0) {
            return false;
        }
        // If min starts after the prefix range (prefix + max char), skip
        String prefixEnd = prefix.substring(0, prefix.length() - 1) +
                (char) (prefix.charAt(prefix.length() - 1) + 1);
        if (stats.minValue.compareTo(prefixEnd) >= 0) {
            return false;
        }
        return true;
    }

    // ---------------------------------------------------------------
    // Value comparison
    // ---------------------------------------------------------------

    /**
     * Compare a filter literal against a stat value (stored as String).
     * Handles numeric, date, timestamp, and string comparisons.
     *
     * Type dispatch order:
     *   1. Boolean values (true=1, false=0)
     *   2. Integer types (byte, short, int, long)
     *   3. Floating point (float, double)
     *   4. BigDecimal
     *   5. Date (epoch days → ISO string)
     *   6. Timestamp (epoch micros → ISO string)
     *   7. Numeric string fallback (parseDouble)
     *   8. Lexicographic string comparison
     */
    static int compareValues(Object filterValue, String statValue) {
        if (filterValue == null || statValue == null) {
            return 0; // conservative
        }

        // Handle typed filter values directly
        if (filterValue instanceof Boolean) {
            String fStr = ((Boolean) filterValue) ? "1" : "0";
            return fStr.compareTo(statValue);
        }

        if (filterValue instanceof Integer || filterValue instanceof Long ||
            filterValue instanceof Short || filterValue instanceof Byte) {
            try {
                long fv = ((Number) filterValue).longValue();
                long sv = Long.parseLong(statValue);
                return Long.compare(fv, sv);
            } catch (NumberFormatException e) {
                // fall through
            }
        }

        if (filterValue instanceof Float || filterValue instanceof Double) {
            try {
                double fv = ((Number) filterValue).doubleValue();
                double sv = Double.parseDouble(statValue);
                return Double.compare(fv, sv);
            } catch (NumberFormatException e) {
                // fall through
            }
        }

        if (filterValue instanceof java.math.BigDecimal) {
            try {
                java.math.BigDecimal fv = (java.math.BigDecimal) filterValue;
                java.math.BigDecimal sv = new java.math.BigDecimal(statValue);
                return fv.compareTo(sv);
            } catch (NumberFormatException e) {
                // fall through
            }
        }

        // String fallback: try numeric, then lexicographic
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
