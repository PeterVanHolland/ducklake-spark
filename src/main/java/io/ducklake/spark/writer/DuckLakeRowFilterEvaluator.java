package io.ducklake.spark.writer;

import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

/**
 * Evaluates Spark {@link Filter} predicates against individual Parquet rows.
 * Used by delete/update operations to identify rows matching a WHERE clause.
 */
public class DuckLakeRowFilterEvaluator {

    private final StructType sparkSchema;
    private final MessageType fileSchema;
    private final Map<String, String> logicalToPhysical;

    public DuckLakeRowFilterEvaluator(StructType sparkSchema, MessageType fileSchema,
                                       Map<String, String> logicalToPhysical) {
        this.sparkSchema = sparkSchema;
        this.fileSchema = fileSchema;
        this.logicalToPhysical = logicalToPhysical;
    }

    /**
     * Returns {@code true} if the given row matches all the provided filters.
     */
    public boolean matches(Group group, Filter[] filters) {
        if (filters == null || filters.length == 0) {
            return true;
        }
        for (Filter filter : filters) {
            if (!evaluateFilter(group, filter)) {
                return false;
            }
        }
        return true;
    }

    private boolean evaluateFilter(Group group, Filter filter) {
        if (filter instanceof EqualTo) {
            return evalEqualTo(group, (EqualTo) filter);
        } else if (filter instanceof EqualNullSafe) {
            return evalEqualNullSafe(group, (EqualNullSafe) filter);
        } else if (filter instanceof GreaterThan) {
            return evalGreaterThan(group, (GreaterThan) filter);
        } else if (filter instanceof GreaterThanOrEqual) {
            return evalGreaterThanOrEqual(group, (GreaterThanOrEqual) filter);
        } else if (filter instanceof LessThan) {
            return evalLessThan(group, (LessThan) filter);
        } else if (filter instanceof LessThanOrEqual) {
            return evalLessThanOrEqual(group, (LessThanOrEqual) filter);
        } else if (filter instanceof In) {
            return evalIn(group, (In) filter);
        } else if (filter instanceof IsNull) {
            return evalIsNull(group, (IsNull) filter);
        } else if (filter instanceof IsNotNull) {
            return evalIsNotNull(group, (IsNotNull) filter);
        } else if (filter instanceof And) {
            And and = (And) filter;
            return evaluateFilter(group, and.left()) && evaluateFilter(group, and.right());
        } else if (filter instanceof Or) {
            Or or = (Or) filter;
            return evaluateFilter(group, or.left()) || evaluateFilter(group, or.right());
        } else if (filter instanceof Not) {
            return !evaluateFilter(group, ((Not) filter).child());
        } else if (filter instanceof StringStartsWith) {
            return evalStringStartsWith(group, (StringStartsWith) filter);
        } else if (filter instanceof StringEndsWith) {
            return evalStringEndsWith(group, (StringEndsWith) filter);
        } else if (filter instanceof StringContains) {
            return evalStringContains(group, (StringContains) filter);
        }
        return true;
    }

    // ---------------------------------------------------------------
    // Individual filter evaluators
    // ---------------------------------------------------------------

    private boolean evalEqualTo(Group group, EqualTo f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null) return f.value() == null;
        if (f.value() == null) return false;
        return compareValues(rowVal, f.value()) == 0;
    }

    private boolean evalEqualNullSafe(Group group, EqualNullSafe f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null && f.value() == null) return true;
        if (rowVal == null || f.value() == null) return false;
        return compareValues(rowVal, f.value()) == 0;
    }

    private boolean evalGreaterThan(Group group, GreaterThan f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null) return false;
        return compareValues(rowVal, f.value()) > 0;
    }

    private boolean evalGreaterThanOrEqual(Group group, GreaterThanOrEqual f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null) return false;
        return compareValues(rowVal, f.value()) >= 0;
    }

    private boolean evalLessThan(Group group, LessThan f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null) return false;
        return compareValues(rowVal, f.value()) < 0;
    }

    private boolean evalLessThanOrEqual(Group group, LessThanOrEqual f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null) return false;
        return compareValues(rowVal, f.value()) <= 0;
    }

    private boolean evalIn(Group group, In f) {
        Object rowVal = getColumnValue(group, f.attribute());
        for (Object val : f.values()) {
            if (rowVal == null && val == null) return true;
            if (rowVal != null && val != null && compareValues(rowVal, val) == 0) return true;
        }
        return false;
    }

    private boolean evalIsNull(Group group, IsNull f) {
        return getColumnValue(group, f.attribute()) == null;
    }

    private boolean evalIsNotNull(Group group, IsNotNull f) {
        return getColumnValue(group, f.attribute()) != null;
    }

    private boolean evalStringStartsWith(Group group, StringStartsWith f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null) return false;
        return rowVal.toString().startsWith(f.value().toString());
    }

    private boolean evalStringEndsWith(Group group, StringEndsWith f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null) return false;
        return rowVal.toString().endsWith(f.value().toString());
    }

    private boolean evalStringContains(Group group, StringContains f) {
        Object rowVal = getColumnValue(group, f.attribute());
        if (rowVal == null) return false;
        return rowVal.toString().contains(f.value().toString());
    }

    // ---------------------------------------------------------------
    // Value extraction and comparison
    // ---------------------------------------------------------------

    private Object getColumnValue(Group group, String logicalName) {
        String physicalName = logicalToPhysical.getOrDefault(logicalName, logicalName);

        int fieldIndex;
        try {
            fieldIndex = fileSchema.getFieldIndex(physicalName);
        } catch (Exception e) {
            return null;
        }

        if (group.getFieldRepetitionCount(fieldIndex) == 0) {
            return null;
        }

        DataType sparkType = null;
        for (StructField field : sparkSchema.fields()) {
            if (field.name().equalsIgnoreCase(logicalName)) {
                sparkType = field.dataType();
                break;
            }
        }
        if (sparkType == null) {
            return group.getValueToString(fieldIndex, 0);
        }

        return readTypedValue(group, fieldIndex, sparkType);
    }

    private Object readTypedValue(Group group, int fieldIndex, DataType sparkType) {
        try {
            if (sparkType instanceof BooleanType) {
                return group.getBoolean(fieldIndex, 0);
            } else if (sparkType instanceof ByteType) {
                return (int) group.getInteger(fieldIndex, 0);
            } else if (sparkType instanceof ShortType) {
                return (int) group.getInteger(fieldIndex, 0);
            } else if (sparkType instanceof IntegerType) {
                return group.getInteger(fieldIndex, 0);
            } else if (sparkType instanceof LongType) {
                return group.getLong(fieldIndex, 0);
            } else if (sparkType instanceof FloatType) {
                return (double) group.getFloat(fieldIndex, 0);
            } else if (sparkType instanceof DoubleType) {
                return group.getDouble(fieldIndex, 0);
            } else if (sparkType instanceof StringType) {
                return group.getString(fieldIndex, 0);
            } else {
                return group.getValueToString(fieldIndex, 0);
            }
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static int compareValues(Object rowValue, Object filterValue) {
        if (rowValue == null && filterValue == null) return 0;
        if (rowValue == null) return -1;
        if (filterValue == null) return 1;

        if (isNumeric(rowValue) && isNumeric(filterValue)) {
            double rv = ((Number) rowValue).doubleValue();
            double fv = ((Number) filterValue).doubleValue();
            return Double.compare(rv, fv);
        }

        if (rowValue instanceof String || filterValue instanceof String) {
            return rowValue.toString().compareTo(filterValue.toString());
        }

        if (rowValue instanceof Boolean && filterValue instanceof Boolean) {
            return Boolean.compare((Boolean) rowValue, (Boolean) filterValue);
        }

        if (rowValue instanceof Comparable && filterValue instanceof Comparable) {
            try {
                return ((Comparable) rowValue).compareTo(filterValue);
            } catch (ClassCastException e) {
                return rowValue.toString().compareTo(filterValue.toString());
            }
        }

        return rowValue.toString().compareTo(filterValue.toString());
    }

    private static boolean isNumeric(Object value) {
        return value instanceof Number;
    }
}
