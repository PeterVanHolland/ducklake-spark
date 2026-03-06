package io.ducklake.spark.util;

import org.apache.spark.sql.types.*;
import java.util.List;

/**
 * Maps DuckDB/DuckLake type strings to Spark SQL types.
 */
public class DuckLakeTypeMapping {

    /**
     * Convert a DuckDB type string to a Spark DataType.
     */
    public static DataType toSparkType(String duckdbType) {
        if (duckdbType == null) {
            return DataTypes.StringType;
        }
        String upper = duckdbType.toUpperCase().trim();

        // Integer types
        if (upper.equals("TINYINT") || upper.equals("INT1")) return DataTypes.ByteType;
        if (upper.equals("SMALLINT") || upper.equals("INT2") || upper.equals("SHORT")) return DataTypes.ShortType;
        if (upper.equals("INTEGER") || upper.equals("INT4") || upper.equals("INT") || upper.equals("SIGNED")) return DataTypes.IntegerType;
        if (upper.equals("BIGINT") || upper.equals("INT8") || upper.equals("LONG")) return DataTypes.LongType;

        // Unsigned integers → promote
        if (upper.equals("UTINYINT")) return DataTypes.ShortType;
        if (upper.equals("USMALLINT")) return DataTypes.IntegerType;
        if (upper.equals("UINTEGER")) return DataTypes.LongType;
        if (upper.equals("UBIGINT")) return new DecimalType(20, 0);

        // Float types
        if (upper.equals("FLOAT") || upper.equals("FLOAT4") || upper.equals("REAL")) return DataTypes.FloatType;
        if (upper.equals("DOUBLE") || upper.equals("FLOAT8")) return DataTypes.DoubleType;

        // Decimal
        if (upper.startsWith("DECIMAL") || upper.startsWith("NUMERIC")) {
            int[] pd = parseDecimalPrecisionScale(upper);
            return new DecimalType(pd[0], pd[1]);
        }

        // Boolean
        if (upper.equals("BOOLEAN") || upper.equals("BOOL") || upper.equals("LOGICAL")) return DataTypes.BooleanType;

        // String types
        if (upper.equals("VARCHAR") || upper.equals("TEXT") || upper.equals("STRING") || upper.equals("CHAR")
                || upper.startsWith("VARCHAR(") || upper.startsWith("CHAR(")
                || upper.equals("BPCHAR") || upper.equals("NAME")) {
            return DataTypes.StringType;
        }

        // Binary
        if (upper.equals("BLOB") || upper.equals("BYTEA") || upper.equals("BINARY") || upper.equals("VARBINARY")) {
            return DataTypes.BinaryType;
        }

        // Date/time
        if (upper.equals("DATE")) return DataTypes.DateType;
        if (upper.equals("TIME") || upper.equals("TIMETZ") || upper.equals("TIME_NS")) return DataTypes.StringType;
        if (upper.startsWith("TIMESTAMP")) return DataTypes.TimestampType;

        // Interval
        if (upper.equals("INTERVAL")) return DataTypes.StringType;

        // UUID
        if (upper.equals("UUID")) return DataTypes.StringType;

        // JSON
        if (upper.equals("JSON")) return DataTypes.StringType;

        // HUGEINT
        if (upper.equals("HUGEINT")) return new DecimalType(38, 0);
        if (upper.equals("UHUGEINT")) return new DecimalType(38, 0);

        // ENUM → String
        if (upper.startsWith("ENUM")) return DataTypes.StringType;

        // LIST/ARRAY
        if (upper.startsWith("LIST(") || upper.endsWith("[]")) {
            String inner = extractInnerType(upper, "LIST");
            return new ArrayType(toSparkType(inner), true);
        }

        // MAP
        if (upper.startsWith("MAP(")) {
            String[] kv = extractMapTypes(upper);
            return new MapType(toSparkType(kv[0]), toSparkType(kv[1]), true);
        }

        // STRUCT
        if (upper.startsWith("STRUCT(")) {
            return parseStructType(duckdbType);
        }

        // BIT, GEOMETRY, VARIANT → String
        if (upper.equals("BIT") || upper.equals("GEOMETRY") || upper.equals("VARIANT")) {
            return DataTypes.StringType;
        }

        // Fallback
        return DataTypes.StringType;
    }

    private static int[] parseDecimalPrecisionScale(String type) {
        int start = type.indexOf('(');
        int end = type.indexOf(')');
        if (start < 0 || end < 0) return new int[]{18, 3};
        String inner = type.substring(start + 1, end);
        String[] parts = inner.split(",");
        int precision = Integer.parseInt(parts[0].trim());
        int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
        return new int[]{precision, scale};
    }

    private static String extractInnerType(String type, String wrapper) {
        if (type.toUpperCase().startsWith(wrapper + "(")) {
            return type.substring(wrapper.length() + 1, type.length() - 1).trim();
        }
        if (type.endsWith("[]")) {
            return type.substring(0, type.length() - 2).trim();
        }
        return "VARCHAR";
    }

    private static String[] extractMapTypes(String type) {
        String inner = type.substring(4, type.length() - 1);
        int depth = 0;
        int split = -1;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                split = i;
                break;
            }
        }
        if (split < 0) return new String[]{"VARCHAR", "VARCHAR"};
        return new String[]{inner.substring(0, split).trim(), inner.substring(split + 1).trim()};
    }

    private static StructType parseStructType(String type) {
        String inner = type.substring(7, type.length() - 1);
        List<StructField> fields = new java.util.ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i <= inner.length(); i++) {
            if (i == inner.length() || (inner.charAt(i) == ',' && depth == 0)) {
                String fieldDef = inner.substring(start, i).trim();
                int spaceIdx = fieldDef.indexOf(' ');
                if (spaceIdx > 0) {
                    String name = fieldDef.substring(0, spaceIdx).trim();
                    String fieldType = fieldDef.substring(spaceIdx + 1).trim();
                    fields.add(new StructField(name, toSparkType(fieldType), true, Metadata.empty()));
                }
                start = i + 1;
            } else if (inner.charAt(i) == '(') {
                depth++;
            } else if (inner.charAt(i) == ')') {
                depth--;
            }
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    /**
     * Build a Spark StructType from DuckLake column definitions.
     */
    public static StructType buildSchema(
            java.util.List<io.ducklake.spark.catalog.DuckLakeMetadataBackend.ColumnInfo> columns) {
        StructField[] fields = new StructField[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            var col = columns.get(i);
            fields[i] = new StructField(col.name, toSparkType(col.type), col.nullable, Metadata.empty());
        }
        return new StructType(fields);
    }
}
