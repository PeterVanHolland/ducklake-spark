package io.ducklake.spark.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Reads inlined data rows (stored in the catalog database) as InternalRows.
 * Parses string values back to their typed representations.
 */
public class DuckLakeInlinedPartitionReader implements PartitionReader<InternalRow> {

    private final List<Map<String, String>> rows;
    private final StructType requiredSchema;
    private int currentIndex = -1;

    public DuckLakeInlinedPartitionReader(DuckLakeInlinedInputPartition partition,
                                           StructType requiredSchema) {
        this.rows = partition.getRows();
        this.requiredSchema = requiredSchema;
    }

    @Override
    public boolean next() throws IOException {
        currentIndex++;
        return currentIndex < rows.size();
    }

    @Override
    public InternalRow get() {
        Map<String, String> row = rows.get(currentIndex);
        Object[] values = new Object[requiredSchema.fields().length];

        for (int i = 0; i < requiredSchema.fields().length; i++) {
            StructField field = requiredSchema.fields()[i];
            String strValue = row.get(field.name());
            if (strValue == null) {
                values[i] = null;
            } else {
                values[i] = parseValue(strValue, field.dataType());
            }
        }
        return new GenericInternalRow(values);
    }

    @Override
    public void close() throws IOException {
        // Nothing to close
    }

    /**
     * Parse a string value from the catalog database into a typed Spark value.
     */
    private static Object parseValue(String str, DataType type) {
        if (str == null) return null;
        try {
            if (type instanceof BooleanType) {
                return "1".equals(str) || "true".equalsIgnoreCase(str);
            } else if (type instanceof ByteType) {
                return Byte.parseByte(str);
            } else if (type instanceof ShortType) {
                return Short.parseShort(str);
            } else if (type instanceof IntegerType) {
                return Integer.parseInt(str);
            } else if (type instanceof LongType) {
                return Long.parseLong(str);
            } else if (type instanceof FloatType) {
                return Float.parseFloat(str);
            } else if (type instanceof DoubleType) {
                return Double.parseDouble(str);
            } else if (type instanceof StringType) {
                return UTF8String.fromString(str);
            } else if (type instanceof DateType) {
                return (int) LocalDate.parse(str).toEpochDay();
            } else if (type instanceof TimestampType) {
                LocalDateTime ldt = LocalDateTime.parse(str);
                long epochSecond = ldt.toEpochSecond(java.time.ZoneOffset.UTC);
                int nano = ldt.getNano();
                return epochSecond * 1_000_000 + nano / 1000; // micros
            } else if (type instanceof DecimalType) {
                DecimalType dt = (DecimalType) type;
                java.math.BigDecimal bd = new java.math.BigDecimal(str);
                org.apache.spark.sql.types.Decimal d = new org.apache.spark.sql.types.Decimal();
                d.set(new scala.math.BigDecimal(bd), dt.precision(), dt.scale());
                return d;
            } else {
                return UTF8String.fromString(str);
            }
        } catch (Exception e) {
            return null;
        }
    }
}
