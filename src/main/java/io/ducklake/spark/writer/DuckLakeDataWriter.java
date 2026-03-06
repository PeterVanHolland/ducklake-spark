package io.ducklake.spark.writer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Writes InternalRow data to a Parquet file and collects column statistics.
 * Each partition gets its own writer instance writing to a unique file.
 */
public class DuckLakeDataWriter implements DataWriter<InternalRow> {

    private final StructType schema;
    private final long[] columnIds;
    private final String absolutePath;
    private final String relativePath;
    private final MessageType parquetSchema;
    private final SimpleGroupFactory groupFactory;
    private final ParquetWriter<Group> writer;
    private final ColumnStatsAccumulator[] statsAccumulators;
    private long recordCount = 0;

    public DuckLakeDataWriter(StructType schema, long[] columnIds,
                               String writeBasePath, String tablePath,
                               int partitionId, long taskAttemptId) {
        this.schema = schema;
        this.columnIds = columnIds;

        String fileName = "ducklake-" + UUID.randomUUID() + ".parquet";
        this.relativePath = tablePath + fileName;
        this.absolutePath = writeBasePath + fileName;

        // Ensure directory exists
        File parentDir = new File(this.absolutePath).getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        this.parquetSchema = buildParquetSchema(schema, columnIds);
        this.groupFactory = new SimpleGroupFactory(parquetSchema);

        // Initialize stats accumulators
        this.statsAccumulators = new ColumnStatsAccumulator[schema.fields().length];
        for (int i = 0; i < schema.fields().length; i++) {
            statsAccumulators[i] = new ColumnStatsAccumulator(schema.fields()[i].dataType());
        }

        try {
            Configuration conf = new Configuration();
            this.writer = ExampleParquetWriter.builder(new Path(this.absolutePath))
                    .withType(parquetSchema)
                    .withConf(conf)
                    .withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Parquet writer at: " + absolutePath, e);
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        Group group = groupFactory.newGroup();
        for (int i = 0; i < schema.fields().length; i++) {
            DataType type = schema.fields()[i].dataType();
            if (row.isNullAt(i)) {
                statsAccumulators[i].addNull();
                continue;
            }
            Object value = writeField(group, i, row, i, type);
            statsAccumulators[i].addValue(value);
        }
        writer.write(group);
        recordCount++;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        writer.close();

        long fileSize = new File(absolutePath).length();

        List<DuckLakeWriterCommitMessage.ColumnStats> colStats = new ArrayList<>();
        for (int i = 0; i < schema.fields().length; i++) {
            ColumnStatsAccumulator acc = statsAccumulators[i];
            colStats.add(new DuckLakeWriterCommitMessage.ColumnStats(
                    columnIds[i],
                    acc.getMinString(),
                    acc.getMaxString(),
                    acc.nullCount,
                    acc.valueCount));
        }

        return new DuckLakeWriterCommitMessage(absolutePath, relativePath,
                recordCount, fileSize, colStats);
    }

    @Override
    public void abort() throws IOException {
        try {
            writer.close();
        } catch (Exception e) {
            // Ignore close errors on abort
        }
        new File(absolutePath).delete();
    }

    @Override
    public void close() throws IOException {
        // Called after commit or abort
    }

    // ---------------------------------------------------------------
    // Parquet schema construction
    // ---------------------------------------------------------------

    private static MessageType buildParquetSchema(StructType sparkSchema, long[] columnIds) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < sparkSchema.fields().length; i++) {
            StructField field = sparkSchema.fields()[i];
            int fieldId = (int) columnIds[i];
            builder.addField(sparkTypeToParquetType(field.name(), field.dataType(), field.nullable(), fieldId));
        }
        return builder.named("spark_schema");
    }

    private static org.apache.parquet.schema.Type sparkTypeToParquetType(
            String name, DataType type, boolean nullable, int fieldId) {
        if (type instanceof BooleanType) {
            return primitive(nullable, PrimitiveTypeName.BOOLEAN, null, fieldId, name);
        } else if (type instanceof ByteType || type instanceof ShortType || type instanceof IntegerType) {
            return primitive(nullable, PrimitiveTypeName.INT32, null, fieldId, name);
        } else if (type instanceof LongType) {
            return primitive(nullable, PrimitiveTypeName.INT64, null, fieldId, name);
        } else if (type instanceof FloatType) {
            return primitive(nullable, PrimitiveTypeName.FLOAT, null, fieldId, name);
        } else if (type instanceof DoubleType) {
            return primitive(nullable, PrimitiveTypeName.DOUBLE, null, fieldId, name);
        } else if (type instanceof StringType) {
            return primitive(nullable, PrimitiveTypeName.BINARY,
                    LogicalTypeAnnotation.stringType(), fieldId, name);
        } else if (type instanceof BinaryType) {
            return primitive(nullable, PrimitiveTypeName.BINARY, null, fieldId, name);
        } else if (type instanceof DateType) {
            return primitive(nullable, PrimitiveTypeName.INT32,
                    LogicalTypeAnnotation.dateType(), fieldId, name);
        } else if (type instanceof TimestampType) {
            return primitive(nullable, PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS),
                    fieldId, name);
        } else if (type instanceof DecimalType) {
            DecimalType dt = (DecimalType) type;
            if (dt.precision() <= 9) {
                return primitive(nullable, PrimitiveTypeName.INT32,
                        LogicalTypeAnnotation.decimalType(dt.scale(), dt.precision()), fieldId, name);
            } else if (dt.precision() <= 18) {
                return primitive(nullable, PrimitiveTypeName.INT64,
                        LogicalTypeAnnotation.decimalType(dt.scale(), dt.precision()), fieldId, name);
            } else {
                int byteLen = computeDecimalByteLength(dt.precision());
                if (nullable) {
                    return Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(byteLen)
                            .as(LogicalTypeAnnotation.decimalType(dt.scale(), dt.precision()))
                            .id(fieldId).named(name);
                } else {
                    return Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(byteLen)
                            .as(LogicalTypeAnnotation.decimalType(dt.scale(), dt.precision()))
                            .id(fieldId).named(name);
                }
            }
        }
        // Fallback: store as string
        return primitive(nullable, PrimitiveTypeName.BINARY,
                LogicalTypeAnnotation.stringType(), fieldId, name);
    }

    private static PrimitiveType primitive(boolean nullable, PrimitiveTypeName typeName,
                                            LogicalTypeAnnotation logicalType, int fieldId, String name) {
        if (logicalType != null) {
            return nullable
                    ? Types.optional(typeName).as(logicalType).id(fieldId).named(name)
                    : Types.required(typeName).as(logicalType).id(fieldId).named(name);
        } else {
            return nullable
                    ? Types.optional(typeName).id(fieldId).named(name)
                    : Types.required(typeName).id(fieldId).named(name);
        }
    }

    private static int computeDecimalByteLength(int precision) {
        return (int) Math.ceil(Math.log(Math.pow(10, precision)) / Math.log(2) / 8.0) + 1;
    }

    // ---------------------------------------------------------------
    // InternalRow → Parquet Group conversion
    // ---------------------------------------------------------------

    private Object writeField(Group group, int fieldIndex, InternalRow row, int ordinal, DataType type) {
        if (type instanceof BooleanType) {
            boolean v = row.getBoolean(ordinal);
            group.add(fieldIndex, v);
            return v;
        } else if (type instanceof ByteType) {
            byte v = row.getByte(ordinal);
            group.add(fieldIndex, (int) v);
            return (int) v;
        } else if (type instanceof ShortType) {
            short v = row.getShort(ordinal);
            group.add(fieldIndex, (int) v);
            return (int) v;
        } else if (type instanceof IntegerType) {
            int v = row.getInt(ordinal);
            group.add(fieldIndex, v);
            return v;
        } else if (type instanceof LongType) {
            long v = row.getLong(ordinal);
            group.add(fieldIndex, v);
            return v;
        } else if (type instanceof FloatType) {
            float v = row.getFloat(ordinal);
            group.add(fieldIndex, v);
            return v;
        } else if (type instanceof DoubleType) {
            double v = row.getDouble(ordinal);
            group.add(fieldIndex, v);
            return v;
        } else if (type instanceof StringType) {
            String v = row.getUTF8String(ordinal).toString();
            group.add(fieldIndex, v);
            return v;
        } else if (type instanceof BinaryType) {
            byte[] v = row.getBinary(ordinal);
            group.add(fieldIndex, Binary.fromReusedByteArray(v));
            return null; // skip binary stats
        } else if (type instanceof DateType) {
            int v = row.getInt(ordinal);
            group.add(fieldIndex, v);
            return v;
        } else if (type instanceof TimestampType) {
            long v = row.getLong(ordinal);
            group.add(fieldIndex, v);
            return v;
        } else if (type instanceof DecimalType) {
            DecimalType dt = (DecimalType) type;
            org.apache.spark.sql.types.Decimal dec = row.getDecimal(ordinal, dt.precision(), dt.scale());
            if (dt.precision() <= 9) {
                int v = (int) dec.toUnscaledLong();
                group.add(fieldIndex, v);
                return v;
            } else if (dt.precision() <= 18) {
                long v = dec.toUnscaledLong();
                group.add(fieldIndex, v);
                return v;
            } else {
                byte[] unscaled = dec.toJavaBigDecimal().unscaledValue().toByteArray();
                int byteLen = computeDecimalByteLength(dt.precision());
                byte[] padded = new byte[byteLen];
                if (unscaled[0] < 0) {
                    Arrays.fill(padded, (byte) 0xFF);
                }
                System.arraycopy(unscaled, 0, padded, padded.length - unscaled.length, unscaled.length);
                group.add(fieldIndex, Binary.fromConstantByteArray(padded));
                return null; // skip stats for large decimals
            }
        }
        // Fallback: write as string
        String v = row.get(ordinal, type).toString();
        group.add(fieldIndex, v);
        return v;
    }

    // ---------------------------------------------------------------
    // Column statistics tracking
    // ---------------------------------------------------------------

    private static class ColumnStatsAccumulator {
        final DataType dataType;
        long nullCount = 0;
        long valueCount = 0;
        Comparable<?> minValue = null;
        Comparable<?> maxValue = null;

        ColumnStatsAccumulator(DataType dataType) {
            this.dataType = dataType;
        }

        void addNull() {
            nullCount++;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        void addValue(Object value) {
            if (value == null) {
                // Binary or unsupported type - track count only
                valueCount++;
                return;
            }
            valueCount++;
            Comparable comp = (Comparable) value;
            if (minValue == null || comp.compareTo(minValue) < 0) {
                minValue = comp;
            }
            if (maxValue == null || comp.compareTo(maxValue) > 0) {
                maxValue = comp;
            }
        }

        String getMinString() {
            return valueToString(minValue, dataType);
        }

        String getMaxString() {
            return valueToString(maxValue, dataType);
        }

        private static String valueToString(Object value, DataType type) {
            if (value == null) return null;
            if (type instanceof DateType) {
                int days = (Integer) value;
                return LocalDate.ofEpochDay(days).toString();
            }
            if (type instanceof TimestampType) {
                long micros = (Long) value;
                Instant instant = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
                LocalDateTime dt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                return dt.toString().replace('T', ' ');
            }
            return value.toString();
        }
    }
}
