package io.ducklake.spark.writer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.*;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;

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
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.PartitionInfo;

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
    private final Map<Integer, Integer> partitionIndexToFieldIndex; // partition index -> field index in schema
    private final Map<Integer, String> partitionValues; // partition index -> partition value (consistent across all rows)
    private long recordCount = 0;

    public DuckLakeDataWriter(StructType schema, long[] columnIds,
                               String writeBasePath, String tablePath,
                               int partitionId, long taskAttemptId) {
        this(schema, columnIds, writeBasePath, tablePath, partitionId, taskAttemptId, null);
    }

    public DuckLakeDataWriter(StructType schema, long[] columnIds,
                               String writeBasePath, String tablePath,
                               int partitionId, long taskAttemptId,
                               List<PartitionInfo> partitionInfos) {
        this.schema = schema;
        this.columnIds = columnIds;

        String fileName = "ducklake-" + UUID.randomUUID() + ".parquet";
        this.relativePath = tablePath + fileName;
        this.absolutePath = writeBasePath + fileName;

        // Initialize partition tracking
        this.partitionIndexToFieldIndex = new HashMap<>();
        this.partitionValues = new HashMap<>();
        if (partitionInfos != null) {
            for (PartitionInfo partInfo : partitionInfos) {
                for (int fieldIndex = 0; fieldIndex < schema.fields().length; fieldIndex++) {
                    if (schema.fields()[fieldIndex].name().equals(partInfo.columnName)) {
                        this.partitionIndexToFieldIndex.put(partInfo.partitionIndex, fieldIndex);
                        break;
                    }
                }
            }
        }

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

        // Extract partition values from this row (should be consistent across all rows in the file)
        for (Map.Entry<Integer, Integer> entry : partitionIndexToFieldIndex.entrySet()) {
            int partitionIndex = entry.getKey();
            int fieldIndex = entry.getValue();

            String partitionValue;
            if (row.isNullAt(fieldIndex)) {
                partitionValue = null;
            } else {
                DataType fieldType = schema.fields()[fieldIndex].dataType();
                Object value = extractValue(row, fieldIndex, fieldType);
                partitionValue = value != null ? value.toString() : null;
            }

            // Store or validate partition value consistency
            if (partitionValues.containsKey(partitionIndex)) {
                String existingValue = partitionValues.get(partitionIndex);
                if (!Objects.equals(existingValue, partitionValue)) {
                    throw new IllegalStateException("Inconsistent partition values within a single file: " +
                            "partition index " + partitionIndex + " has both '" + existingValue +
                            "' and '" + partitionValue + "'");
                }
            } else {
                partitionValues.put(partitionIndex, partitionValue);
            }
        }

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
                recordCount, fileSize, colStats, partitionValues);
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
    // Value extraction utilities
    // ---------------------------------------------------------------

    private Object extractValue(InternalRow row, int fieldIndex, DataType dataType) {
        if (row.isNullAt(fieldIndex)) {
            return null;
        }

        if (dataType instanceof StringType) {
            return row.getString(fieldIndex);
        } else if (dataType instanceof IntegerType) {
            return row.getInt(fieldIndex);
        } else if (dataType instanceof LongType) {
            return row.getLong(fieldIndex);
        } else if (dataType instanceof DoubleType) {
            return row.getDouble(fieldIndex);
        } else if (dataType instanceof FloatType) {
            return row.getFloat(fieldIndex);
        } else if (dataType instanceof BooleanType) {
            return row.getBoolean(fieldIndex);
        } else if (dataType instanceof DateType) {
            return row.getInt(fieldIndex); // Date stored as days since epoch
        } else if (dataType instanceof TimestampType) {
            return row.getLong(fieldIndex); // Timestamp stored as microseconds since epoch
        } else {
            // For complex types, use toString
            return row.get(fieldIndex, dataType);
        }
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
        } else if (type instanceof ArrayType) {
            // Standard 3-level Parquet LIST encoding
            ArrayType at = (ArrayType) type;
            org.apache.parquet.schema.Type elementType =
                    sparkTypeToParquetType("element", at.elementType(), at.containsNull(), 0);
            GroupType repeatedList = Types.repeatedGroup()
                    .addField(elementType)
                    .named("list");
            if (nullable) {
                return Types.optionalGroup()
                        .as(LogicalTypeAnnotation.listType())
                        .addField(repeatedList)
                        .id(fieldId)
                        .named(name);
            } else {
                return Types.requiredGroup()
                        .as(LogicalTypeAnnotation.listType())
                        .addField(repeatedList)
                        .id(fieldId)
                        .named(name);
            }
        } else if (type instanceof StructType) {
            StructType st = (StructType) type;
            Types.GroupBuilder<GroupType> builder = nullable
                    ? Types.optionalGroup() : Types.requiredGroup();
            for (int i = 0; i < st.fields().length; i++) {
                StructField f = st.fields()[i];
                builder.addField(sparkTypeToParquetType(f.name(), f.dataType(), f.nullable(), 0));
            }
            return builder.id(fieldId).named(name);
        } else if (type instanceof MapType) {
            // Standard Parquet MAP encoding
            MapType mt = (MapType) type;
            org.apache.parquet.schema.Type keyType =
                    sparkTypeToParquetType("key", mt.keyType(), false, 0);
            org.apache.parquet.schema.Type valueType =
                    sparkTypeToParquetType("value", mt.valueType(), mt.valueContainsNull(), 0);
            GroupType repeatedKeyValue = Types.repeatedGroup()
                    .addField(keyType)
                    .addField(valueType)
                    .named("key_value");
            if (nullable) {
                return Types.optionalGroup()
                        .as(LogicalTypeAnnotation.mapType())
                        .addField(repeatedKeyValue)
                        .id(fieldId)
                        .named(name);
            } else {
                return Types.requiredGroup()
                        .as(LogicalTypeAnnotation.mapType())
                        .addField(repeatedKeyValue)
                        .id(fieldId)
                        .named(name);
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
    // InternalRow -> Parquet Group conversion
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
        } else if (type instanceof ArrayType) {
            ArrayType at = (ArrayType) type;
            ArrayData arrayData = row.getArray(ordinal);
            writeArrayToGroup(group, fieldIndex, arrayData, at.elementType());
            return null; // skip stats for complex types
        } else if (type instanceof StructType) {
            StructType st = (StructType) type;
            InternalRow structRow = row.getStruct(ordinal, st.fields().length);
            writeStructToGroup(group, fieldIndex, structRow, st);
            return null; // skip stats for complex types
        } else if (type instanceof MapType) {
            MapType mt = (MapType) type;
            MapData mapData = row.getMap(ordinal);
            writeMapToGroup(group, fieldIndex, mapData, mt.keyType(), mt.valueType());
            return null; // skip stats for complex types
        }
        // Fallback: write as string
        String v = row.get(ordinal, type).toString();
        group.add(fieldIndex, v);
        return v;
    }

    // ---------------------------------------------------------------
    // Complex type writers
    // ---------------------------------------------------------------

    /**
     * Write an ArrayData as a Parquet LIST group (3-level encoding).
     * Schema: group (LIST) { repeated group list { optional element } }
     */
    private void writeArrayToGroup(Group parentGroup, int fieldIndex,
                                    ArrayData arrayData, DataType elementType) {
        Group listGroup = parentGroup.addGroup(fieldIndex);
        for (int j = 0; j < arrayData.numElements(); j++) {
            Group elementGroup = listGroup.addGroup(0); // "list" repeated group
            if (!arrayData.isNullAt(j)) {
                writeValueFromArrayData(elementGroup, 0, arrayData, j, elementType);
            }
            // null elements: the optional "element" field is simply not populated
        }
    }

    /**
     * Write an InternalRow (struct) as a Parquet group with named fields.
     */
    private void writeStructToGroup(Group parentGroup, int fieldIndex,
                                     InternalRow structRow, StructType structType) {
        Group structGroup = parentGroup.addGroup(fieldIndex);
        for (int j = 0; j < structType.fields().length; j++) {
            if (!structRow.isNullAt(j)) {
                writeField(structGroup, j, structRow, j, structType.fields()[j].dataType());
            }
        }
    }

    /**
     * Write a MapData as a Parquet MAP group.
     * Schema: group (MAP) { repeated group key_value { required key; optional value } }
     */
    private void writeMapToGroup(Group parentGroup, int fieldIndex,
                                  MapData mapData, DataType keyType, DataType valueType) {
        Group mapGroup = parentGroup.addGroup(fieldIndex);
        ArrayData keys = mapData.keyArray();
        ArrayData values = mapData.valueArray();
        for (int j = 0; j < mapData.numElements(); j++) {
            Group kvGroup = mapGroup.addGroup(0); // "key_value" repeated group
            writeValueFromArrayData(kvGroup, 0, keys, j, keyType);
            if (!values.isNullAt(j)) {
                writeValueFromArrayData(kvGroup, 1, values, j, valueType);
            }
        }
    }

    /**
     * Write a single value from an ArrayData to a Parquet Group field.
     * Handles all primitive types and recurses for nested complex types.
     */
    private void writeValueFromArrayData(Group group, int fieldIndex,
                                          ArrayData array, int ordinal, DataType type) {
        if (type instanceof BooleanType) {
            group.add(fieldIndex, array.getBoolean(ordinal));
        } else if (type instanceof ByteType) {
            group.add(fieldIndex, (int) array.getByte(ordinal));
        } else if (type instanceof ShortType) {
            group.add(fieldIndex, (int) array.getShort(ordinal));
        } else if (type instanceof IntegerType) {
            group.add(fieldIndex, array.getInt(ordinal));
        } else if (type instanceof LongType) {
            group.add(fieldIndex, array.getLong(ordinal));
        } else if (type instanceof FloatType) {
            group.add(fieldIndex, array.getFloat(ordinal));
        } else if (type instanceof DoubleType) {
            group.add(fieldIndex, array.getDouble(ordinal));
        } else if (type instanceof StringType) {
            group.add(fieldIndex, array.getUTF8String(ordinal).toString());
        } else if (type instanceof BinaryType) {
            group.add(fieldIndex, Binary.fromReusedByteArray(array.getBinary(ordinal)));
        } else if (type instanceof DateType) {
            group.add(fieldIndex, array.getInt(ordinal));
        } else if (type instanceof TimestampType) {
            group.add(fieldIndex, array.getLong(ordinal));
        } else if (type instanceof DecimalType) {
            DecimalType dt = (DecimalType) type;
            org.apache.spark.sql.types.Decimal dec = array.getDecimal(ordinal, dt.precision(), dt.scale());
            if (dt.precision() <= 9) {
                group.add(fieldIndex, (int) dec.toUnscaledLong());
            } else if (dt.precision() <= 18) {
                group.add(fieldIndex, dec.toUnscaledLong());
            } else {
                byte[] unscaled = dec.toJavaBigDecimal().unscaledValue().toByteArray();
                int byteLen = computeDecimalByteLength(dt.precision());
                byte[] padded = new byte[byteLen];
                if (unscaled[0] < 0) {
                    Arrays.fill(padded, (byte) 0xFF);
                }
                System.arraycopy(unscaled, 0, padded, padded.length - unscaled.length, unscaled.length);
                group.add(fieldIndex, Binary.fromConstantByteArray(padded));
            }
        } else if (type instanceof ArrayType) {
            ArrayType at = (ArrayType) type;
            writeArrayToGroup(group, fieldIndex, array.getArray(ordinal), at.elementType());
        } else if (type instanceof StructType) {
            StructType st = (StructType) type;
            writeStructToGroup(group, fieldIndex, array.getStruct(ordinal, st.fields().length), st);
        } else if (type instanceof MapType) {
            MapType mt = (MapType) type;
            writeMapToGroup(group, fieldIndex, array.getMap(ordinal), mt.keyType(), mt.valueType());
        } else {
            group.add(fieldIndex, array.get(ordinal, type).toString());
        }
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
                // Binary, complex, or unsupported type - track count only
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
