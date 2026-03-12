package io.ducklake.spark.reader;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Columnar (vectorized) Parquet reader for DuckLake data files.
 * Uses Spark's built-in VectorizedParquetRecordReader.
 */
public class DuckLakeColumnarPartitionReader implements PartitionReader<ColumnarBatch> {

    private final VectorizedParquetRecordReader reader;

    /** Types that VectorizedParquetRecordReader handles reliably. */
    private static final Set<Class<? extends DataType>> VECTORIZABLE_TYPES = new HashSet<>();
    static {
        VECTORIZABLE_TYPES.add(IntegerType.class);
        VECTORIZABLE_TYPES.add(LongType.class);
        VECTORIZABLE_TYPES.add(FloatType.class);
        VECTORIZABLE_TYPES.add(DoubleType.class);
        VECTORIZABLE_TYPES.add(StringType.class);
        VECTORIZABLE_TYPES.add(BooleanType.class);
        VECTORIZABLE_TYPES.add(DateType.class);
        VECTORIZABLE_TYPES.add(TimestampType.class);
        VECTORIZABLE_TYPES.add(BinaryType.class);
        VECTORIZABLE_TYPES.add(ShortType.class);
        VECTORIZABLE_TYPES.add(ByteType.class);
    }

    /** Check if a schema can be read with the vectorized reader. */
    public static boolean canVectorize(StructType schema) {
        for (StructField field : schema.fields()) {
            DataType dt = field.dataType();
            if (dt instanceof DecimalType) continue; // DecimalType is supported
            if (!VECTORIZABLE_TYPES.contains(dt.getClass())) return false;
        }
        return true;
    }

    public DuckLakeColumnarPartitionReader(DuckLakeInputPartition partition,
                                            StructType requiredSchema) {
        this.reader = new VectorizedParquetRecordReader(false, 4096);
        try {
            List<String> columnNames = new ArrayList<>();
            for (StructField field : requiredSchema.fields()) {
                columnNames.add(field.name());
            }
            reader.initialize(partition.getFilePath(), columnNames);
            reader.initBatch(new StructType(), null);
            reader.enableReturningBatches();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize vectorized Parquet reader for: "
                    + partition.getFilePath(), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        return reader.nextBatch();
    }

    @Override
    public ColumnarBatch get() {
        return reader.resultBatch();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
