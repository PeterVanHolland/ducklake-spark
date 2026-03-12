package io.ducklake.spark.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;

/**
 * Creates partition readers for DuckLake data files.
 *
 * Uses Spark's built-in vectorized Parquet reader (ColumnarBatch) for
 * data file partitions — the same reader Spark's native Parquet source
 * and Iceberg use. Falls back to row-by-row for inlined data and for
 * partitions with delete files or name mappings (schema evolution).
 */
public class DuckLakePartitionReaderFactory implements PartitionReaderFactory, Serializable {
    private static final long serialVersionUID = 1L;

    private final StructType requiredSchema;
    private final StructType fullSchema;
    private final boolean allColumnar;

    public DuckLakePartitionReaderFactory(StructType requiredSchema, StructType fullSchema,
                                          boolean allColumnar) {
        this.requiredSchema = requiredSchema;
        this.fullSchema = fullSchema;
        this.allColumnar = allColumnar;
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        // Must return the same value for ALL partitions in a scan —
        // Spark does not allow mixing row-based and columnar partitions.
        return allColumnar;
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        if (partition instanceof DuckLakeBinnedInputPartition) {
            return new DuckLakeColumnarBinnedPartitionReader(
                    (DuckLakeBinnedInputPartition) partition, requiredSchema);
        }
        return new DuckLakeColumnarPartitionReader(
                (DuckLakeInputPartition) partition, requiredSchema);
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        if (partition instanceof DuckLakeInlinedInputPartition) {
            return new DuckLakeInlinedPartitionReader(
                    (DuckLakeInlinedInputPartition) partition, requiredSchema);
        }
        if (partition instanceof DuckLakeBinnedInputPartition) {
            return new DuckLakeBinnedPartitionReader(
                    (DuckLakeBinnedInputPartition) partition, requiredSchema, fullSchema);
        }
        DuckLakeInputPartition dlPartition = (DuckLakeInputPartition) partition;
        return new DuckLakePartitionReader(dlPartition, requiredSchema, fullSchema);
    }
}
