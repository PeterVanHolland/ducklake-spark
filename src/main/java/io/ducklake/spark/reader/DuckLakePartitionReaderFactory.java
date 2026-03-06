package io.ducklake.spark.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Creates partition readers for DuckLake data files (Parquet).
 */
public class DuckLakePartitionReaderFactory implements PartitionReaderFactory, Serializable {
    private static final long serialVersionUID = 1L;

    private final StructType requiredSchema;
    private final StructType fullSchema;

    public DuckLakePartitionReaderFactory(StructType requiredSchema, StructType fullSchema) {
        this.requiredSchema = requiredSchema;
        this.fullSchema = fullSchema;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        DuckLakeInputPartition dlPartition = (DuckLakeInputPartition) partition;
        return new DuckLakePartitionReader(dlPartition, requiredSchema, fullSchema);
    }
}
