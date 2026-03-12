package io.ducklake.spark.reader;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

/**
 * Columnar reader for bin-packed multi-file partitions.
 * Reads each sub-file using Spark's vectorized Parquet reader.
 */
public class DuckLakeColumnarBinnedPartitionReader implements PartitionReader<ColumnarBatch> {

    private final List<DuckLakeInputPartition> subPartitions;
    private final StructType requiredSchema;
    private int currentIndex = 0;
    private DuckLakeColumnarPartitionReader currentReader;

    public DuckLakeColumnarBinnedPartitionReader(DuckLakeBinnedInputPartition partition,
                                                   StructType requiredSchema) {
        this.subPartitions = partition.getSubPartitions();
        this.requiredSchema = requiredSchema;
        advanceToNextReader();
    }

    private void advanceToNextReader() {
        if (currentReader != null) {
            try { currentReader.close(); } catch (IOException e) { /* ignore */ }
        }
        currentReader = null;
        if (currentIndex < subPartitions.size()) {
            currentReader = new DuckLakeColumnarPartitionReader(
                    subPartitions.get(currentIndex), requiredSchema);
            currentIndex++;
        }
    }

    @Override
    public boolean next() throws IOException {
        while (currentReader != null) {
            if (currentReader.next()) {
                return true;
            }
            advanceToNextReader();
        }
        return false;
    }

    @Override
    public ColumnarBatch get() {
        return currentReader.get();
    }

    @Override
    public void close() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
    }
}
