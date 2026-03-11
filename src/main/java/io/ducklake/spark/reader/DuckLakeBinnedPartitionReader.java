package io.ducklake.spark.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;

/**
 * Reads multiple small DuckLake data files sequentially in a single Spark task.
 * This eliminates the per-task scheduling overhead for many small files.
 */
public class DuckLakeBinnedPartitionReader implements PartitionReader<InternalRow> {

    private final List<DuckLakeInputPartition> subPartitions;
    private final StructType requiredSchema;
    private final StructType fullSchema;
    private int currentIndex = 0;
    private DuckLakePartitionReader currentReader;

    public DuckLakeBinnedPartitionReader(DuckLakeBinnedInputPartition partition,
                                          StructType requiredSchema, StructType fullSchema) {
        this.subPartitions = partition.getSubPartitions();
        this.requiredSchema = requiredSchema;
        this.fullSchema = fullSchema;
        advanceToNextReader();
    }

    private void advanceToNextReader() {
        if (currentReader != null) {
            try { currentReader.close(); } catch (IOException e) { /* ignore */ }
        }
        currentReader = null;
        while (currentIndex < subPartitions.size()) {
            currentReader = new DuckLakePartitionReader(
                    subPartitions.get(currentIndex), requiredSchema, fullSchema);
            currentIndex++;
            return;
        }
    }

    @Override
    public boolean next() throws IOException {
        while (currentReader != null) {
            if (currentReader.next()) {
                return true;
            }
            // Current file exhausted, move to next
            advanceToNextReader();
        }
        return false;
    }

    @Override
    public InternalRow get() {
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
