package io.ducklake.spark.reader;

import org.apache.spark.sql.connector.read.InputPartition;
import java.io.Serializable;
import java.util.List;

/**
 * An InputPartition that represents multiple small DuckLake data files
 * bin-packed into a single Spark task. This reduces Spark task scheduling
 * overhead when reading many small files.
 */
public class DuckLakeBinnedInputPartition implements InputPartition, Serializable {
    private static final long serialVersionUID = 1L;

    private final List<DuckLakeInputPartition> subPartitions;

    public DuckLakeBinnedInputPartition(List<DuckLakeInputPartition> subPartitions) {
        this.subPartitions = subPartitions;
    }

    public List<DuckLakeInputPartition> getSubPartitions() {
        return subPartitions;
    }
}
