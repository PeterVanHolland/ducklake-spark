package io.ducklake.spark.writer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.PartitionInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Factory for creating partition-level Parquet writers.
 * Serialized and sent to executors by Spark.
 */
public class DuckLakeDataWriterFactory implements DataWriterFactory, Serializable {
    private static final long serialVersionUID = 1L;

    private final StructType schema;
    private final long[] columnIds;
    private final String writeBasePath;
    private final String tablePath;
    private final List<PartitionInfo> partitionInfos;

    public DuckLakeDataWriterFactory(StructType schema, long[] columnIds,
                                     String writeBasePath, String tablePath) {
        this(schema, columnIds, writeBasePath, tablePath, null);
    }

    public DuckLakeDataWriterFactory(StructType schema, long[] columnIds,
                                     String writeBasePath, String tablePath,
                                     List<PartitionInfo> partitionInfos) {
        this.schema = schema;
        this.columnIds = columnIds;
        this.writeBasePath = writeBasePath;
        this.tablePath = tablePath;
        this.partitionInfos = partitionInfos;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskAttemptId) {
        if (partitionInfos != null && !partitionInfos.isEmpty()) {
            return new DuckLakePartitioningDataWriter(schema, columnIds, writeBasePath,
                    tablePath, partitionId, taskAttemptId, partitionInfos);
        }
        return new DuckLakeDataWriter(schema, columnIds, writeBasePath, tablePath,
                partitionId, taskAttemptId, partitionInfos);
    }
}
