package io.ducklake.spark.writer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Factory for creating partition-level Parquet writers in streaming mode.
 * Serialized and sent to executors by Spark for each micro-batch.
 */
public class DuckLakeStreamWriterFactory implements StreamingDataWriterFactory, Serializable {
    private static final long serialVersionUID = 1L;

    private final StructType schema;
    private final long[] columnIds;
    private final String writeBasePath;
    private final String tablePath;

    public DuckLakeStreamWriterFactory(StructType schema, long[] columnIds,
                                       String writeBasePath, String tablePath) {
        this.schema = schema;
        this.columnIds = columnIds;
        this.writeBasePath = writeBasePath;
        this.tablePath = tablePath;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskAttemptId, long epochId) {
        return new DuckLakeDataWriter(schema, columnIds, writeBasePath, tablePath,
                partitionId, taskAttemptId);
    }
}
