package io.ducklake.spark.writer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

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

    public DuckLakeDataWriterFactory(StructType schema, long[] columnIds,
                                     String writeBasePath, String tablePath) {
        this.schema = schema;
        this.columnIds = columnIds;
        this.writeBasePath = writeBasePath;
        this.tablePath = tablePath;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskAttemptId) {
        return new DuckLakeDataWriter(schema, columnIds, writeBasePath, tablePath,
                partitionId, taskAttemptId);
    }
}
