package io.ducklake.spark.writer;

import org.apache.spark.sql.connector.write.WriterCommitMessage;
import java.util.List;

/**
 * Composite commit message from a partitioned write: wraps
 * multiple per-partition DuckLakeWriterCommitMessages.
 */
public class DuckLakePartitionedWriterCommitMessage implements WriterCommitMessage {
    private static final long serialVersionUID = 1L;
    public final List<DuckLakeWriterCommitMessage> partitionMessages;

    public DuckLakePartitionedWriterCommitMessage(List<DuckLakeWriterCommitMessage> partitionMessages) {
        this.partitionMessages = partitionMessages;
    }
}
