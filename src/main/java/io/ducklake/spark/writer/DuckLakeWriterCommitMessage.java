package io.ducklake.spark.writer;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Commit message from a single partition writer.
 * Contains the written file path, record count, and per-column statistics.
 */
public class DuckLakeWriterCommitMessage implements WriterCommitMessage {
    private static final long serialVersionUID = 1L;

    public final String absolutePath;
    public final String relativePath;
    public final long recordCount;
    public final long fileSize;
    public final List<ColumnStats> columnStats;
    public final Map<Integer, String> partitionValues;  // partition_key_index -> partition_value
    public String[] partitionColNames;   // set by partitioned writer
    public String[] partitionColValues;  // set by partitioned writer

    public DuckLakeWriterCommitMessage(String absolutePath, String relativePath,
                                        long recordCount, long fileSize,
                                        List<ColumnStats> columnStats) {
        this(absolutePath, relativePath, recordCount, fileSize, columnStats, null);
    }

    public DuckLakeWriterCommitMessage(String absolutePath, String relativePath,
                                        long recordCount, long fileSize,
                                        List<ColumnStats> columnStats,
                                        Map<Integer, String> partitionValues) {
        this.absolutePath = absolutePath;
        this.relativePath = relativePath;
        this.recordCount = recordCount;
        this.fileSize = fileSize;
        this.columnStats = columnStats;
        this.partitionValues = partitionValues;
    }

    public static class ColumnStats implements Serializable {
        private static final long serialVersionUID = 1L;

        public final long columnId;
        public final String minValue;
        public final String maxValue;
        public final long nullCount;
        public final long valueCount;

        public ColumnStats(long columnId, String minValue, String maxValue,
                           long nullCount, long valueCount) {
            this.columnId = columnId;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.nullCount = nullCount;
            this.valueCount = valueCount;
        }
    }
}
