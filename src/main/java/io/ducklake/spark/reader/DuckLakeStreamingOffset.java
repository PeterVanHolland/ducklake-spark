package io.ducklake.spark.reader;

import org.apache.spark.sql.connector.read.streaming.Offset;

/**
 * Streaming offset for DuckLake: wraps a snapshot ID.
 */
public class DuckLakeStreamingOffset extends Offset {
    private final long snapshotId;

    public DuckLakeStreamingOffset(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String json() {
        return "{\"snapshotId\":" + snapshotId + "}";
    }

    public static DuckLakeStreamingOffset fromJson(String json) {
        // Simple parse: {"snapshotId":123}
        String num = json.replaceAll("[^0-9]", "");
        return new DuckLakeStreamingOffset(Long.parseLong(num));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DuckLakeStreamingOffset)) return false;
        return snapshotId == ((DuckLakeStreamingOffset) o).snapshotId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(snapshotId);
    }
}
