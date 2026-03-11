package io.ducklake.spark.catalog;

/**
 * Thrown when a DuckLake snapshot version or timestamp is not found.
 */
public class DuckLakeSnapshotNotFoundException extends DuckLakeException {
    public DuckLakeSnapshotNotFoundException(String message) {
        super(message);
    }

    public static DuckLakeSnapshotNotFoundException forVersion(long version) {
        return new DuckLakeSnapshotNotFoundException("Snapshot version " + version + " not found");
    }

    public static DuckLakeSnapshotNotFoundException forTimestamp(String timestamp) {
        return new DuckLakeSnapshotNotFoundException(
                "No snapshot found at or before timestamp: " + timestamp);
    }
}
