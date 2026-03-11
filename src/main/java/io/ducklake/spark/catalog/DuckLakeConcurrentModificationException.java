package io.ducklake.spark.catalog;

/**
 * Thrown when a concurrent modification is detected (OCC conflict).
 */
public class DuckLakeConcurrentModificationException extends DuckLakeException {
    private final long expectedSnapshot;
    private final long actualSnapshot;

    public DuckLakeConcurrentModificationException(long expectedSnapshot, long actualSnapshot) {
        super("Concurrent modification detected: expected snapshot " + expectedSnapshot +
              " but found " + actualSnapshot);
        this.expectedSnapshot = expectedSnapshot;
        this.actualSnapshot = actualSnapshot;
    }

    public long getExpectedSnapshot() { return expectedSnapshot; }
    public long getActualSnapshot() { return actualSnapshot; }
}
