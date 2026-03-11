package io.ducklake.spark.catalog;

/**
 * Base exception for DuckLake catalog operations.
 */
public class DuckLakeException extends RuntimeException {
    public DuckLakeException(String message) {
        super(message);
    }
    public DuckLakeException(String message, Throwable cause) {
        super(message, cause);
    }
}
