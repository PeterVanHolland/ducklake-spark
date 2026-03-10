package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.util.DuckLakeTypeMapping;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;

import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

/**
 * CDC (Change Data Capture) utility for DuckLake tables.
 *
 * <p>Provides static methods to read changes between DuckLake snapshots.
 * Returns DataFrames with all original columns plus CDC metadata columns:</p>
 * <ul>
 *   <li>{@code _change_type} (STRING): "insert" or "delete"</li>
 *   <li>{@code _snapshot_id} (LONG): which snapshot made the change</li>
 * </ul>
 *
 * <p>Usage:</p>
 * <pre>
 *   // Get changes between two snapshots (exclusive start, inclusive end)
 *   Dataset&lt;Row&gt; changes = DuckLakeChanges.between(spark, catalogPath, tableName, 5, 10);
 *
 *   // Get changes since a snapshot (from snapshot to latest)
 *   Dataset&lt;Row&gt; changes = DuckLakeChanges.since(spark, catalogPath, tableName, 5);
 * </pre>
 */
public class DuckLakeChanges {

    /**
     * Get changes made to a table between two snapshots.
     *
     * @param spark      SparkSession
     * @param catalogPath Path to the DuckLake catalog database
     * @param tableName   Name of the table
     * @param fromSnapshot Snapshot ID to start from (exclusive)
     * @param toSnapshot   Snapshot ID to end at (inclusive)
     * @return DataFrame with original columns plus _change_type and _snapshot_id
     */
    public static Dataset<Row> between(SparkSession spark, String catalogPath, String tableName,
                                       long fromSnapshot, long toSnapshot) {
        return between(spark, catalogPath, tableName, "main", fromSnapshot, toSnapshot);
    }

    /**
     * Get changes made to a table between two snapshots.
     *
     * @param spark      SparkSession
     * @param catalogPath Path to the DuckLake catalog database
     * @param tableName   Name of the table
     * @param schemaName  Name of the schema (usually "main")
     * @param fromSnapshot Snapshot ID to start from (exclusive)
     * @param toSnapshot   Snapshot ID to end at (inclusive)
     * @return DataFrame with original columns plus _change_type and _snapshot_id
     */
    public static Dataset<Row> between(SparkSession spark, String catalogPath, String tableName,
                                       String schemaName, long fromSnapshot, long toSnapshot) {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, null)) {
            return getChangesInRange(spark, backend, tableName, schemaName, fromSnapshot, toSnapshot);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read changes from DuckLake catalog", e);
        }
    }

    /**
     * Get changes made to a table since a snapshot (up to the latest snapshot).
     *
     * @param spark      SparkSession
     * @param catalogPath Path to the DuckLake catalog database
     * @param tableName   Name of the table
     * @param fromSnapshot Snapshot ID to start from (exclusive)
     * @return DataFrame with original columns plus _change_type and _snapshot_id
     */
    public static Dataset<Row> since(SparkSession spark, String catalogPath, String tableName,
                                     long fromSnapshot) {
        return since(spark, catalogPath, tableName, "main", fromSnapshot);
    }

    /**
     * Get changes made to a table since a snapshot (up to the latest snapshot).
     *
     * @param spark      SparkSession
     * @param catalogPath Path to the DuckLake catalog database
     * @param tableName   Name of the table
     * @param schemaName  Name of the schema (usually "main")
     * @param fromSnapshot Snapshot ID to start from (exclusive)
     * @return DataFrame with original columns plus _change_type and _snapshot_id
     */
    public static Dataset<Row> since(SparkSession spark, String catalogPath, String tableName,
                                     String schemaName, long fromSnapshot) {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, null)) {
            long toSnapshot = backend.getCurrentSnapshotId();
            return getChangesInRange(spark, backend, tableName, schemaName, fromSnapshot, toSnapshot);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read changes from DuckLake catalog", e);
        }
    }

    /**
     * Internal method to read changes between snapshots.
     */
    private static Dataset<Row> getChangesInRange(SparkSession spark, DuckLakeMetadataBackend backend,
                                                  String tableName, String schemaName,
                                                  long fromSnapshot, long toSnapshot) throws SQLException {
        // Get table metadata
        SchemaInfo schema = backend.getSchemaByName(schemaName);
        if (schema == null) {
            throw new IllegalArgumentException("Schema not found: " + schemaName);
        }

        TableInfo table = backend.getTable(schema.schemaId, tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + schemaName + "." + tableName);
        }

        // Get table schema at the latest snapshot to build the result schema
        long latestSnapshot = backend.getCurrentSnapshotId();
        List<ColumnInfo> columns = backend.getColumns(table.tableId, latestSnapshot);
        StructType tableSchema = DuckLakeTypeMapping.buildSchema(columns);

        // Create result schema with CDC columns
        StructType resultSchema = tableSchema
                .add("_change_type", DataTypes.StringType, false)
                .add("_snapshot_id", DataTypes.LongType, false);

        // Get data path
        String dataPath = backend.getDataPath();
        if (dataPath == null) {
            throw new IllegalArgumentException("Data path not configured in catalog");
        }

        List<Dataset<Row>> changeSets = new ArrayList<>();

        // Get inserted data (from data files added in the range)
        List<DataFileInfo> insertedFiles = backend.getDataFilesInRange(table.tableId, fromSnapshot, toSnapshot);
        for (DataFileInfo fileInfo : insertedFiles) {
            String filePath = resolveFilePath(dataPath, fileInfo.path, fileInfo.pathIsRelative);

            try {
                Dataset<Row> fileData = spark.read().parquet(filePath);

                // Add CDC metadata columns
                Dataset<Row> insertChanges = fileData
                        .withColumn("_change_type", functions.lit("insert"))
                        .withColumn("_snapshot_id", functions.lit(fileInfo.beginSnapshot));

                changeSets.add(insertChanges);
            } catch (Exception e) {
                // Log warning but continue - file might be compacted or deleted
                System.err.println("Warning: Could not read data file " + filePath + ": " + e.getMessage());
            }
        }

        // Get deleted data (from delete files added in the range)
        List<DeleteFileInfo> deletedFiles = backend.getDeleteFilesInRange(table.tableId, fromSnapshot, toSnapshot);
        for (DeleteFileInfo fileInfo : deletedFiles) {
            String filePath = resolveFilePath(dataPath, fileInfo.path, fileInfo.pathIsRelative);

            try {
                Dataset<Row> fileData = spark.read().parquet(filePath);

                // Add CDC metadata columns
                Dataset<Row> deleteChanges = fileData
                        .withColumn("_change_type", functions.lit("delete"))
                        .withColumn("_snapshot_id", functions.lit(fileInfo.beginSnapshot));

                changeSets.add(deleteChanges);
            } catch (Exception e) {
                // Log warning but continue - file might be compacted or deleted
                System.err.println("Warning: Could not read delete file " + filePath + ": " + e.getMessage());
            }
        }

        // Union all change sets
        if (changeSets.isEmpty()) {
            // Return empty DataFrame with correct schema
            return spark.createDataFrame(Collections.emptyList(), resultSchema);
        }

        Dataset<Row> result = changeSets.get(0);
        for (int i = 1; i < changeSets.size(); i++) {
            result = result.union(changeSets.get(i));
        }

        // Order by snapshot_id, then change_type (deletes before inserts in same snapshot)
        return result.orderBy(
                functions.col("_snapshot_id"),
                functions.col("_change_type").desc() // "delete" comes before "insert" alphabetically
        );
    }

    /**
     * Resolve file path (handle relative vs absolute paths).
     */
    private static String resolveFilePath(String basePath, String filePath, boolean isRelative) {
        if (isRelative) {
            return Paths.get(basePath, filePath).toString();
        } else {
            return filePath;
        }
    }
}