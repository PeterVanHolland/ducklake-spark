package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeCatalog;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.*;

import java.util.*;

/**
 * High-performance DuckLake operations that bypass Spark SQL parsing.
 * Uses the DuckLake SQLite catalog directly, only delegating to Spark
 * for actual data I/O (Parquet reads).
 *
 * This mirrors ducklake_pyspark: catalog ops via ducklake_core (SQLite),
 * data I/O via Spark's native Parquet reader.
 */
public class DuckLakeFastOps {

    // ---------------------------------------------------------------
    // DDL: direct catalog mutations (no Spark SQL parse overhead)
    // ---------------------------------------------------------------

    /** Add a column directly via SQLite catalog — bypasses spark.sql() parsing. */
    public static void addColumn(String catalogPath, String tableName,
                                  String colName, String colType) {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, null)) {
            TableInfo tableInfo = backend.getTable(tableName);
            if (tableInfo == null) throw new RuntimeException("Table not found: " + tableName);
            backend.addColumnWithDefault(tableInfo.tableId, colName, colType, true, null, null);
        } catch (Exception e) {
            throw new RuntimeException("addColumn failed: " + colName, e);
        }
    }

    /** Rename a column directly via SQLite catalog — bypasses spark.sql() parsing. */
    public static void renameColumn(String catalogPath, String tableName,
                                     String oldName, String newName) {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, null)) {
            TableInfo tableInfo = backend.getTable(tableName);
            if (tableInfo == null) throw new RuntimeException("Table not found: " + tableName);
            backend.renameColumn(tableInfo.tableId, oldName, newName);
        } catch (Exception e) {
            throw new RuntimeException("renameColumn failed: " + oldName + " -> " + newName, e);
        }
    }

    // ---------------------------------------------------------------
    // Fast read: catalog resolution + spark.read.parquet()
    // ---------------------------------------------------------------

    /** Read current snapshot via catalog + spark.read.parquet(). */
    public static Dataset<Row> read(SparkSession spark, String catalogPath, String tableName) {
        return readAtSnapshot(spark, catalogPath, tableName, -1);
    }

    /** Read at a specific snapshot version via catalog + spark.read.parquet(). */
    public static Dataset<Row> readAtVersion(SparkSession spark, String catalogPath,
                                              String tableName, long snapshotVersion) {
        return readAtSnapshot(spark, catalogPath, tableName, snapshotVersion);
    }

    private static Dataset<Row> readAtSnapshot(SparkSession spark, String catalogPath,
                                                String tableName, long version) {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, null)) {
            long snapId;
            if (version >= 0) {
                // Find snapshot ID for the given version
                snapId = version; // In DuckLake, snapshot_id = version
            } else {
                snapId = backend.getCurrentSnapshotId();
            }

            String dataPath = backend.getDataPath();
            TableInfo tableInfo = backend.getTable(tableName, "main", snapId);
            if (tableInfo == null) throw new RuntimeException("Table not found: " + tableName);

            List<DataFileInfo> files = backend.getDataFiles(tableInfo.tableId, snapId);
            if (files.isEmpty()) {
                return spark.emptyDataFrame();
            }

            String[] paths = new String[files.size()];
            for (int i = 0; i < files.size(); i++) {
                DataFileInfo f = files.get(i);
                paths[i] = f.pathIsRelative ? dataPath + f.path : f.path;
            }

            return spark.read().parquet(paths);
        } catch (Exception e) {
            throw new RuntimeException("Fast read failed", e);
        }
    }
}
