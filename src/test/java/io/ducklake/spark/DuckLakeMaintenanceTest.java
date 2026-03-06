package io.ducklake.spark;

import io.ducklake.spark.maintenance.DuckLakeMaintenance;

import org.apache.spark.sql.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests for DuckLake maintenance operations:
 * rewriteDataFiles, expireSnapshots, and vacuum.
 */
public class DuckLakeMaintenanceTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-maintenance-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);

        Thread.currentThread().setContextClassLoader(DuckLakeMaintenanceTest.class.getClassLoader());

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeMaintenanceTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog", catalogPath)
                .config("spark.sql.catalog.ducklake.data_path", dataPath)
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
        if (tempDir != null) {
            deleteRecursive(new File(tempDir));
        }
    }

    // ---------------------------------------------------------------
    // Helper: insert test data via SQL
    // ---------------------------------------------------------------

    private void createAndPopulateTable(String tableName, int numRows) {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main." + tableName +
                " (id INT, name STRING, value DOUBLE)");

        StringBuilder values = new StringBuilder();
        for (int i = 1; i <= numRows; i++) {
            if (i > 1) values.append(", ");
            values.append("(").append(i).append(", 'name_").append(i).append("', ").append(i * 10.0).append(")");
        }
        spark.sql("INSERT INTO ducklake.main." + tableName + " VALUES " + values);
    }

    private void insertBatch(String tableName, int startId, int endId) {
        StringBuilder values = new StringBuilder();
        for (int i = startId; i <= endId; i++) {
            if (i > startId) values.append(", ");
            values.append("(").append(i).append(", 'name_").append(i).append("', ").append(i * 10.0).append(")");
        }
        spark.sql("INSERT INTO ducklake.main." + tableName + " VALUES " + values);
    }

    // ---------------------------------------------------------------
    // Helper: count catalog entries
    // ---------------------------------------------------------------

    private int countActiveDataFiles(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            long tableId = getTableId(conn, tableName);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, tableId);
                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private int countActiveDeleteFiles(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            long tableId = getTableId(conn, tableName);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM ducklake_delete_file WHERE table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, tableId);
                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private int countSnapshots() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ducklake_snapshot")) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private int countEndedDataFiles(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            long tableId = getTableId(conn, tableName);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NOT NULL")) {
                ps.setLong(1, tableId);
                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private long getTableRecordCount(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            long tableId = getTableId(conn, tableName);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT record_count FROM ducklake_table_stats WHERE table_id = ?")) {
                ps.setLong(1, tableId);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) return rs.getLong(1);
                return -1;
            }
        }
    }

    private long getTableId(Connection conn, String tableName) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL")) {
            ps.setString(1, tableName);
            ResultSet rs = ps.executeQuery();
            assertTrue("Table not found: " + tableName, rs.next());
            return rs.getLong(1);
        }
    }

    private int countParquetFiles(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            long tableId = getTableId(conn, tableName);
            // Get table path
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT path FROM ducklake_table WHERE table_id = ?")) {
                ps.setLong(1, tableId);
                ResultSet rs = ps.executeQuery();
                rs.next();
                String tablePath = rs.getString("path");
                File tableDir = new File(dataPath + tablePath);
                if (!tableDir.exists()) return 0;
                File[] files = tableDir.listFiles((dir, name) -> name.endsWith(".parquet"));
                return files == null ? 0 : files.length;
            }
        }
    }

    // ---------------------------------------------------------------
    // rewrite_data_files tests
    // ---------------------------------------------------------------

    @Test
    public void testRewriteDataFilesMergesSmallFiles() throws Exception {
        String tableName = "compact_merge";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");

        // Insert multiple batches to create multiple data files
        insertBatch(tableName, 1, 5);
        insertBatch(tableName, 6, 10);
        insertBatch(tableName, 11, 15);

        // Verify we have multiple data files
        int filesBefore = countActiveDataFiles(tableName);
        assertTrue("Should have multiple data files before compaction", filesBefore >= 3);

        // Compact
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");

        // Should have exactly 1 active data file after compaction
        assertEquals(1, countActiveDataFiles(tableName));

        // Verify data integrity
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main." + tableName + " ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(15, rows.size());
        for (int i = 0; i < 15; i++) {
            assertEquals(i + 1, rows.get(i).getInt(0));
            assertEquals("name_" + (i + 1), rows.get(i).getString(1));
            assertEquals((i + 1) * 10.0, rows.get(i).getDouble(2), 0.001);
        }
    }

    @Test
    public void testRewriteDataFilesAppliesDeleteVectors() throws Exception {
        String tableName = "compact_deletes";
        createAndPopulateTable(tableName, 10);

        // Delete some rows (creates delete files)
        spark.sql("DELETE FROM ducklake.main." + tableName + " WHERE id IN (2, 4, 6, 8)");

        // Verify delete files exist
        assertTrue("Should have delete files", countActiveDeleteFiles(tableName) > 0);

        // Compact
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");

        // After compaction, no delete files should be active
        assertEquals(0, countActiveDeleteFiles(tableName));

        // Verify data integrity -- only non-deleted rows remain
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main." + tableName + " ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(6, rows.size());
        int[] expectedIds = {1, 3, 5, 7, 9, 10};
        for (int i = 0; i < expectedIds.length; i++) {
            assertEquals(expectedIds[i], rows.get(i).getInt(0));
        }
    }

    @Test
    public void testRewriteDataFilesIdempotent() throws Exception {
        String tableName = "compact_idempotent";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");
        insertBatch(tableName, 1, 5);
        insertBatch(tableName, 6, 10);

        // First compaction
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");
        assertEquals(1, countActiveDataFiles(tableName));

        int snapshotsBefore = countSnapshots();

        // Second compaction -- should be a no-op (1 file, no deletes)
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");
        assertEquals(1, countActiveDataFiles(tableName));
        assertEquals(snapshotsBefore, countSnapshots());

        // Data still correct
        assertEquals(10, spark.sql("SELECT * FROM ducklake.main." + tableName).count());
    }

    @Test
    public void testDataIntegrityAfterCompaction() throws Exception {
        String tableName = "compact_integrity";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");

        // Insert in multiple batches
        insertBatch(tableName, 1, 3);
        insertBatch(tableName, 4, 6);

        // Delete from first batch
        spark.sql("DELETE FROM ducklake.main." + tableName + " WHERE id = 2");

        // Insert more
        insertBatch(tableName, 7, 9);

        // Get data before compaction
        Dataset<Row> before = spark.sql(
                "SELECT * FROM ducklake.main." + tableName + " ORDER BY id");
        List<Row> rowsBefore = before.collectAsList();

        // Compact
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");

        // Get data after compaction
        Dataset<Row> after = spark.sql(
                "SELECT * FROM ducklake.main." + tableName + " ORDER BY id");
        List<Row> rowsAfter = after.collectAsList();

        // Verify identical data
        assertEquals(rowsBefore.size(), rowsAfter.size());
        for (int i = 0; i < rowsBefore.size(); i++) {
            assertEquals(rowsBefore.get(i).getInt(0), rowsAfter.get(i).getInt(0));
            assertEquals(rowsBefore.get(i).getString(1), rowsAfter.get(i).getString(1));
            assertEquals(rowsBefore.get(i).getDouble(2), rowsAfter.get(i).getDouble(2), 0.001);
        }
    }

    @Test
    public void testStatsCorrectAfterCompaction() throws Exception {
        String tableName = "compact_stats";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");
        insertBatch(tableName, 1, 10);
        insertBatch(tableName, 11, 20);

        // Delete some rows
        spark.sql("DELETE FROM ducklake.main." + tableName + " WHERE id > 15");

        // Compact
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");

        // Check table stats
        long recordCount = getTableRecordCount(tableName);
        assertEquals(15, recordCount);

        // Verify actual data matches stats
        long actualCount = spark.sql("SELECT * FROM ducklake.main." + tableName).count();
        assertEquals(recordCount, actualCount);
    }

    @Test
    public void testFileCountReductionAfterCompaction() throws Exception {
        String tableName = "compact_file_count";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");

        // Create many small files
        insertBatch(tableName, 1, 3);
        insertBatch(tableName, 4, 6);
        insertBatch(tableName, 7, 9);
        insertBatch(tableName, 10, 12);

        int filesBefore = countActiveDataFiles(tableName);
        assertTrue("Should have >= 4 files before compaction", filesBefore >= 4);

        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");

        int filesAfter = countActiveDataFiles(tableName);
        assertEquals("Should have exactly 1 file after compaction", 1, filesAfter);
        assertTrue("File count should decrease", filesAfter < filesBefore);
    }

    // ---------------------------------------------------------------
    // expire_snapshots tests
    // ---------------------------------------------------------------

    @Test
    public void testExpireSnapshotsRemovesOldSnapshots() throws Exception {
        String tableName = "expire_test";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");
        insertBatch(tableName, 1, 5);  // snapshot N+1
        insertBatch(tableName, 6, 10); // snapshot N+2

        int snapshotsBefore = countSnapshots();
        assertTrue("Should have multiple snapshots", snapshotsBefore >= 3);

        // Expire all snapshots except the latest (olderThanDays=0 means older than now)
        DuckLakeMaintenance.expireSnapshots(spark, catalogPath, 0);

        int snapshotsAfter = countSnapshots();
        assertEquals("Should keep only the latest snapshot", 1, snapshotsAfter);

        // Data should still be readable
        assertEquals(10, spark.sql("SELECT * FROM ducklake.main." + tableName).count());
    }

    @Test
    public void testExpireSnapshotsKeepsLatest() throws Exception {
        String tableName = "expire_keeps_latest";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");
        insertBatch(tableName, 1, 5);

        // Even with aggressive expiration, the latest snapshot survives
        DuckLakeMaintenance.expireSnapshots(spark, catalogPath, 0);

        assertTrue("Should keep at least 1 snapshot", countSnapshots() >= 1);
        assertEquals(5, spark.sql("SELECT * FROM ducklake.main." + tableName).count());
    }

    // ---------------------------------------------------------------
    // vacuum tests
    // ---------------------------------------------------------------

    @Test
    public void testVacuumDeletesOrphanedFiles() throws Exception {
        String tableName = "vacuum_test";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");
        insertBatch(tableName, 1, 5);  // Creates data file(s)
        insertBatch(tableName, 6, 10); // Creates more data file(s)

        // Compact -- old files are now logically deleted
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");

        int endedFilesBefore = countEndedDataFiles(tableName);
        assertTrue("Should have ended data files after compaction", endedFilesBefore > 0);

        // Expire old snapshots so the old files become orphaned
        DuckLakeMaintenance.expireSnapshots(spark, catalogPath, 0);

        // Vacuum -- should physically delete orphaned files
        DuckLakeMaintenance.vacuum(spark, catalogPath);

        // No more ended data files in catalog
        int endedFilesAfter = countEndedDataFiles(tableName);
        assertEquals("Vacuum should remove orphaned file records", 0, endedFilesAfter);

        // Data still readable
        assertEquals(10, spark.sql("SELECT * FROM ducklake.main." + tableName).count());
    }

    @Test
    public void testVacuumCleansDeleteFiles() throws Exception {
        String tableName = "vacuum_del_files";
        createAndPopulateTable(tableName, 10);

        // Create delete files
        spark.sql("DELETE FROM ducklake.main." + tableName + " WHERE id <= 3");

        // Compact -- delete files become ended
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");

        // Expire + vacuum
        DuckLakeMaintenance.expireSnapshots(spark, catalogPath, 0);
        DuckLakeMaintenance.vacuum(spark, catalogPath);

        // Verify no ended delete file records remain
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT COUNT(*) FROM ducklake_delete_file WHERE end_snapshot IS NOT NULL")) {
                rs.next();
                assertEquals(0, rs.getInt(1));
            }
        }

        // Data still correct
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main." + tableName + " ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(7, rows.size());
        assertEquals(4, rows.get(0).getInt(0));
    }

    @Test
    public void testFullMaintenancePipeline() throws Exception {
        String tableName = "full_pipeline";
        spark.sql("CREATE TABLE ducklake.main." + tableName + " (id INT, name STRING, value DOUBLE)");

        // Insert multiple batches
        insertBatch(tableName, 1, 10);
        insertBatch(tableName, 11, 20);
        insertBatch(tableName, 21, 30);

        // Delete rows with id = 3, 6, 9, ..., 30 (every 3rd)
        spark.sql("DELETE FROM ducklake.main." + tableName +
                " WHERE id IN (3, 6, 9, 12, 15, 18, 21, 24, 27, 30)");

        // Many snapshots, many files, some deletes
        assertTrue("Multiple data files", countActiveDataFiles(tableName) >= 3);

        // Step 1: Compact
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");
        assertEquals(1, countActiveDataFiles(tableName));
        assertEquals(0, countActiveDeleteFiles(tableName));

        // Step 2: Expire old snapshots
        DuckLakeMaintenance.expireSnapshots(spark, catalogPath, 0);
        assertEquals(1, countSnapshots());

        // Step 3: Vacuum
        DuckLakeMaintenance.vacuum(spark, catalogPath);
        assertEquals(0, countEndedDataFiles(tableName));

        // Verify data integrity
        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main." + tableName + " ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(20, rows.size()); // 30 - 10 deleted rows

        // Verify no deleted rows remain
        Set<Integer> deletedIds = new HashSet<>(Arrays.asList(3, 6, 9, 12, 15, 18, 21, 24, 27, 30));
        for (Row row : rows) {
            assertFalse("Deleted id should not exist: " + row.getInt(0),
                    deletedIds.contains(row.getInt(0)));
        }

        // Verify only 1 parquet file remains on disk
        assertEquals(1, countParquetFiles(tableName));
    }

    // ---------------------------------------------------------------
    // Catalog setup helper (minimal)
    // ---------------------------------------------------------------

    private static void createMinimalCatalog(String catPath, String dp) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catPath)) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE ducklake_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT)");
                st.execute("CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TEXT, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)");
                st.execute("CREATE TABLE ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)");
                st.execute("CREATE TABLE ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid TEXT, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN)");
                st.execute("CREATE TABLE ducklake_table(table_id BIGINT, table_uuid TEXT, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN)");
                st.execute("CREATE TABLE ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)");
                st.execute("CREATE TABLE ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT)");
                st.execute("CREATE TABLE ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR)");
                st.execute("CREATE TABLE ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)");
                st.execute("CREATE TABLE ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT)");
                st.execute("CREATE TABLE ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN)");
                st.execute("CREATE TABLE ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)");
                st.execute("CREATE TABLE ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)");

                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dp + "')");

                st.execute("INSERT INTO ducklake_snapshot VALUES (0, datetime('now'), 0, 1, 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', 1)");
            }
            conn.commit();
        }
    }

    private static void deleteRecursive(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursive(child);
                }
            }
        }
        file.delete();
    }
}
