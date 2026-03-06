package io.ducklake.spark;

import io.ducklake.spark.writer.DuckLakeUpdateExecutor;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests for DuckLake row-level delete and update operations.
 */
public class DuckLakeDeleteTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-delete-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);

        Thread.currentThread().setContextClassLoader(DuckLakeDeleteTest.class.getClassLoader());

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeDeleteTest")
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

    // ---------------------------------------------------------------
    // DELETE tests
    // ---------------------------------------------------------------

    @Test
    public void testDeleteByPredicate() {
        createAndPopulateTable("del_pred", 5);

        // Verify initial data
        assertEquals(5, spark.sql("SELECT * FROM ducklake.main.del_pred").count());

        // Delete rows where id > 3
        spark.sql("DELETE FROM ducklake.main.del_pred WHERE id > 3");

        // Verify remaining rows
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.del_pred ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals(3, rows.get(2).getInt(0));
    }

    @Test
    public void testDeleteAllRows() {
        createAndPopulateTable("del_all", 3);
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.del_all").count());

        // Delete all rows
        spark.sql("DELETE FROM ducklake.main.del_all WHERE id >= 1");

        assertEquals(0, spark.sql("SELECT * FROM ducklake.main.del_all").count());
    }

    @Test
    public void testDeleteNoMatch() {
        createAndPopulateTable("del_nomatch", 3);

        // Delete with non-matching predicate — should be a no-op
        spark.sql("DELETE FROM ducklake.main.del_nomatch WHERE id > 100");

        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.del_nomatch").count());
    }

    @Test
    public void testDeleteByStringPredicate() {
        createAndPopulateTable("del_str", 5);

        // Delete by string equality
        spark.sql("DELETE FROM ducklake.main.del_str WHERE name = 'name_3'");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.del_str ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(4, rows.size());

        // Verify name_3 is gone
        for (Row row : rows) {
            assertNotEquals("name_3", row.getString(1));
        }
    }

    @Test
    public void testDeleteWithCompoundPredicate() {
        createAndPopulateTable("del_compound", 5);

        // Delete rows where id <= 2 OR id = 5
        spark.sql("DELETE FROM ducklake.main.del_compound WHERE id <= 2 OR id = 5");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.del_compound ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals(3, rows.get(0).getInt(0));
        assertEquals(4, rows.get(1).getInt(0));
    }

    @Test
    public void testMultipleDeletesOnSameTable() {
        createAndPopulateTable("del_multi", 5);

        // First delete
        spark.sql("DELETE FROM ducklake.main.del_multi WHERE id = 1");
        assertEquals(4, spark.sql("SELECT * FROM ducklake.main.del_multi").count());

        // Second delete
        spark.sql("DELETE FROM ducklake.main.del_multi WHERE id = 3");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.del_multi").count());

        // Third delete
        spark.sql("DELETE FROM ducklake.main.del_multi WHERE id = 5");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.del_multi ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals(2, rows.get(0).getInt(0));
        assertEquals(4, rows.get(1).getInt(0));
    }

    @Test
    public void testDeleteThenInsert() {
        createAndPopulateTable("del_ins", 3);

        // Delete some rows
        spark.sql("DELETE FROM ducklake.main.del_ins WHERE id = 2");

        // Insert new data
        spark.sql("INSERT INTO ducklake.main.del_ins VALUES (10, 'new_row', 100.0)");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.del_ins ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(3, rows.get(1).getInt(0));
        assertEquals(10, rows.get(2).getInt(0));
    }

    @Test
    public void testDeleteUpdatesTableStats() throws Exception {
        createAndPopulateTable("del_stats", 5);

        spark.sql("DELETE FROM ducklake.main.del_stats WHERE id > 3");

        // Check table stats in catalog
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            // Find the table_id
            long tableId;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT table_id FROM ducklake_table WHERE table_name = 'del_stats' AND end_snapshot IS NULL")) {
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                tableId = rs.getLong(1);
            }

            // Check record count was updated
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT record_count FROM ducklake_table_stats WHERE table_id = ?")) {
                ps.setLong(1, tableId);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                assertEquals(3, rs.getLong("record_count"));
            }

            // Check delete file was registered
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM ducklake_delete_file WHERE table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, tableId);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                assertTrue("Should have at least one delete file", rs.getInt(1) >= 1);
            }
        }
    }

    @Test
    public void testDeleteCreatesNewSnapshot() throws Exception {
        createAndPopulateTable("del_snap", 3);

        // Get current snapshot before delete
        long snapBefore;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
                rs.next();
                snapBefore = rs.getLong(1);
            }
        }

        spark.sql("DELETE FROM ducklake.main.del_snap WHERE id = 1");

        // Verify new snapshot was created
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
                rs.next();
                assertTrue("Delete should create new snapshot", rs.getLong(1) > snapBefore);
            }

            // Verify snapshot changes reference the delete
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT changes_made FROM ducklake_snapshot_changes WHERE snapshot_id = " +
                     "(SELECT MAX(snapshot_id) FROM ducklake_snapshot)")) {
                assertTrue(rs.next());
                assertTrue("Should record delete changes",
                        rs.getString("changes_made").contains("deleted_from_table"));
            }
        }
    }

    @Test
    public void testDeleteByDoublePredicate() {
        createAndPopulateTable("del_dbl", 5);

        // Delete where value > 30.0
        spark.sql("DELETE FROM ducklake.main.del_dbl WHERE value > 30.0");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.del_dbl ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals(3, rows.get(2).getInt(0));
    }

    // ---------------------------------------------------------------
    // UPDATE tests (via DuckLakeUpdateExecutor)
    // ---------------------------------------------------------------

    @Test
    public void testUpdateRows() {
        createAndPopulateTable("upd_basic", 5);

        // Update name where id > 3
        DuckLakeUpdateExecutor updater = new DuckLakeUpdateExecutor(
                catalogPath, dataPath, "upd_basic", "main");

        Map<String, Object> updates = new HashMap<>();
        updates.put("name", "updated");

        org.apache.spark.sql.sources.Filter filter =
                new org.apache.spark.sql.sources.GreaterThan("id", 3);
        updater.updateWhere(new org.apache.spark.sql.sources.Filter[]{filter}, updates);

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.upd_basic ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(5, rows.size());

        // First 3 rows unchanged
        for (int i = 0; i < 3; i++) {
            assertEquals("name_" + (i + 1), rows.get(i).getString(1));
        }
        // Last 2 rows updated
        assertEquals("updated", rows.get(3).getString(1));
        assertEquals("updated", rows.get(4).getString(1));

        // id and value should be preserved
        assertEquals(4, rows.get(3).getInt(0));
        assertEquals(5, rows.get(4).getInt(0));
        assertEquals(40.0, rows.get(3).getDouble(2), 0.001);
        assertEquals(50.0, rows.get(4).getDouble(2), 0.001);
    }

    @Test
    public void testUpdateMultipleColumns() {
        createAndPopulateTable("upd_multi", 3);

        DuckLakeUpdateExecutor updater = new DuckLakeUpdateExecutor(
                catalogPath, dataPath, "upd_multi", "main");

        Map<String, Object> updates = new HashMap<>();
        updates.put("name", "changed");
        updates.put("value", 999.0);

        org.apache.spark.sql.sources.Filter filter =
                new org.apache.spark.sql.sources.EqualTo("id", 2);
        updater.updateWhere(new org.apache.spark.sql.sources.Filter[]{filter}, updates);

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.upd_multi ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        // Row 2 should be updated
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("changed", rows.get(1).getString(1));
        assertEquals(999.0, rows.get(1).getDouble(2), 0.001);

        // Other rows unchanged
        assertEquals("name_1", rows.get(0).getString(1));
        assertEquals("name_3", rows.get(2).getString(1));
    }

    @Test
    public void testUpdateNoMatch() {
        createAndPopulateTable("upd_nomatch", 3);

        DuckLakeUpdateExecutor updater = new DuckLakeUpdateExecutor(
                catalogPath, dataPath, "upd_nomatch", "main");

        Map<String, Object> updates = new HashMap<>();
        updates.put("name", "should_not_exist");

        org.apache.spark.sql.sources.Filter filter =
                new org.apache.spark.sql.sources.EqualTo("id", 999);
        updater.updateWhere(new org.apache.spark.sql.sources.Filter[]{filter}, updates);

        // Should be unchanged
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.upd_nomatch ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());
        for (Row row : rows) {
            assertNotEquals("should_not_exist", row.getString(1));
        }
    }

    @Test
    public void testUpdateThenDelete() {
        createAndPopulateTable("upd_del", 5);

        // First: update some rows
        DuckLakeUpdateExecutor updater = new DuckLakeUpdateExecutor(
                catalogPath, dataPath, "upd_del", "main");
        Map<String, Object> updates = new HashMap<>();
        updates.put("name", "updated");
        org.apache.spark.sql.sources.Filter filter =
                new org.apache.spark.sql.sources.LessThanOrEqual("id", 2);
        updater.updateWhere(new org.apache.spark.sql.sources.Filter[]{filter}, updates);

        // Then: delete some rows (including an updated one)
        spark.sql("DELETE FROM ducklake.main.upd_del WHERE id = 1");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.upd_del ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(4, rows.size());

        // id=2 should have updated name
        assertEquals(2, rows.get(0).getInt(0));
        assertEquals("updated", rows.get(0).getString(1));

        // id=3,4,5 should be original
        assertEquals(3, rows.get(1).getInt(0));
        assertEquals("name_3", rows.get(1).getString(1));
    }

    @Test
    public void testDeleteAfterUpdate() {
        createAndPopulateTable("del_after_upd", 3);

        // Update row 2
        DuckLakeUpdateExecutor updater = new DuckLakeUpdateExecutor(
                catalogPath, dataPath, "del_after_upd", "main");
        Map<String, Object> updates = new HashMap<>();
        updates.put("value", 999.0);
        org.apache.spark.sql.sources.Filter filter =
                new org.apache.spark.sql.sources.EqualTo("id", 2);
        updater.updateWhere(new org.apache.spark.sql.sources.Filter[]{filter}, updates);

        // Now delete the updated row
        spark.sql("DELETE FROM ducklake.main.del_after_upd WHERE value > 500.0");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.del_after_upd ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(3, rows.get(1).getInt(0));
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
