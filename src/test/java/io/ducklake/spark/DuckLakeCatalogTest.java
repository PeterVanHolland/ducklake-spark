package io.ducklake.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests for DuckLakeCatalog (CatalogPlugin).
 * Tests SQL DDL and DML through Spark SQL using the catalog interface.
 */
public class DuckLakeCatalogTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-catalog-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);

        // Ensure Spark can find our catalog plugin class
        Thread.currentThread().setContextClassLoader(DuckLakeCatalogTest.class.getClassLoader());

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeCatalogTest")
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
    // Namespace tests
    // ---------------------------------------------------------------

    @Test
    public void testShowNamespaces() {
        Dataset<Row> result = spark.sql("SHOW NAMESPACES IN ducklake");
        List<Row> rows = result.collectAsList();
        assertTrue("Should contain at least 'main'", rows.size() >= 1);
        boolean foundMain = false;
        for (Row row : rows) {
            if ("main".equals(row.getString(0))) foundMain = true;
        }
        assertTrue("Should contain 'main'", foundMain);
    }

    @Test
    public void testCreateAndDropNamespace() {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ducklake.ns_test");

        Dataset<Row> result = spark.sql("SHOW NAMESPACES IN ducklake");
        List<String> names = new ArrayList<>();
        for (Row row : result.collectAsList()) {
            names.add(row.getString(0));
        }
        assertTrue("Should contain 'ns_test'", names.contains("ns_test"));

        // Drop it
        spark.sql("DROP NAMESPACE ducklake.ns_test");

        result = spark.sql("SHOW NAMESPACES IN ducklake");
        names.clear();
        for (Row row : result.collectAsList()) {
            names.add(row.getString(0));
        }
        assertFalse("Should not contain 'ns_test' after drop", names.contains("ns_test"));
    }

    // ---------------------------------------------------------------
    // Table DDL tests
    // ---------------------------------------------------------------

    @Test
    public void testCreateAndShowTable() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.tbl_show (id INT, name STRING, value DOUBLE)");

        Dataset<Row> tables = spark.sql("SHOW TABLES IN ducklake.main");
        List<String> tableNames = new ArrayList<>();
        for (Row row : tables.collectAsList()) {
            tableNames.add(row.getString(1)); // tableName is second column
        }
        assertTrue("Should contain 'tbl_show'", tableNames.contains("tbl_show"));
    }

    @Test
    public void testCreateAndDropTable() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.tbl_drop (id INT)");

        // Verify it exists
        assertTrue(spark.sql("SHOW TABLES IN ducklake.main").collectAsList()
                .stream().anyMatch(r -> r.getString(1).equals("tbl_drop")));

        // Drop it
        spark.sql("DROP TABLE ducklake.main.tbl_drop");

        // Verify it's gone
        assertFalse(spark.sql("SHOW TABLES IN ducklake.main").collectAsList()
                .stream().anyMatch(r -> r.getString(1).equals("tbl_drop")));
    }

    @Test
    public void testDropTableIfExists() {
        // DROP TABLE IF EXISTS should not throw for non-existent table
        spark.sql("DROP TABLE IF EXISTS ducklake.main.nonexistent_xyz");
    }

    // ---------------------------------------------------------------
    // INSERT + SELECT tests
    // ---------------------------------------------------------------

    @Test
    public void testInsertAndSelect() throws Exception {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.tbl_rw (id INT, name STRING)");
        new File(dataPath + "main/tbl_rw/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.tbl_rw VALUES (1, 'hello'), (2, 'world')");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.tbl_rw ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("hello", rows.get(0).getString(1));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("world", rows.get(1).getString(1));
    }

    @Test
    public void testInsertMultipleBatches() throws Exception {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.tbl_multi (id INT, val DOUBLE)");
        new File(dataPath + "main/tbl_multi/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.tbl_multi VALUES (1, 10.0), (2, 20.0)");
        spark.sql("INSERT INTO ducklake.main.tbl_multi VALUES (3, 30.0)");

        Dataset<Row> result = spark.sql("SELECT COUNT(*) FROM ducklake.main.tbl_multi");
        assertEquals(3L, result.collectAsList().get(0).getLong(0));
    }

    @Test
    public void testSelectWithFilter() throws Exception {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.tbl_filter (id INT, name STRING)");
        new File(dataPath + "main/tbl_filter/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.tbl_filter VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.tbl_filter WHERE id > 1 ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals(2, rows.get(0).getInt(0));
        assertEquals(3, rows.get(1).getInt(0));
    }

    // ---------------------------------------------------------------
    // Type roundtrip tests
    // ---------------------------------------------------------------

    @Test
    public void testVariousTypes() throws Exception {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.tbl_types (" +
                "bool_col BOOLEAN, int_col INT, long_col BIGINT, " +
                "float_col FLOAT, double_col DOUBLE, str_col STRING)");
        new File(dataPath + "main/tbl_types/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.tbl_types VALUES " +
                "(true, 42, 9999999999, cast(3.14 as float), 2.718, 'hello')");

        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.tbl_types");
        Row row = result.collectAsList().get(0);
        assertTrue(row.getBoolean(0));
        assertEquals(42, row.getInt(1));
        assertEquals(9999999999L, row.getLong(2));
        assertEquals(3.14f, row.getFloat(3), 0.01);
        assertEquals(2.718, row.getDouble(4), 0.001);
        assertEquals("hello", row.getString(5));
    }

    // ---------------------------------------------------------------
    // Catalog setup helper — minimal empty catalog with the "main" schema
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

                // Snapshot 0: create "main" schema
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
