package io.ducklake.spark;

import io.ducklake.spark.writer.DuckLakeUpdateExecutor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;
import org.junit.*;
import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import static org.junit.Assert.*;

/** Standalone UPDATE tests via DuckLakeUpdateExecutor. */
public class DuckLakeUpdateExtendedTest {
    private static SparkSession spark;
    private static String tempDir, catalogPath, dataPath;

    @BeforeClass public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-upd-ext-").toString();
        dataPath = tempDir + "/data/"; new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);
        Thread.currentThread().setContextClassLoader(DuckLakeUpdateExtendedTest.class.getClassLoader());
        spark = SparkSession.builder().master("local[2]").appName("DuckLakeUpdateExtendedTest")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "localhost")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog", catalogPath)
                .config("spark.sql.catalog.ducklake.data_path", dataPath).getOrCreate();
    }
    @AfterClass public static void tearDown() {
        if (spark != null) { spark.stop(); spark = null; }
        if (tempDir != null) deleteRecursive(new File(tempDir));
    }

    private void doUpdate(String table, Map<String, Object> updates, Filter... filters) {
        new DuckLakeUpdateExecutor(catalogPath, dataPath, table, "main")
                .updateWhere(filters, updates);
    }

    @Test public void testUpdateSingleColumnLiteral() {
        spark.sql("CREATE TABLE ducklake.main.upd_scl (id INT, name STRING)");
        spark.sql("INSERT INTO ducklake.main.upd_scl VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        Map<String, Object> u = new HashMap<>(); u.put("name", "updated");
        doUpdate("upd_scl", u, new GreaterThan("id", 2));
        List<Row> rows = spark.sql("SELECT * FROM ducklake.main.upd_scl ORDER BY id").collectAsList();
        assertEquals("a", rows.get(0).getString(1));
        assertEquals("updated", rows.get(2).getString(1));
    }

    @Test public void testUpdateMultipleColumns() {
        spark.sql("CREATE TABLE ducklake.main.upd_mc (id INT, name STRING, val DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.upd_mc VALUES (1, 'a', 1.0), (2, 'b', 2.0)");
        Map<String, Object> u = new HashMap<>(); u.put("name", "x"); u.put("val", 99.0);
        doUpdate("upd_mc", u, new EqualTo("id", 1));
        Row r = spark.sql("SELECT * FROM ducklake.main.upd_mc WHERE id = 1").collectAsList().get(0);
        assertEquals("x", r.getString(1));
        assertEquals(99.0, r.getDouble(2), 0.001);
    }

    @Test public void testUpdateNoMatchingRows() {
        spark.sql("CREATE TABLE ducklake.main.upd_nomatch (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.upd_nomatch VALUES (1, 'a'), (2, 'b')");
        Map<String, Object> u = new HashMap<>(); u.put("val", "x");
        doUpdate("upd_nomatch", u, new GreaterThan("id", 100));
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.upd_nomatch").count());
    }

    @Test public void testUpdateAllRows() {
        spark.sql("CREATE TABLE ducklake.main.upd_all (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.upd_all VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        Map<String, Object> u = new HashMap<>(); u.put("val", "z");
        doUpdate("upd_all", u, new GreaterThanOrEqual("id", 1));
        for (Row r : spark.sql("SELECT val FROM ducklake.main.upd_all").collectAsList()) {
            assertEquals("z", r.getString(0));
        }
    }

    @Test public void testUpdateAcrossMultipleFiles() {
        spark.sql("CREATE TABLE ducklake.main.upd_mf (id INT, val INT)");
        spark.sql("INSERT INTO ducklake.main.upd_mf VALUES (1, 10)");
        spark.sql("INSERT INTO ducklake.main.upd_mf VALUES (2, 20)");
        spark.sql("INSERT INTO ducklake.main.upd_mf VALUES (3, 30)");
        Map<String, Object> u = new HashMap<>(); u.put("val", 99);
        doUpdate("upd_mf", u, new GreaterThan("id", 1));
        List<Row> rows = spark.sql("SELECT * FROM ducklake.main.upd_mf ORDER BY id").collectAsList();
        assertEquals(10, rows.get(0).getInt(1));
        assertEquals(99, rows.get(1).getInt(1));
        assertEquals(99, rows.get(2).getInt(1));
    }

    @Test public void testUpdatePreservesUnmatchedRows() {
        spark.sql("CREATE TABLE ducklake.main.upd_pres (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.upd_pres VALUES (1, 'keep'), (2, 'change'), (3, 'keep')");
        Map<String, Object> u = new HashMap<>(); u.put("val", "changed");
        doUpdate("upd_pres", u, new EqualTo("id", 2));
        List<Row> rows = spark.sql("SELECT * FROM ducklake.main.upd_pres ORDER BY id").collectAsList();
        assertEquals("keep", rows.get(0).getString(1));
        assertEquals("changed", rows.get(1).getString(1));
        assertEquals("keep", rows.get(2).getString(1));
    }

    @Test public void testUpdateWithNullValue() {
        spark.sql("CREATE TABLE ducklake.main.upd_null (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.upd_null VALUES (1, 'a'), (2, 'b')");
        Map<String, Object> u = new HashMap<>(); u.put("val", null);
        doUpdate("upd_null", u, new EqualTo("id", 1));
        assertTrue(spark.sql("SELECT val FROM ducklake.main.upd_null WHERE id = 1").collectAsList().get(0).isNullAt(0));
    }

    @Test public void testUpdateThenDelete() {
        spark.sql("CREATE TABLE ducklake.main.upd_del (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.upd_del VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        Map<String, Object> u = new HashMap<>(); u.put("val", "updated");
        doUpdate("upd_del", u, new EqualTo("id", 2));
        spark.sql("DELETE FROM ducklake.main.upd_del WHERE id = 2");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.upd_del").count());
    }

    @Test public void testUpdateThenReadColumns() {
        spark.sql("CREATE TABLE ducklake.main.upd_rc (id INT, a STRING, b INT)");
        spark.sql("INSERT INTO ducklake.main.upd_rc VALUES (1, 'x', 10), (2, 'y', 20)");
        Map<String, Object> u = new HashMap<>(); u.put("b", 99);
        doUpdate("upd_rc", u, new EqualTo("id", 1));
        Row r = spark.sql("SELECT b FROM ducklake.main.upd_rc WHERE id = 1").collectAsList().get(0);
        assertEquals(99, r.getInt(0));
    }

    @Test public void testUpdatePartitionedTable() {
        spark.sql("CREATE TABLE ducklake.main.upd_part (id INT, part STRING, val INT) PARTITIONED BY (part)");
        spark.sql("INSERT INTO ducklake.main.upd_part VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)");
        Map<String, Object> u = new HashMap<>(); u.put("val", 99);
        doUpdate("upd_part", u, new EqualTo("part", "a"));
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.upd_part WHERE val = 99").count());
    }

    // Helpers
    private static void createMinimalCatalog(String p, String dp) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + p)) {
            c.setAutoCommit(false); try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE ducklake_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT)");
                s.execute("CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TEXT, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)");
                s.execute("CREATE TABLE ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)");
                s.execute("CREATE TABLE ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid TEXT, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN)");
                s.execute("CREATE TABLE ducklake_table(table_id BIGINT, table_uuid TEXT, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN)");
                s.execute("CREATE TABLE ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)");
                s.execute("CREATE TABLE ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT)");
                s.execute("CREATE TABLE ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR)");
                s.execute("CREATE TABLE ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)");
                s.execute("CREATE TABLE ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT)");
                s.execute("CREATE TABLE ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN)");
                s.execute("CREATE TABLE ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)");
                s.execute("CREATE TABLE ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)");
                s.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                s.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dp + "')");
                s.execute("INSERT INTO ducklake_snapshot VALUES (0, datetime('now'), 0, 1, 0)");
                s.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)");
                s.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', 1)");
            } c.commit();
        }
    }
    private static void deleteRecursive(File f) { if (f.isDirectory()) { File[] c = f.listFiles(); if (c != null) for (File x : c) deleteRecursive(x); } f.delete(); }
}
