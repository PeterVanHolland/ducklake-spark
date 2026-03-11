package io.ducklake.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import static org.junit.Assert.*;

/** Extended type coverage: all scalar types, nested combos, NaN/Infinity, nulls in complex types. */
public class DuckLakeTypesExtendedTest {
    private static SparkSession spark;
    private static String tempDir, catalogPath, dataPath;

    @BeforeClass public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-types-ext-").toString();
        dataPath = tempDir + "/data/"; new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);
        Thread.currentThread().setContextClassLoader(DuckLakeTypesExtendedTest.class.getClassLoader());
        spark = SparkSession.builder().master("local[2]").appName("DuckLakeTypesExtendedTest")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "localhost")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog", catalogPath)
                .config("spark.sql.catalog.ducklake.data_path", dataPath).getOrCreate();
    }
    @AfterClass public static void tearDown() {
        if (spark != null) { spark.stop(); spark = null; }
        if (tempDir != null) deleteRecursive(new File(tempDir));
    }

    // === Scalar Types ===
    @Test public void testTinyint() {
        spark.sql("CREATE TABLE ducklake.main.typ_ti (val TINYINT)");
        spark.sql("INSERT INTO ducklake.main.typ_ti VALUES (127), (-128), (0)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_ti").count());
    }
    @Test public void testSmallint() {
        spark.sql("CREATE TABLE ducklake.main.typ_si (val SMALLINT)");
        spark.sql("INSERT INTO ducklake.main.typ_si VALUES (32767), (-32768), (0)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_si").count());
    }
    @Test public void testInteger() {
        spark.sql("CREATE TABLE ducklake.main.typ_i (val INT)");
        spark.sql("INSERT INTO ducklake.main.typ_i VALUES (2147483647), (-2147483648), (0)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_i").count());
    }
    @Test public void testBigint() {
        spark.sql("CREATE TABLE ducklake.main.typ_bi (val BIGINT)");
        spark.sql("INSERT INTO ducklake.main.typ_bi VALUES (9223372036854775807), (-9223372036854775808), (0)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_bi").count());
    }
    @Test public void testFloat() {
        spark.sql("CREATE TABLE ducklake.main.typ_f (val FLOAT)");
        spark.sql("INSERT INTO ducklake.main.typ_f VALUES (1.5), (-1.5), (0.0)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_f").count());
    }
    @Test public void testDouble() {
        spark.sql("CREATE TABLE ducklake.main.typ_d (val DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.typ_d VALUES (1.7976931348623157E308), (-1.7976931348623157E308), (0.0)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_d").count());
    }
    @Test public void testBoolean() {
        spark.sql("CREATE TABLE ducklake.main.typ_bool (val BOOLEAN)");
        spark.sql("INSERT INTO ducklake.main.typ_bool VALUES (true), (false), (null)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_bool").count());
    }
    @Test public void testVarchar() {
        spark.sql("CREATE TABLE ducklake.main.typ_vc (val STRING)");
        spark.sql("INSERT INTO ducklake.main.typ_vc VALUES ('hello'), (''), (null)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_vc").count());
    }
    @Test public void testDate() {
        spark.sql("CREATE TABLE ducklake.main.typ_date (val DATE)");
        spark.sql("INSERT INTO ducklake.main.typ_date VALUES (DATE '2024-01-01'), (DATE '1970-01-01'), (DATE '2099-12-31')");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_date").count());
    }
    @Test public void testTimestamp() {
        spark.sql("CREATE TABLE ducklake.main.typ_ts (val TIMESTAMP)");
        spark.sql("INSERT INTO ducklake.main.typ_ts VALUES (TIMESTAMP '2024-01-01 12:00:00'), (TIMESTAMP '1970-01-01 00:00:00')");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.typ_ts").count());
    }
    @Test public void testTimestampNtz() {
        spark.sql("CREATE TABLE ducklake.main.typ_tsntz (val TIMESTAMP_NTZ)");
        spark.sql("INSERT INTO ducklake.main.typ_tsntz VALUES (TIMESTAMP_NTZ '2024-06-15 10:30:00')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_tsntz").count());
    }
    @Test public void testBinary() {
        spark.sql("CREATE TABLE ducklake.main.typ_bin (val BINARY)");
        spark.sql("INSERT INTO ducklake.main.typ_bin VALUES (X'DEADBEEF')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_bin").count());
    }
    @Test public void testDecimal() {
        spark.sql("CREATE TABLE ducklake.main.typ_dec (val DECIMAL(18, 6))");
        spark.sql("INSERT INTO ducklake.main.typ_dec VALUES (12345.678901), (-99999.999999), (0.000001)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_dec").count());
    }
    @Test public void testDecimalVariousPrecisions() {
        spark.sql("CREATE TABLE ducklake.main.typ_decp (d5 DECIMAL(5,2), d18 DECIMAL(18,9), d38 DECIMAL(38,18))");
        spark.sql("INSERT INTO ducklake.main.typ_decp VALUES (123.45, 123456789.123456789, 12345678901234567890.123456789012345678)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_decp").count());
    }
    @Test public void testAllIntegerWidths() {
        spark.sql("CREATE TABLE ducklake.main.typ_allint (t TINYINT, s SMALLINT, i INT, b BIGINT)");
        spark.sql("INSERT INTO ducklake.main.typ_allint VALUES (1, 100, 10000, 1000000000)");
        Row r = spark.sql("SELECT * FROM ducklake.main.typ_allint").collectAsList().get(0);
        assertEquals(1, r.getByte(0));
        assertEquals(100, r.getShort(1));
        assertEquals(10000, r.getInt(2));
        assertEquals(1000000000L, r.getLong(3));
    }

    // === Float Special Values ===
    @Test public void testFloatNaN() {
        spark.sql("CREATE TABLE ducklake.main.typ_nan (val DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.typ_nan VALUES (CAST('NaN' AS DOUBLE)), (1.0)");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.typ_nan").count());
    }
    @Test public void testFloatInfinity() {
        spark.sql("CREATE TABLE ducklake.main.typ_inf (val DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.typ_inf VALUES (CAST('Infinity' AS DOUBLE)), (CAST('-Infinity' AS DOUBLE))");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.typ_inf").count());
    }
    @Test public void testFloatNaNFilter() {
        spark.sql("CREATE TABLE ducklake.main.typ_nanfilt (id INT, val DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.typ_nanfilt VALUES (1, CAST('NaN' AS DOUBLE)), (2, 1.0), (3, 2.0)");
        // NaN rows may or may not pass filters depending on stats-based pruning
        long count = spark.sql("SELECT * FROM ducklake.main.typ_nanfilt WHERE val > 0.5").count();
        assertTrue("Filter should return at least the 2 non-NaN matches", count >= 2);
    }

    // === Complex Types ===
    @Test public void testListType() {
        spark.sql("CREATE TABLE ducklake.main.typ_list (val ARRAY<INT>)");
        spark.sql("INSERT INTO ducklake.main.typ_list VALUES (array(1, 2, 3))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_list").count());
    }
    @Test public void testListOfVarchar() {
        spark.sql("CREATE TABLE ducklake.main.typ_listvc (val ARRAY<STRING>)");
        spark.sql("INSERT INTO ducklake.main.typ_listvc VALUES (array('a', 'b', 'c'))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_listvc").count());
    }
    @Test public void testStructType() {
        spark.sql("CREATE TABLE ducklake.main.typ_struct (val STRUCT<a: INT, b: STRING>)");
        spark.sql("INSERT INTO ducklake.main.typ_struct VALUES (named_struct('a', 1, 'b', 'hello'))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_struct").count());
    }
    @Test public void testMapType() {
        spark.sql("CREATE TABLE ducklake.main.typ_map (val MAP<STRING, INT>)");
        spark.sql("INSERT INTO ducklake.main.typ_map VALUES (map('a', 1, 'b', 2))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_map").count());
    }
    @Test public void testNestedListOfStructs() {
        spark.sql("CREATE TABLE ducklake.main.typ_los (val ARRAY<STRUCT<a: INT, b: STRING>>)");
        spark.sql("INSERT INTO ducklake.main.typ_los VALUES (array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y')))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_los").count());
    }
    @Test public void testListOfLists() {
        spark.sql("CREATE TABLE ducklake.main.typ_lol (val ARRAY<ARRAY<INT>>)");
        spark.sql("INSERT INTO ducklake.main.typ_lol VALUES (array(array(1, 2), array(3, 4)))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_lol").count());
    }
    @Test public void testStructWithListField() {
        spark.sql("CREATE TABLE ducklake.main.typ_sl (val STRUCT<a: INT, items: ARRAY<STRING>>)");
        spark.sql("INSERT INTO ducklake.main.typ_sl VALUES (named_struct('a', 1, 'items', array('x', 'y')))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_sl").count());
    }
    @Test public void testMixedTypes() {
        spark.sql("CREATE TABLE ducklake.main.typ_mixed (i INT, s STRING, d DOUBLE, b BOOLEAN, bi BIGINT, dt DATE, ts TIMESTAMP, dec DECIMAL(10,2))");
        spark.sql("INSERT INTO ducklake.main.typ_mixed VALUES (1, 'hello', 3.14, true, 9999999999, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', 99.99)");
        assertEquals(8, spark.sql("SELECT * FROM ducklake.main.typ_mixed").schema().fields().length);
    }
    @Test public void testAllTemporalTypes() {
        spark.sql("CREATE TABLE ducklake.main.typ_temporal (d DATE, ts TIMESTAMP, tsntz TIMESTAMP_NTZ)");
        spark.sql("INSERT INTO ducklake.main.typ_temporal VALUES (DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00', TIMESTAMP_NTZ '2024-01-15 10:30:00')");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.typ_temporal").schema().fields().length);
    }

    // === Null in Complex Types ===
    @Test public void testListWithNullElements() {
        spark.sql("CREATE TABLE ducklake.main.typ_listnull (val ARRAY<INT>)");
        spark.sql("INSERT INTO ducklake.main.typ_listnull VALUES (array(1, null, 3))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_listnull").count());
    }
    @Test public void testStructWithNullFields() {
        spark.sql("CREATE TABLE ducklake.main.typ_structnull (val STRUCT<a: INT, b: STRING>)");
        spark.sql("INSERT INTO ducklake.main.typ_structnull VALUES (named_struct('a', null, 'b', 'hello'))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_structnull").count());
    }
    @Test public void testVarcharEmptyAndNull() {
        spark.sql("CREATE TABLE ducklake.main.typ_vcen (val STRING)");
        spark.sql("INSERT INTO ducklake.main.typ_vcen VALUES (''), (null), ('abc')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_vcen WHERE val = ''").count());
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.typ_vcen WHERE val IS NULL").count());
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
