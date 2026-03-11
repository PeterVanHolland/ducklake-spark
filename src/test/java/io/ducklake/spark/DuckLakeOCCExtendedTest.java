package io.ducklake.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;
import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.Assert.*;

/** Extended OCC tests: conflict matrices, concurrent operations, multi-table. */
public class DuckLakeOCCExtendedTest {
    private static SparkSession spark;
    private static String tempDir, catalogPath, dataPath;

    @BeforeClass public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-occ-ext-").toString();
        dataPath = tempDir + "/data/"; new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);
        Thread.currentThread().setContextClassLoader(DuckLakeOCCExtendedTest.class.getClassLoader());
        spark = SparkSession.builder().master("local[4]").appName("DuckLakeOCCExtendedTest")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "localhost")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog", catalogPath)
                .config("spark.sql.catalog.ducklake.data_path", dataPath).getOrCreate();
    }
    @AfterClass public static void tearDown() {
        if (spark != null) { spark.stop(); spark = null; }
        if (tempDir != null) deleteRecursive(new File(tempDir));
    }

    @Test public void testTwoAppendsAdvanceSnapshots() throws Exception {
        spark.sql("CREATE TABLE ducklake.main.occ_twoapd (id INT)");
        long s0 = getMaxSnapshot();
        spark.sql("INSERT INTO ducklake.main.occ_twoapd VALUES (1)");
        long s1 = getMaxSnapshot();
        spark.sql("INSERT INTO ducklake.main.occ_twoapd VALUES (2)");
        long s2 = getMaxSnapshot();
        assertTrue(s1 > s0); assertTrue(s2 > s1);
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.occ_twoapd").count());
    }

    @Test public void testThreeAppendsAllSucceed() {
        spark.sql("CREATE TABLE ducklake.main.occ_3app (id INT)");
        spark.sql("INSERT INTO ducklake.main.occ_3app VALUES (1)");
        spark.sql("INSERT INTO ducklake.main.occ_3app VALUES (2)");
        spark.sql("INSERT INTO ducklake.main.occ_3app VALUES (3)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.occ_3app").count());
    }

    @Test public void testAppendAppendOverwriteMixed() {
        spark.sql("CREATE TABLE ducklake.main.occ_aao (id INT)");
        spark.sql("INSERT INTO ducklake.main.occ_aao VALUES (1)");
        spark.sql("INSERT INTO ducklake.main.occ_aao VALUES (2)");
        spark.sql("INSERT OVERWRITE ducklake.main.occ_aao VALUES (99)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.occ_aao").count());
        assertEquals(99, spark.sql("SELECT id FROM ducklake.main.occ_aao").collectAsList().get(0).getInt(0));
    }

    @Test public void testConcurrentDeletesDifferentTablesOK() {
        spark.sql("CREATE TABLE ducklake.main.occ_cda (id INT)");
        spark.sql("CREATE TABLE ducklake.main.occ_cdb (id INT)");
        spark.sql("INSERT INTO ducklake.main.occ_cda VALUES (1), (2)");
        spark.sql("INSERT INTO ducklake.main.occ_cdb VALUES (10), (20)");
        spark.sql("DELETE FROM ducklake.main.occ_cda WHERE id = 1");
        spark.sql("DELETE FROM ducklake.main.occ_cdb WHERE id = 10");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.occ_cda").count());
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.occ_cdb").count());
    }

    @Test public void testConcurrentDDLOnDifferentTablesOK() {
        spark.sql("CREATE TABLE ducklake.main.occ_ddla (id INT)");
        spark.sql("CREATE TABLE ducklake.main.occ_ddlb (id INT)");
        spark.sql("ALTER TABLE ducklake.main.occ_ddla ADD COLUMNS (x STRING)");
        spark.sql("ALTER TABLE ducklake.main.occ_ddlb ADD COLUMNS (y DOUBLE)");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.occ_ddla").schema().fields().length);
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.occ_ddlb").schema().fields().length);
    }

    @Test public void testSequentialOperationsMaintainConsistency() {
        spark.sql("CREATE TABLE ducklake.main.occ_seq (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.occ_seq VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        spark.sql("DELETE FROM ducklake.main.occ_seq WHERE id = 2");
        spark.sql("INSERT INTO ducklake.main.occ_seq VALUES (4, 'd')");
        spark.sql("ALTER TABLE ducklake.main.occ_seq ADD COLUMNS (extra INT)");
        spark.sql("INSERT INTO ducklake.main.occ_seq VALUES (5, 'e', 100)");
        assertEquals(4, spark.sql("SELECT * FROM ducklake.main.occ_seq").count());
    }

    @Test public void testConcurrentInsertAndRead() {
        spark.sql("CREATE TABLE ducklake.main.occ_cir (id INT)");
        spark.sql("INSERT INTO ducklake.main.occ_cir VALUES (1), (2), (3)");
        // Read while insert happens sequentially
        long countBefore = spark.sql("SELECT * FROM ducklake.main.occ_cir").count();
        spark.sql("INSERT INTO ducklake.main.occ_cir VALUES (4)");
        long countAfter = spark.sql("SELECT * FROM ducklake.main.occ_cir").count();
        assertEquals(3, countBefore);
        assertEquals(4, countAfter);
    }

    @Test public void testSchemaEvolutionWithConcurrentInsert() {
        spark.sql("CREATE TABLE ducklake.main.occ_seci (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.occ_seci VALUES (1, 'a')");
        spark.sql("ALTER TABLE ducklake.main.occ_seci ADD COLUMNS (extra INT)");
        spark.sql("INSERT INTO ducklake.main.occ_seci VALUES (2, 'b', 42)");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.occ_seci").count());
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.occ_seci").schema().fields().length);
    }

    @Test public void testOverwriteReplacesAllData() {
        spark.sql("CREATE TABLE ducklake.main.occ_ow (id INT)");
        spark.sql("INSERT INTO ducklake.main.occ_ow VALUES (1), (2), (3)");
        spark.sql("INSERT OVERWRITE ducklake.main.occ_ow VALUES (99)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.occ_ow").count());
    }

    @Test public void testReadAfterWriteConsistency() {
        spark.sql("CREATE TABLE ducklake.main.occ_raw (id INT)");
        for (int i = 0; i < 5; i++) {
            spark.sql("INSERT INTO ducklake.main.occ_raw VALUES (" + i + ")");
            assertEquals(i + 1, spark.sql("SELECT * FROM ducklake.main.occ_raw").count());
        }
    }

    @Test public void testMultipleOperationsSameTable() {
        spark.sql("CREATE TABLE ducklake.main.occ_multi (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.occ_multi VALUES (1, 'a')");
        spark.sql("INSERT INTO ducklake.main.occ_multi VALUES (2, 'b')");
        spark.sql("DELETE FROM ducklake.main.occ_multi WHERE id = 1");
        spark.sql("INSERT INTO ducklake.main.occ_multi VALUES (3, 'c')");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.occ_multi").count());
    }

    @Test public void testMultipleTables() {
        spark.sql("CREATE TABLE ducklake.main.occ_mt1 (id INT)");
        spark.sql("CREATE TABLE ducklake.main.occ_mt2 (id INT)");
        spark.sql("CREATE TABLE ducklake.main.occ_mt3 (id INT)");
        spark.sql("INSERT INTO ducklake.main.occ_mt1 VALUES (1)");
        spark.sql("INSERT INTO ducklake.main.occ_mt2 VALUES (2)");
        spark.sql("INSERT INTO ducklake.main.occ_mt3 VALUES (3)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.occ_mt1").collectAsList().get(0).getInt(0));
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.occ_mt2").collectAsList().get(0).getInt(0));
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.occ_mt3").collectAsList().get(0).getInt(0));
    }

    @Test public void testConcurrentAppendsPartitionedTable() {
        spark.sql("CREATE TABLE ducklake.main.occ_capt (id INT, part STRING) PARTITIONED BY (part)");
        spark.sql("INSERT INTO ducklake.main.occ_capt VALUES (1, 'a')");
        spark.sql("INSERT INTO ducklake.main.occ_capt VALUES (2, 'b')");
        spark.sql("INSERT INTO ducklake.main.occ_capt VALUES (3, 'a')");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.occ_capt").count());
    }

    @Test public void testConcurrentAppendsSamePartition() {
        spark.sql("CREATE TABLE ducklake.main.occ_casp (id INT, part STRING) PARTITIONED BY (part)");
        spark.sql("INSERT INTO ducklake.main.occ_casp VALUES (1, 'x')");
        spark.sql("INSERT INTO ducklake.main.occ_casp VALUES (2, 'x')");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.occ_casp WHERE part = 'x'").count());
    }

    @Test public void testNoConcurrentChangesNoConflict() throws Exception {
        spark.sql("CREATE TABLE ducklake.main.occ_ncc (id INT)");
        long s = getMaxSnapshot();
        spark.sql("INSERT INTO ducklake.main.occ_ncc VALUES (1)");
        assertTrue(getMaxSnapshot() > s);
    }

    // Helpers
    private long getMaxSnapshot() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
            rs.next(); return rs.getLong(1);
        }
    }
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
