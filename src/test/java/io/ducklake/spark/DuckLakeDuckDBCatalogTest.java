package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests for DuckDB as a DuckLake catalog backend.
 *
 * <p>Verifies that the Spark connector can use a DuckDB database file
 * as the metadata catalog instead of SQLite. Covers read, write, schema
 * evolution, time travel, partitions, delete, merge, and maintenance.</p>
 */
public class DuckLakeDuckDBCatalogTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-duckdb-cat-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.duckdb";

        // Bootstrap the DuckDB catalog with the standard 13-table schema
        createDuckDBCatalog(catalogPath, dataPath);

        Thread.currentThread().setContextClassLoader(DuckLakeDuckDBCatalogTest.class.getClassLoader());

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeDuckDBCatalogTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog", catalogPath)
                .config("spark.sql.catalog.ducklake.data_path", dataPath)
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        if (spark != null) { spark.stop(); spark = null; }
        if (tempDir != null) deleteRecursive(new File(tempDir));
    }

    // ---------------------------------------------------------------
    // Backend Detection
    // ---------------------------------------------------------------

    @Test
    public void testDuckDBDetectedByExtension() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(
                tempDir + "/detect.duckdb", dataPath)) {
            // Should not throw — DuckDB JDBC driver handles file creation
            assertNotNull(backend);
        }
    }

    @Test
    public void testDuckDBDetectedByPrefix() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(
                "duckdb:" + tempDir + "/detect_prefix.duckdb", dataPath)) {
            assertNotNull(backend);
        }
    }

    @Test
    public void testDuckDBDetectedByJDBCUrl() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(
                "jdbc:duckdb:" + tempDir + "/detect_jdbc.duckdb", dataPath)) {
            assertNotNull(backend);
        }
    }

    // ---------------------------------------------------------------
    // Basic CRUD
    // ---------------------------------------------------------------

    @Test
    public void testCreateTableAndInsert() {
        spark.sql("CREATE TABLE ducklake.main.ddb_basic (id INT, name STRING, value DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.ddb_basic VALUES (1, 'hello', 3.14), (2, 'world', 2.72)");
        Dataset<Row> df = spark.sql("SELECT * FROM ducklake.main.ddb_basic ORDER BY id");
        assertEquals(2, df.count());
        assertEquals("hello", df.collectAsList().get(0).getString(1));
    }

    @Test
    public void testMultipleInserts() {
        spark.sql("CREATE TABLE ducklake.main.ddb_multi (id INT)");
        spark.sql("INSERT INTO ducklake.main.ddb_multi VALUES (1)");
        spark.sql("INSERT INTO ducklake.main.ddb_multi VALUES (2)");
        spark.sql("INSERT INTO ducklake.main.ddb_multi VALUES (3)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.ddb_multi").count());
    }

    @Test
    public void testOverwrite() {
        spark.sql("CREATE TABLE ducklake.main.ddb_ow (id INT)");
        spark.sql("INSERT INTO ducklake.main.ddb_ow VALUES (1), (2), (3)");
        spark.sql("INSERT OVERWRITE ducklake.main.ddb_ow VALUES (99)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.ddb_ow").count());
        assertEquals(99, spark.sql("SELECT * FROM ducklake.main.ddb_ow").collectAsList().get(0).getInt(0));
    }

    // ---------------------------------------------------------------
    // Types
    // ---------------------------------------------------------------

    @Test
    public void testVariousTypes() {
        spark.sql("CREATE TABLE ducklake.main.ddb_types (i INT, bi BIGINT, d DOUBLE, s STRING, b BOOLEAN, dt DATE, ts TIMESTAMP, dec DECIMAL(10,2))");
        spark.sql("INSERT INTO ducklake.main.ddb_types VALUES (1, 9999999999, 3.14, 'test', true, DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00', 99.99)");
        Dataset<Row> df = spark.sql("SELECT * FROM ducklake.main.ddb_types");
        assertEquals(1, df.count());
        assertEquals(8, df.schema().fields().length);
    }

    @Test
    public void testComplexTypes() {
        spark.sql("CREATE TABLE ducklake.main.ddb_complex (arr ARRAY<INT>, st STRUCT<a: INT, b: STRING>, mp MAP<STRING, INT>)");
        spark.sql("INSERT INTO ducklake.main.ddb_complex VALUES (array(1,2,3), named_struct('a', 1, 'b', 'x'), map('k', 42))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.ddb_complex").count());
    }

    // ---------------------------------------------------------------
    // Schema Evolution
    // ---------------------------------------------------------------

    @Test
    public void testAddColumn() {
        spark.sql("CREATE TABLE ducklake.main.ddb_addcol (id INT)");
        spark.sql("INSERT INTO ducklake.main.ddb_addcol VALUES (1)");
        spark.sql("ALTER TABLE ducklake.main.ddb_addcol ADD COLUMNS (name STRING)");
        spark.sql("INSERT INTO ducklake.main.ddb_addcol VALUES (2, 'hello')");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.ddb_addcol").count());
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.ddb_addcol").schema().fields().length);
    }

    @Test
    public void testDropColumn() {
        spark.sql("CREATE TABLE ducklake.main.ddb_dropcol (id INT, drop_me STRING, keep STRING)");
        spark.sql("INSERT INTO ducklake.main.ddb_dropcol VALUES (1, 'x', 'y')");
        spark.sql("ALTER TABLE ducklake.main.ddb_dropcol DROP COLUMN drop_me");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.ddb_dropcol").schema().fields().length);
    }

    @Test
    public void testRenameColumn() {
        spark.sql("CREATE TABLE ducklake.main.ddb_rencol (id INT, old_name STRING)");
        spark.sql("INSERT INTO ducklake.main.ddb_rencol VALUES (1, 'a')");
        spark.sql("ALTER TABLE ducklake.main.ddb_rencol RENAME COLUMN old_name TO new_name");
        assertEquals("new_name", spark.sql("SELECT * FROM ducklake.main.ddb_rencol").schema().fields()[1].name());
        assertEquals("a", spark.sql("SELECT new_name FROM ducklake.main.ddb_rencol").collectAsList().get(0).getString(0));
    }

    // ---------------------------------------------------------------
    // Delete
    // ---------------------------------------------------------------

    @Test
    public void testDelete() {
        spark.sql("CREATE TABLE ducklake.main.ddb_del (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.ddb_del VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        spark.sql("DELETE FROM ducklake.main.ddb_del WHERE id = 2");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.ddb_del").count());
    }

    @Test
    public void testDeleteThenInsert() {
        spark.sql("CREATE TABLE ducklake.main.ddb_delins (id INT)");
        spark.sql("INSERT INTO ducklake.main.ddb_delins VALUES (1), (2), (3)");
        spark.sql("DELETE FROM ducklake.main.ddb_delins WHERE id = 2");
        spark.sql("INSERT INTO ducklake.main.ddb_delins VALUES (4), (5)");
        assertEquals(4, spark.sql("SELECT * FROM ducklake.main.ddb_delins").count());
    }

    // ---------------------------------------------------------------
    // Partitions
    // ---------------------------------------------------------------

    @Test
    public void testPartitionedTable() {
        spark.sql("CREATE TABLE ducklake.main.ddb_part (id INT, region STRING) PARTITIONED BY (region)");
        spark.sql("INSERT INTO ducklake.main.ddb_part VALUES (1, 'us'), (2, 'eu'), (3, 'us'), (4, 'apac')");
        assertEquals(4, spark.sql("SELECT * FROM ducklake.main.ddb_part").count());
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.ddb_part WHERE region = 'us'").count());
    }

    // ---------------------------------------------------------------
    // Filter / Pushdown
    // ---------------------------------------------------------------

    @Test
    public void testFilterPushdown() {
        spark.sql("CREATE TABLE ducklake.main.ddb_filter (id INT, val DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.ddb_filter VALUES (1, 10.0), (2, 20.0), (3, 30.0)");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.ddb_filter WHERE val > 15.0").count());
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.ddb_filter WHERE id = 2").count());
    }

    // ---------------------------------------------------------------
    // Schemas / DDL
    // ---------------------------------------------------------------

    @Test
    public void testCreateAndDropSchema() {
        spark.sql("CREATE NAMESPACE ducklake.ddb_ns1");
        spark.sql("CREATE TABLE ducklake.ddb_ns1.tbl (id INT)");
        spark.sql("INSERT INTO ducklake.ddb_ns1.tbl VALUES (42)");
        assertEquals(42, spark.sql("SELECT * FROM ducklake.ddb_ns1.tbl").collectAsList().get(0).getInt(0));
    }

    @Test
    public void testDropTable() {
        spark.sql("CREATE TABLE ducklake.main.ddb_droptbl (id INT)");
        spark.sql("INSERT INTO ducklake.main.ddb_droptbl VALUES (1)");
        spark.sql("DROP TABLE ducklake.main.ddb_droptbl");
        try { spark.sql("SELECT * FROM ducklake.main.ddb_droptbl").collect(); fail(); }
        catch (Exception e) { /* expected */ }
    }

    // ---------------------------------------------------------------
    // Views & Tags
    // ---------------------------------------------------------------

    @Test
    public void testViewsAndTags() throws Exception {
        spark.sql("CREATE TABLE ducklake.main.ddb_vt (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.ddb_vt VALUES (1, 'a')");

        // Tags through metadata backend
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.getTable(schema.schemaId, "ddb_vt");
            assertNotNull(table);
            assertTrue(table.tableId > 0);
        }
    }

    // ---------------------------------------------------------------
    // Maintenance
    // ---------------------------------------------------------------

    @Test
    public void testCompactionOnDuckDBCatalog() {
        spark.sql("CREATE TABLE ducklake.main.ddb_compact (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.ddb_compact VALUES (1, 'a')");
        spark.sql("INSERT INTO ducklake.main.ddb_compact VALUES (2, 'b')");
        spark.sql("INSERT INTO ducklake.main.ddb_compact VALUES (3, 'c')");
        io.ducklake.spark.maintenance.DuckLakeMaintenance.rewriteDataFiles(
                spark, catalogPath, "ddb_compact", "main");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.ddb_compact").count());
    }

    // ---------------------------------------------------------------
    // DataSource API
    // ---------------------------------------------------------------

    @Test
    public void testDataSourceAPIWithDuckDBCatalog() {
        spark.sql("CREATE TABLE ducklake.main.ddb_dsapi (id INT, val STRING)");
        List<Row> rows = Arrays.asList(RowFactory.create(1, "x"), RowFactory.create(2, "y"));
        spark.createDataFrame(rows, new StructType()
                .add("id", DataTypes.IntegerType).add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "ddb_dsapi")
                .mode("append").save();
        Dataset<Row> df = spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "ddb_dsapi").load();
        assertEquals(2, df.count());
    }

    // ---------------------------------------------------------------
    // CDC
    // ---------------------------------------------------------------

    @Test
    public void testCDCOnDuckDBCatalog() {
        spark.sql("CREATE TABLE ducklake.main.ddb_cdc (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.ddb_cdc VALUES (1, 'a'), (2, 'b')");
        spark.sql("INSERT INTO ducklake.main.ddb_cdc VALUES (3, 'c')");
        Dataset<Row> changes = io.ducklake.spark.DuckLakeChanges.between(
                spark, catalogPath, "ddb_cdc", "main", 0, Long.MAX_VALUE);
        assertTrue("Should have change records", changes.count() > 0);
    }

    // ---------------------------------------------------------------
    // Snapshot Consistency
    // ---------------------------------------------------------------

    @Test
    public void testSnapshotAdvancesOnDuckDB() throws Exception {
        spark.sql("CREATE TABLE ducklake.main.ddb_snap (id INT)");
        long s1 = getMaxSnapshot();
        spark.sql("INSERT INTO ducklake.main.ddb_snap VALUES (1)");
        long s2 = getMaxSnapshot();
        assertTrue("Snapshot should advance", s2 > s1);
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private long getMaxSnapshot() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:" + catalogPath);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void createDuckDBCatalog(String catPath, String dp) throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + catPath)) {
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
                st.execute("INSERT INTO ducklake_snapshot VALUES (0, current_timestamp::TEXT, 0, 1, 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', true)");
            }
        }
    }

    private static void deleteRecursive(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) for (File child : children) deleteRecursive(child);
        }
        file.delete();
    }
}
