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
 * Integration tests for ALTER TABLE operations via the DuckLakeCatalog plugin.
 * Tests ADD COLUMN, DROP COLUMN, RENAME COLUMN through Spark SQL DDL,
 * and verifies schema changes are reflected in reads and writes.
 */
public class DuckLakeAlterTableTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-alter-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);

        Thread.currentThread().setContextClassLoader(DuckLakeAlterTableTest.class.getClassLoader());

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeAlterTableTest")
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
    // Test 1: ADD COLUMN via SQL + verify read
    // ---------------------------------------------------------------

    @Test
    public void testAddColumn() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.alt_add (id INT, name STRING)");
        new File(dataPath + "main/alt_add/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.alt_add VALUES (1, 'alice'), (2, 'bob')");

        // Add a column
        spark.sql("ALTER TABLE ducklake.main.alt_add ADD COLUMN age INT");

        // Verify the schema has 3 columns
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.alt_add ORDER BY id");
        StructType schema = result.schema();
        assertEquals(3, schema.fields().length);
        assertEquals("id", schema.fields()[0].name());
        assertEquals("name", schema.fields()[1].name());
        assertEquals("age", schema.fields()[2].name());

        // Old rows should have null for the new column
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertTrue(rows.get(0).isNullAt(2));
    }

    // ---------------------------------------------------------------
    // Test 2: DROP COLUMN via SQL + verify
    // ---------------------------------------------------------------

    @Test
    public void testDropColumn() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.alt_drop (id INT, name STRING, extra STRING)");
        new File(dataPath + "main/alt_drop/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.alt_drop VALUES (1, 'alice', 'x'), (2, 'bob', 'y')");

        // Drop the extra column
        spark.sql("ALTER TABLE ducklake.main.alt_drop DROP COLUMN extra");

        // Verify only 2 columns remain
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.alt_drop ORDER BY id");
        StructType schema = result.schema();
        assertEquals(2, schema.fields().length);
        assertEquals("id", schema.fields()[0].name());
        assertEquals("name", schema.fields()[1].name());

        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("bob", rows.get(1).getString(1));
    }

    // ---------------------------------------------------------------
    // Test 3: RENAME COLUMN via SQL + verify
    // ---------------------------------------------------------------

    @Test
    public void testRenameColumn() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.alt_rename (id INT, name STRING)");
        new File(dataPath + "main/alt_rename/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.alt_rename VALUES (1, 'alice'), (2, 'bob')");

        // Rename the column
        spark.sql("ALTER TABLE ducklake.main.alt_rename RENAME COLUMN name TO full_name");

        // Verify column is renamed
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.alt_rename ORDER BY id");
        StructType schema = result.schema();
        assertEquals(2, schema.fields().length);
        assertEquals("id", schema.fields()[0].name());
        assertEquals("full_name", schema.fields()[1].name());

        // Data should still be there
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals("bob", rows.get(1).getString(1));
    }

    // ---------------------------------------------------------------
    // Test 4: ADD COLUMN with write after (schema evolution)
    // ---------------------------------------------------------------

    @Test
    public void testAddColumnThenWrite() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.alt_add_write (id INT, name STRING)");
        new File(dataPath + "main/alt_add_write/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.alt_add_write VALUES (1, 'alice')");

        // Add a column
        spark.sql("ALTER TABLE ducklake.main.alt_add_write ADD COLUMN score DOUBLE");

        // Write new data with the new column
        spark.sql("INSERT INTO ducklake.main.alt_add_write VALUES (2, 'bob', 95.5)");

        // Read all rows -- old row should have null score, new row has value
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.alt_add_write ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());

        // Old row: score is null
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertTrue(rows.get(0).isNullAt(2));

        // New row: score has value
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("bob", rows.get(1).getString(1));
        assertEquals(95.5, rows.get(1).getDouble(2), 0.001);
    }

    // ---------------------------------------------------------------
    // Test 5: Multiple ALTER operations in sequence
    // ---------------------------------------------------------------

    @Test
    public void testMultipleAlterSequence() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.alt_multi (id INT, name STRING)");
        new File(dataPath + "main/alt_multi/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.alt_multi VALUES (1, 'alice')");

        // 1) Add column
        spark.sql("ALTER TABLE ducklake.main.alt_multi ADD COLUMN age INT");

        // 2) Rename column
        spark.sql("ALTER TABLE ducklake.main.alt_multi RENAME COLUMN name TO full_name");

        // 3) Add another column
        spark.sql("ALTER TABLE ducklake.main.alt_multi ADD COLUMN email STRING");

        // Verify final schema
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.alt_multi ORDER BY id");
        StructType schema = result.schema();
        assertEquals(4, schema.fields().length);
        assertEquals("id", schema.fields()[0].name());
        assertEquals("full_name", schema.fields()[1].name());
        assertEquals("age", schema.fields()[2].name());
        assertEquals("email", schema.fields()[3].name());

        // Old row should have null for added columns, renamed column keeps value
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertTrue(rows.get(0).isNullAt(2)); // age
        assertTrue(rows.get(0).isNullAt(3)); // email
    }

    // ---------------------------------------------------------------
    // Test 6: ADD then DROP column
    // ---------------------------------------------------------------

    @Test
    public void testAddThenDropColumn() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.alt_add_drop (id INT, name STRING)");
        new File(dataPath + "main/alt_add_drop/").mkdirs();

        spark.sql("INSERT INTO ducklake.main.alt_add_drop VALUES (1, 'alice')");

        // Add column
        spark.sql("ALTER TABLE ducklake.main.alt_add_drop ADD COLUMN temp STRING");

        // Verify 3 columns
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.alt_add_drop").schema().fields().length);

        // Drop it
        spark.sql("ALTER TABLE ducklake.main.alt_add_drop DROP COLUMN temp");

        // Verify back to 2 columns
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.alt_add_drop ORDER BY id");
        assertEquals(2, result.schema().fields().length);
        assertEquals("id", result.schema().fields()[0].name());
        assertEquals("name", result.schema().fields()[1].name());

        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
    }

    // ---------------------------------------------------------------
    // Test 7: ALTER TABLE on empty table
    // ---------------------------------------------------------------

    @Test
    public void testAlterEmptyTable() {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.alt_empty (id INT)");

        // Add column to empty table
        spark.sql("ALTER TABLE ducklake.main.alt_empty ADD COLUMN name STRING");

        // Verify schema
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.alt_empty");
        StructType schema = result.schema();
        assertEquals(2, schema.fields().length);
        assertEquals("id", schema.fields()[0].name());
        assertEquals("name", schema.fields()[1].name());
        assertEquals(0, result.count());
    }

    // ---------------------------------------------------------------
    // Catalog setup helper
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
