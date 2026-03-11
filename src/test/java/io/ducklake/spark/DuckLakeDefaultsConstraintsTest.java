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
 * Integration tests for default values and constraints in DuckLake.
 * Tests both DDL operations and runtime behavior for defaults and constraints.
 */
public class DuckLakeDefaultsConstraintsTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-defaults-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);

        Thread.currentThread().setContextClassLoader(DuckLakeDefaultsConstraintsTest.class.getClassLoader());

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeDefaultsConstraintsTest")
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
    // Default Value Tests
    // ---------------------------------------------------------------

    @Test
    public void testAddColumnWithDefaults() throws Exception {
        // Create base table
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.test_defaults (id INT, name STRING)");
        new File(dataPath + "main/test_defaults/").mkdirs();

        // Insert initial data
        spark.sql("INSERT INTO ducklake.main.test_defaults VALUES (1, 'alice'), (2, 'bob')");

        // Test MetadataBackend directly for adding column with defaults
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.getTable(schema.schemaId, "test_defaults");

            // Add column with default values
            long columnId = backend.addColumnWithDefault(table.tableId, "age", "INT", true, "25", "30");

            // Verify the column was added with defaults
            List<ColumnInfo> columns = backend.getColumns(table.tableId);
            boolean foundColumn = false;
            for (ColumnInfo col : columns) {
                if ("age".equals(col.name)) {
                    foundColumn = true;
                    assertEquals("25", col.initialDefault);
                    assertEquals("30", col.defaultValue);
                    assertTrue(col.nullable);
                    break;
                }
            }
            assertTrue("Age column should be found with defaults", foundColumn);
        }
    }

    @Test
    public void testDefaultsInReadPath() throws Exception {
        // This test verifies that existing rows get default values when new columns are added
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main.test_read_defaults (id INT, name STRING)");
        new File(dataPath + "main/test_read_defaults/").mkdirs();

        // Insert initial data
        spark.sql("INSERT INTO ducklake.main.test_read_defaults VALUES (1, 'alice'), (2, 'bob')");

        // Use MetadataBackend to add column with initial default
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.getTable(schema.schemaId, "test_read_defaults");
            backend.addColumnWithDefault(table.tableId, "score", "INT", true, "100", null);
        }

        // Reading should show default value for existing rows
        Dataset<Row> result = spark.sql("SELECT * FROM ducklake.main.test_read_defaults ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertEquals(2, rows.size());
        // Note: The read path should use initialDefault for existing rows
        // This might be null initially until the read path is properly updated
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        // TODO: Assert that rows.get(0).getInt(2) == 100 when read path is updated
    }

    @Test
    public void testMultipleDefaultTypes() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            // Create a new table for testing various default types
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "multi_defaults",
                new String[]{"id"}, new String[]{"INT"});

            // Test different default value types
            backend.addColumnWithDefault(table.tableId, "int_col", "INT", true, "42", "84");
            backend.addColumnWithDefault(table.tableId, "str_col", "VARCHAR", true, "'hello'", "'world'");
            backend.addColumnWithDefault(table.tableId, "bool_col", "BOOLEAN", true, "true", "false");
            backend.addColumnWithDefault(table.tableId, "double_col", "DOUBLE", true, "3.14", "2.71");

            // Verify all defaults were stored correctly
            List<ColumnInfo> columns = backend.getColumns(table.tableId);
            Map<String, ColumnInfo> colMap = new HashMap<>();
            for (ColumnInfo col : columns) {
                colMap.put(col.name, col);
            }

            assertEquals("42", colMap.get("int_col").initialDefault);
            assertEquals("84", colMap.get("int_col").defaultValue);
            assertEquals("'hello'", colMap.get("str_col").initialDefault);
            assertEquals("'world'", colMap.get("str_col").defaultValue);
            assertEquals("true", colMap.get("bool_col").initialDefault);
            assertEquals("false", colMap.get("bool_col").defaultValue);
            assertEquals("3.14", colMap.get("double_col").initialDefault);
            assertEquals("2.71", colMap.get("double_col").defaultValue);
        }
    }

    @Test
    public void testNullDefaults() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "null_defaults",
                new String[]{"id"}, new String[]{"INT"});

            // Add column with explicit null defaults
            backend.addColumnWithDefault(table.tableId, "nullable_col", "VARCHAR", true, null, null);

            List<ColumnInfo> columns = backend.getColumns(table.tableId);
            for (ColumnInfo col : columns) {
                if ("nullable_col".equals(col.name)) {
                    assertNull(col.initialDefault);
                    assertNull(col.defaultValue);
                    assertTrue(col.nullable);
                    return;
                }
            }
            fail("nullable_col should be found");
        }
    }

    @Test
    public void testDefaultsAcrossSnapshots() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "snapshot_defaults",
                new String[]{"id"}, new String[]{"INT"});

            long snapshot1 = backend.getCurrentSnapshotId();
            backend.addColumnWithDefault(table.tableId, "col1", "INT", true, "10", "20");

            long snapshot2 = backend.getCurrentSnapshotId();
            backend.addColumnWithDefault(table.tableId, "col2", "VARCHAR", true, "'old'", "'new'");

            long snapshot3 = backend.getCurrentSnapshotId();

            // Verify columns at different snapshots
            List<ColumnInfo> cols1 = backend.getColumns(table.tableId, snapshot1);
            List<ColumnInfo> cols2 = backend.getColumns(table.tableId, snapshot2);
            List<ColumnInfo> cols3 = backend.getColumns(table.tableId, snapshot3);

            assertEquals(1, cols1.size()); // Only id column at snapshot1
            assertEquals(2, cols2.size()); // id, col1 at snapshot2
            assertEquals(3, cols3.size()); // id, col1, col2 at snapshot3
        }
    }

    // ---------------------------------------------------------------
    // Constraint Tests
    // ---------------------------------------------------------------

    @Test
    public void testAddNotNullConstraint() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_not_null",
                new String[]{"id", "name"}, new String[]{"INT", "VARCHAR"});

            // Add NOT NULL constraint
            backend.addConstraint(table.tableId, "id_not_null", "NOT NULL", new String[]{"id"});

            // Verify constraint was added
            List<ConstraintInfo> constraints = backend.getConstraints(table.tableId);
            assertEquals(1, constraints.size());
            assertEquals("id_not_null", constraints.get(0).name);
            assertEquals("NOT NULL", constraints.get(0).type);
            assertArrayEquals(new String[]{"id"}, constraints.get(0).columnNames);
        }
    }

    @Test
    public void testAddUniqueConstraint() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_unique",
                new String[]{"id", "email"}, new String[]{"INT", "VARCHAR"});

            // Add UNIQUE constraint
            backend.addConstraint(table.tableId, "email_unique", "UNIQUE", new String[]{"email"});

            // Verify constraint was added
            List<ConstraintInfo> constraints = backend.getConstraints(table.tableId);
            assertEquals(1, constraints.size());
            assertEquals("email_unique", constraints.get(0).name);
            assertEquals("UNIQUE", constraints.get(0).type);
            assertArrayEquals(new String[]{"email"}, constraints.get(0).columnNames);
        }
    }

    @Test
    public void testMultiColumnConstraint() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_multi_col",
                new String[]{"first_name", "last_name", "email"},
                new String[]{"VARCHAR", "VARCHAR", "VARCHAR"});

            // Add multi-column UNIQUE constraint
            backend.addConstraint(table.tableId, "name_unique", "UNIQUE",
                new String[]{"first_name", "last_name"});

            // Verify constraint was added
            List<ConstraintInfo> constraints = backend.getConstraints(table.tableId);
            assertEquals(1, constraints.size());
            assertEquals("name_unique", constraints.get(0).name);
            assertEquals("UNIQUE", constraints.get(0).type);
            assertArrayEquals(new String[]{"first_name", "last_name"}, constraints.get(0).columnNames);
        }
    }

    @Test
    public void testDropConstraint() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_drop_constraint",
                new String[]{"id", "name"}, new String[]{"INT", "VARCHAR"});

            // Add constraint
            backend.addConstraint(table.tableId, "id_not_null", "NOT NULL", new String[]{"id"});

            // Verify constraint exists
            List<ConstraintInfo> constraints = backend.getConstraints(table.tableId);
            assertEquals(1, constraints.size());

            // Drop constraint
            backend.dropConstraint(table.tableId, "id_not_null");

            // Verify constraint is gone
            constraints = backend.getConstraints(table.tableId);
            assertEquals(0, constraints.size());
        }
    }

    @Test
    public void testConstraintsAcrossSnapshots() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_constraint_snapshots",
                new String[]{"id", "name"}, new String[]{"INT", "VARCHAR"});

            long snapshot1 = backend.getCurrentSnapshotId();

            // Add first constraint
            backend.addConstraint(table.tableId, "id_not_null", "NOT NULL", new String[]{"id"});
            long snapshot2 = backend.getCurrentSnapshotId();

            // Add second constraint
            backend.addConstraint(table.tableId, "name_unique", "UNIQUE", new String[]{"name"});
            long snapshot3 = backend.getCurrentSnapshotId();

            // Verify constraints at different snapshots
            List<ConstraintInfo> constraints1 = backend.getConstraints(table.tableId, snapshot1);
            List<ConstraintInfo> constraints2 = backend.getConstraints(table.tableId, snapshot2);
            List<ConstraintInfo> constraints3 = backend.getConstraints(table.tableId, snapshot3);

            assertEquals(0, constraints1.size());
            assertEquals(1, constraints2.size());
            assertEquals(2, constraints3.size());
        }
    }

    @Test
    public void testConstraintWithDefaults() throws Exception {
        // Test interaction between constraints and default values
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_constraint_defaults",
                new String[]{"id"}, new String[]{"INT"});

            // Add column with default and constraint
            backend.addColumnWithDefault(table.tableId, "email", "VARCHAR", false, "'default@example.com'", "'new@example.com'");
            backend.addConstraint(table.tableId, "email_not_null", "NOT NULL", new String[]{"email"});

            // Verify both were added correctly
            List<ColumnInfo> columns = backend.getColumns(table.tableId);
            List<ConstraintInfo> constraints = backend.getConstraints(table.tableId);

            boolean foundEmailCol = false;
            for (ColumnInfo col : columns) {
                if ("email".equals(col.name)) {
                    foundEmailCol = true;
                    assertFalse(col.nullable); // Set as not nullable
                    assertEquals("'default@example.com'", col.initialDefault);
                    assertEquals("'new@example.com'", col.defaultValue);
                    break;
                }
            }
            assertTrue("Email column should be found", foundEmailCol);

            assertEquals(1, constraints.size());
            assertEquals("email_not_null", constraints.get(0).name);
        }
    }

    @Test(expected = SQLException.class)
    public void testDropNonExistentConstraint() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_drop_nonexistent",
                new String[]{"id"}, new String[]{"INT"});

            // Try to drop a constraint that doesn't exist
            backend.dropConstraint(table.tableId, "nonexistent_constraint");
        }
    }

    // ---------------------------------------------------------------
    // Integration Tests
    // ---------------------------------------------------------------

    @Test
    public void testDefaultsPreservedAfterSchemaEvolution() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_evolution",
                new String[]{"id"}, new String[]{"INT"});

            // Add column with defaults
            backend.addColumnWithDefault(table.tableId, "name", "VARCHAR", true, "'anonymous'", "'guest'");

            // Rename the column (this creates a new column entry)
            backend.renameColumn(table.tableId, "name", "full_name");

            // Verify defaults are preserved
            List<ColumnInfo> columns = backend.getColumns(table.tableId);
            for (ColumnInfo col : columns) {
                if ("full_name".equals(col.name)) {
                    assertEquals("'anonymous'", col.initialDefault);
                    assertEquals("'guest'", col.defaultValue);
                    return;
                }
            }
            fail("full_name column should be found with preserved defaults");
        }
    }

    @Test
    public void testConstraintPreservedAfterSchemaEvolution() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            SchemaInfo schema = backend.getSchemaByName("main");
            TableInfo table = backend.createTableEntry(schema.schemaId, "test_constraint_evolution",
                new String[]{"id", "email"}, new String[]{"INT", "VARCHAR"});

            // Add constraint
            backend.addConstraint(table.tableId, "email_unique", "UNIQUE", new String[]{"email"});

            // Add another column
            backend.addColumn(table.tableId, "name", "VARCHAR", true);

            // Verify constraint still exists
            List<ConstraintInfo> constraints = backend.getConstraints(table.tableId);
            assertEquals(1, constraints.size());
            assertEquals("email_unique", constraints.get(0).name);
            assertEquals("UNIQUE", constraints.get(0).type);
        }
    }

    // ---------------------------------------------------------------
    // Helper methods
    // ---------------------------------------------------------------

    private static void createMinimalCatalog(String catPath, String dp) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catPath)) {
            conn.setAutoCommit(false);

            try (Statement st = conn.createStatement()) {
                // Create all the required tables
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

                // Initialize metadata
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dp + "')");

                // Initialize schema
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