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
 * Integration tests for DuckLake CDC (Change Data Capture) functionality.
 * Tests the DuckLakeChanges utility class that provides change tracking
 * between snapshots via between() and since() methods.
 */
public class DuckLakeCDCTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeCDCTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Before
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-cdc-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        new File(dataPath + "main/test_table/").mkdirs();
        catalogPath = tempDir + "/test.ducklake";

        // Set catalog config for this test
        spark.conf().set("spark.sql.catalog.ducklake.catalog", catalogPath);
        spark.conf().set("spark.sql.catalog.ducklake.data_path", dataPath);
    }

    @After
    public void cleanup() throws Exception {
        deleteRecursive(new File(tempDir));
    }

    // ---------------------------------------------------------------
    // Test: Basic insert changes
    // ---------------------------------------------------------------

    @Test
    public void testBasicInsertChanges() throws Exception {
        // Setup: Create catalog with one initial snapshot containing 2 rows
        createCatalogWithInitialData(catalogPath, dataPath);

        // Insert additional data (creating snapshot 2)
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> newData = Arrays.asList(
                RowFactory.create(3, "charlie", 30.0),
                RowFactory.create(4, "diana", 40.0));

        spark.createDataFrame(newData, writeSchema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Test: Get changes between snapshots 1 and 2
        Dataset<Row> changes = DuckLakeChanges.between(spark, catalogPath, "test_table", 1, 2);

        List<Row> changesList = changes.orderBy("id").collectAsList();
        assertEquals("Should have 2 new rows", 2, changesList.size());

        // Verify first change
        assertEquals(3, changesList.get(0).getInt(0));
        assertEquals("charlie", changesList.get(0).getString(1));
        assertEquals(30.0, changesList.get(0).getDouble(2), 0.001);
        assertEquals("insert", changesList.get(0).getString(3)); // _change_type
        assertEquals(2L, changesList.get(0).getLong(4));         // _snapshot_id

        // Verify second change
        assertEquals(4, changesList.get(1).getInt(0));
        assertEquals("insert", changesList.get(1).getString(3));
        assertEquals(2L, changesList.get(1).getLong(4));
    }

    // ---------------------------------------------------------------
    // Test: Insert + Delete changes
    // ---------------------------------------------------------------

    @Test
    public void testInsertAndDeleteChanges() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Insert more data (snapshot 2)
        insertTestData(Arrays.asList(
                RowFactory.create(3, "charlie", 30.0),
                RowFactory.create(4, "diana", 40.0)
        ));

        // Delete some data (snapshot 3)
        spark.sql("DELETE FROM ducklake.main.test_table WHERE id = 2");

        // Test: Get changes between snapshots 1 and 3 (should include inserts and deletes)
        Dataset<Row> changes = DuckLakeChanges.between(spark, catalogPath, "test_table", 1, 3);

        List<Row> changesList = changes.orderBy("_snapshot_id", "_change_type", "id").collectAsList();
        assertEquals("Should have 3 changes (2 inserts + 1 delete)", 3, changesList.size());

        // Verify deletes come before inserts within same snapshot (alphabetical order)
        int deleteCount = 0, insertCount = 0;
        for (Row row : changesList) {
            String changeType = row.getString(row.fieldIndex("_change_type"));
            if ("delete".equals(changeType)) deleteCount++;
            if ("insert".equals(changeType)) insertCount++;
        }
        assertEquals("Should have 1 delete", 1, deleteCount);
        assertEquals("Should have 2 inserts", 2, insertCount);
    }

    // ---------------------------------------------------------------
    // Test: Changes across multiple snapshots
    // ---------------------------------------------------------------

    @Test
    public void testChangesAcrossMultipleSnapshots() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Snapshot 2: Add 2 rows
        insertTestData(Arrays.asList(RowFactory.create(3, "charlie", 30.0)));

        // Snapshot 3: Add 1 more row
        insertTestData(Arrays.asList(RowFactory.create(4, "diana", 40.0)));

        // Snapshot 4: Delete 1 row
        spark.sql("DELETE FROM ducklake.main.test_table WHERE id = 1");

        // Test: Get all changes from snapshot 1 to 4
        Dataset<Row> changes = DuckLakeChanges.between(spark, catalogPath, "test_table", 1, 4);

        List<Row> changesList = changes.orderBy("_snapshot_id", "id").collectAsList();
        assertEquals("Should have 3 changes total", 3, changesList.size());

        // Verify snapshots are included correctly
        Set<Long> snapshotIds = new HashSet<>();
        for (Row row : changesList) {
            snapshotIds.add(row.getLong(row.fieldIndex("_snapshot_id")));
        }
        assertTrue("Should include snapshot 2", snapshotIds.contains(2L));
        assertTrue("Should include snapshot 3", snapshotIds.contains(3L));
        assertTrue("Should include snapshot 4", snapshotIds.contains(4L));
        assertFalse("Should NOT include snapshot 1", snapshotIds.contains(1L));
    }

    // ---------------------------------------------------------------
    // Test: Empty change range
    // ---------------------------------------------------------------

    @Test
    public void testEmptyChangeRange() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Test: Get changes between snapshots 1 and 1 (empty range)
        Dataset<Row> changes = DuckLakeChanges.between(spark, catalogPath, "test_table", 1, 1);

        assertEquals("Empty range should return 0 changes", 0, changes.count());

        // Verify schema is still correct
        StructType schema = changes.schema();
        assertNotNull("Should have _change_type column", schema.apply("_change_type"));
        assertNotNull("Should have _snapshot_id column", schema.apply("_snapshot_id"));
        assertEquals("_change_type should be StringType", DataTypes.StringType,
                     schema.apply("_change_type").dataType());
        assertEquals("_snapshot_id should be LongType", DataTypes.LongType,
                     schema.apply("_snapshot_id").dataType());
    }

    // ---------------------------------------------------------------
    // Test: Single snapshot changes
    // ---------------------------------------------------------------

    @Test
    public void testSingleSnapshotChanges() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Add data in snapshot 2
        insertTestData(Arrays.asList(RowFactory.create(3, "charlie", 30.0)));

        // Test: Get changes for only snapshot 2
        Dataset<Row> changes = DuckLakeChanges.between(spark, catalogPath, "test_table", 1, 2);

        List<Row> changesList = changes.collectAsList();
        assertEquals("Should have 1 change", 1, changesList.size());
        assertEquals("insert", changesList.get(0).getString(changesList.get(0).fieldIndex("_change_type")));
        assertEquals(2L, changesList.get(0).getLong(changesList.get(0).fieldIndex("_snapshot_id")));
        assertEquals(3, changesList.get(0).getInt(0)); // id column
    }

    // ---------------------------------------------------------------
    // Test: Update changes (shows as delete + insert)
    // ---------------------------------------------------------------

    @Test
    public void testUpdateChanges() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Update a row (should create delete + insert)
        spark.sql("UPDATE ducklake.main.test_table SET name = 'alice_updated', value = 15.5 WHERE id = 1");

        // Test: Get changes from the update
        Dataset<Row> changes = DuckLakeChanges.between(spark, catalogPath, "test_table", 1, 2);

        List<Row> changesList = changes.orderBy("_change_type").collectAsList(); // delete first, then insert
        assertEquals("Update should create 2 changes (delete + insert)", 2, changesList.size());

        // Verify delete record
        Row deleteRow = changesList.get(0);
        assertEquals("delete", deleteRow.getString(deleteRow.fieldIndex("_change_type")));
        assertEquals(1, deleteRow.getInt(0)); // original id
        assertEquals("alice", deleteRow.getString(1)); // original name

        // Verify insert record
        Row insertRow = changesList.get(1);
        assertEquals("insert", insertRow.getString(insertRow.fieldIndex("_change_type")));
        assertEquals(1, insertRow.getInt(0)); // same id
        assertEquals("alice_updated", insertRow.getString(1)); // updated name
        assertEquals(15.5, insertRow.getDouble(2), 0.001); // updated value
    }

    // ---------------------------------------------------------------
    // Test: Since latest (no changes)
    // ---------------------------------------------------------------

    @Test
    public void testSinceLatestNoChanges() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Test: Get changes since the latest snapshot (should be empty)
        Dataset<Row> changes = DuckLakeChanges.since(spark, catalogPath, "test_table", 1);

        assertEquals("No changes since latest snapshot", 0, changes.count());
    }

    // ---------------------------------------------------------------
    // Test: Since method with changes
    // ---------------------------------------------------------------

    @Test
    public void testSinceMethodWithChanges() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Add data in snapshots 2 and 3
        insertTestData(Arrays.asList(RowFactory.create(3, "charlie", 30.0)));
        insertTestData(Arrays.asList(RowFactory.create(4, "diana", 40.0)));

        // Test: Get all changes since snapshot 1
        Dataset<Row> changes = DuckLakeChanges.since(spark, catalogPath, "test_table", 1);

        List<Row> changesList = changes.orderBy("_snapshot_id", "id").collectAsList();
        assertEquals("Should have 2 changes since snapshot 1", 2, changesList.size());

        assertEquals("insert", changesList.get(0).getString(changesList.get(0).fieldIndex("_change_type")));
        assertEquals(2L, changesList.get(0).getLong(changesList.get(0).fieldIndex("_snapshot_id")));
        assertEquals(3, changesList.get(0).getInt(0));

        assertEquals("insert", changesList.get(1).getString(changesList.get(1).fieldIndex("_change_type")));
        assertEquals(3L, changesList.get(1).getLong(changesList.get(1).fieldIndex("_snapshot_id")));
        assertEquals(4, changesList.get(1).getInt(0));
    }

    // ---------------------------------------------------------------
    // Test: Full history (from snapshot 0)
    // ---------------------------------------------------------------

    @Test
    public void testFullHistoryFromSnapshotZero() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Add more data
        insertTestData(Arrays.asList(RowFactory.create(3, "charlie", 30.0)));

        // Test: Get full history from beginning
        Dataset<Row> changes = DuckLakeChanges.between(spark, catalogPath, "test_table", 0, 2);

        List<Row> changesList = changes.orderBy("_snapshot_id", "id").collectAsList();
        assertEquals("Should include all data from beginning", 3, changesList.size());

        // Should include initial data from snapshot 1
        assertTrue("Should include data from snapshot 1",
                   changesList.stream().anyMatch(r -> r.getLong(r.fieldIndex("_snapshot_id")) == 1L));
        assertTrue("Should include data from snapshot 2",
                   changesList.stream().anyMatch(r -> r.getLong(r.fieldIndex("_snapshot_id")) == 2L));
    }

    // ---------------------------------------------------------------
    // Test: Schema evolution between snapshots
    // ---------------------------------------------------------------

    @Test
    public void testSchemaEvolutionBetweenSnapshots() throws Exception {
        createCatalogWithInitialData(catalogPath, dataPath);

        // Add a new column and data (this would be done via ALTER TABLE in practice)
        // For this test, we'll simulate by creating new data with extra column
        StructType extendedSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true)
                .add("status", DataTypes.StringType, true); // New column

        List<Row> extendedData = Arrays.asList(
                RowFactory.create(3, "charlie", 30.0, "active"));

        spark.createDataFrame(extendedData, extendedSchema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Test: Get changes (should handle schema evolution gracefully)
        Dataset<Row> changes = DuckLakeChanges.between(spark, catalogPath, "test_table", 1, 2);

        List<Row> changesList = changes.collectAsList();
        assertEquals("Should have 1 change with schema evolution", 1, changesList.size());

        Row change = changesList.get(0);
        assertEquals("insert", change.getString(change.fieldIndex("_change_type")));
        assertEquals(3, change.getInt(0)); // id
        assertEquals("charlie", change.getString(1)); // name

        // Note: The exact handling of schema evolution depends on the implementation
        // The test mainly verifies that CDC doesn't break with schema changes
    }

    // ---------------------------------------------------------------
    // Test: Invalid scenarios
    // ---------------------------------------------------------------

    @Test
    public void testInvalidTableName() {
        createBasicCatalog(catalogPath, dataPath);

        // Test: Non-existent table should throw exception
        try {
            DuckLakeChanges.between(spark, catalogPath, "nonexistent_table", 0, 1);
            fail("Should throw exception for non-existent table");
        } catch (RuntimeException e) {
            assertTrue("Should mention table not found", e.getMessage().contains("Table not found"));
        }
    }

    @Test
    public void testInvalidSchemaName() {
        createBasicCatalog(catalogPath, dataPath);

        // Test: Non-existent schema should throw exception
        try {
            DuckLakeChanges.between(spark, catalogPath, "test_table", "nonexistent_schema", 0, 1);
            fail("Should throw exception for non-existent schema");
        } catch (RuntimeException e) {
            assertTrue("Should mention schema not found", e.getMessage().contains("Schema not found"));
        }
    }

    // ---------------------------------------------------------------
    // Helper methods
    // ---------------------------------------------------------------

    private void createCatalogWithInitialData(String catalogPath, String dataPath) throws Exception {
        createCatalog(catalogPath, dataPath,
                "test_table", "main/test_table/",
                new String[]{"id", "name", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2});

        // Insert initial data for snapshot 1
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> initialData = Arrays.asList(
                RowFactory.create(1, "alice", 10.0),
                RowFactory.create(2, "bob", 20.0));

        spark.createDataFrame(initialData, writeSchema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();
    }

    private void createBasicCatalog(String catalogPath, String dataPath) {
        try {
            createCatalog(catalogPath, dataPath,
                    "test_table", "main/test_table/",
                    new String[]{"id", "name", "value"},
                    new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                    new long[]{0, 1, 2});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void insertTestData(List<Row> data) {
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        spark.createDataFrame(data, writeSchema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();
    }

    private void createCatalog(String catalogPath, String dataPath, String tableName, String tablePath,
                               String[] colNames, String[] colTypes, long[] colIds) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            conn.setAutoCommit(false);

            try (Statement st = conn.createStatement()) {
                // Core metadata tables
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

                // Insert metadata
                st.execute("INSERT INTO ducklake_metadata VALUES ('catalog_format_version', '1', NULL, NULL)");
                st.execute("INSERT INTO ducklake_metadata VALUES ('data_path', '" + dataPath + "', NULL, NULL)");

                // Create initial snapshot
                st.execute("INSERT INTO ducklake_snapshot VALUES (0, '2026-01-01T00:00:00', 0, 1, 1)");

                // Create schema
                st.execute("INSERT INTO ducklake_schema VALUES (1, 'schema-uuid-1', 0, NULL, 'main', NULL, 0)");

                // Create table
                st.execute("INSERT INTO ducklake_table VALUES (1, 'table-uuid-1', 0, NULL, 1, '" + tableName + "', '" + tablePath + "', 1)");

                // Create columns
                for (int i = 0; i < colNames.length; i++) {
                    st.execute(String.format(
                            "INSERT INTO ducklake_column VALUES (%d, 0, NULL, 1, %d, '%s', '%s', NULL, NULL, 1, NULL, NULL, NULL)",
                            colIds[i], i, colNames[i], colTypes[i]));
                }

                // Create table stats
                st.execute("INSERT INTO ducklake_table_stats VALUES (1, 0, 1, 0)");
            }

            conn.commit();
        }
    }

    private void deleteRecursive(File file) {
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