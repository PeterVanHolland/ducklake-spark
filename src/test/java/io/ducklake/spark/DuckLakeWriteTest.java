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
 * Integration tests for DuckLake write support.
 * Sets up a SQLite-based DuckLake catalog and writes/reads data via Spark.
 */
public class DuckLakeWriteTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeWriteTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
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
        tempDir = Files.createTempDirectory("ducklake-write-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        new File(dataPath + "main/test_table/").mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createCatalog(catalogPath, dataPath,
                "test_table", "main/test_table/",
                new String[]{"id", "name", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2});
    }

    @After
    public void cleanup() throws Exception {
        deleteRecursive(new File(tempDir));
    }

    // ---------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------

    @Test
    public void testBasicAppendWrite() {
        // Write data
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "alice", 10.5),
                RowFactory.create(2, "bob", 20.3),
                RowFactory.create(3, "charlie", 30.7));

        Dataset<Row> df = spark.createDataFrame(data, writeSchema);
        df.write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Read back
        Dataset<Row> result = spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(3, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals(10.5, rows.get(0).getDouble(2), 0.001);
    }

    @Test
    public void testMultipleAppends() {
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // First append
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "alice", 10.0),
                RowFactory.create(2, "bob", 20.0));
        spark.createDataFrame(data1, writeSchema).write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Second append
        List<Row> data2 = Arrays.asList(
                RowFactory.create(3, "charlie", 30.0),
                RowFactory.create(4, "diana", 40.0));
        spark.createDataFrame(data2, writeSchema).write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Read back
        Dataset<Row> result = spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table").load();

        assertEquals(4, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(4, rows.get(3).getInt(0));
    }

    @Test
    public void testOverwriteMode() {
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // First write
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "alice", 10.0),
                RowFactory.create(2, "bob", 20.0));
        spark.createDataFrame(data1, writeSchema).write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Overwrite
        List<Row> data2 = Arrays.asList(
                RowFactory.create(100, "new_data", 99.9));
        spark.createDataFrame(data2, writeSchema).write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Overwrite).save();

        // Read back — should only see overwritten data
        Dataset<Row> result = spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table").load();

        assertEquals(1, result.count());
        Row row = result.collectAsList().get(0);
        assertEquals(100, row.getInt(0));
        assertEquals("new_data", row.getString(1));
    }

    @Test
    public void testTypeRoundTrip() throws Exception {
        // Create a table with various types
        String catPath2 = tempDir + "/types.ducklake";
        String dp2 = tempDir + "/types_data/";
        new File(dp2).mkdirs();
        new File(dp2 + "main/types_table/").mkdirs();

        createCatalog(catPath2, dp2, "types_table", "main/types_table/",
                new String[]{"bool_col", "byte_col", "short_col", "int_col", "long_col",
                             "float_col", "double_col", "string_col"},
                new String[]{"BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
                             "FLOAT", "DOUBLE", "VARCHAR"},
                new long[]{0, 1, 2, 3, 4, 5, 6, 7});

        StructType writeSchema = new StructType()
                .add("bool_col", DataTypes.BooleanType, true)
                .add("byte_col", DataTypes.ByteType, true)
                .add("short_col", DataTypes.ShortType, true)
                .add("int_col", DataTypes.IntegerType, true)
                .add("long_col", DataTypes.LongType, true)
                .add("float_col", DataTypes.FloatType, true)
                .add("double_col", DataTypes.DoubleType, true)
                .add("string_col", DataTypes.StringType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(true, (byte) 42, (short) 1000, 100000, 9999999999L,
                        3.14f, 2.718281828, "hello world"),
                RowFactory.create(false, (byte) -1, (short) -100, -42, -1L,
                        0.0f, -99.99, "test"));

        spark.createDataFrame(data, writeSchema).write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catPath2)
                .option("table", "types_table")
                .mode(SaveMode.Append).save();

        Dataset<Row> result = spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catPath2)
                .option("table", "types_table").load();

        assertEquals(2, result.count());
        List<Row> rows = result.orderBy("int_col").collectAsList();

        // Second row (int_col = -42)
        Row r0 = rows.get(0);
        assertFalse(r0.getBoolean(0));
        assertEquals(-1, r0.getByte(1));
        assertEquals(-100, r0.getShort(2));
        assertEquals(-42, r0.getInt(3));
        assertEquals(-1L, r0.getLong(4));
        assertEquals(0.0f, r0.getFloat(5), 0.001);
        assertEquals(-99.99, r0.getDouble(6), 0.001);
        assertEquals("test", r0.getString(7));

        // First row (int_col = 100000)
        Row r1 = rows.get(1);
        assertTrue(r1.getBoolean(0));
        assertEquals(42, r1.getByte(1));
        assertEquals(1000, r1.getShort(2));
        assertEquals(100000, r1.getInt(3));
        assertEquals(9999999999L, r1.getLong(4));
        assertEquals(3.14f, r1.getFloat(5), 0.01);
        assertEquals(2.718281828, r1.getDouble(6), 0.00001);
        assertEquals("hello world", r1.getString(7));
    }

    @Test
    public void testNullValues() {
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, null, 10.0),
                RowFactory.create(2, "bob", null),
                RowFactory.create(3, null, null));

        spark.createDataFrame(data, writeSchema).write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        Dataset<Row> result = spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table").load();

        assertEquals(3, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();
        assertNull(rows.get(0).getString(1));
        assertEquals(10.0, rows.get(0).getDouble(2), 0.001);
        assertEquals("bob", rows.get(1).getString(1));
        assertTrue(rows.get(1).isNullAt(2));
        assertNull(rows.get(2).getString(1));
        assertTrue(rows.get(2).isNullAt(2));
    }

    @Test
    public void testColumnStatsRecorded() throws Exception {
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "alice", 10.5),
                RowFactory.create(5, null, 50.0),
                RowFactory.create(3, "charlie", 30.0));

        spark.createDataFrame(data, writeSchema).coalesce(1).write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Verify stats in the catalog (single partition via coalesce)
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            // Check data file was created
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT SUM(record_count) as total FROM ducklake_data_file WHERE table_id = 1 AND end_snapshot IS NULL")) {
                assertTrue("Data file should exist", rs.next());
                assertEquals(3, rs.getLong("total"));
            }

            // Check column stats for 'id' column (column_id = 0) — aggregate across files
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT MIN(min_value) as min_v, MAX(max_value) as max_v, " +
                    "SUM(null_count) as nulls, SUM(value_count) as vals " +
                    "FROM ducklake_file_column_stats WHERE table_id = 1 AND column_id = 0")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("Stats for id column should exist", rs.next());
                    assertEquals("1", rs.getString("min_v"));
                    assertEquals("5", rs.getString("max_v"));
                    assertEquals(0, rs.getLong("nulls"));
                    assertEquals(3, rs.getLong("vals"));
                }
            }

            // Check column stats for 'name' column (column_id = 1) — has 1 null
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT SUM(null_count) as nulls, SUM(value_count) as vals " +
                    "FROM ducklake_file_column_stats WHERE table_id = 1 AND column_id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("Stats for name column should exist", rs.next());
                    assertEquals(1, rs.getLong("nulls"));
                    assertEquals(2, rs.getLong("vals"));
                }
            }

            // Check table stats updated
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT record_count, next_row_id FROM ducklake_table_stats WHERE table_id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("Table stats should exist", rs.next());
                    assertEquals(3, rs.getLong("record_count"));
                    assertEquals(3, rs.getLong("next_row_id"));
                }
            }
        }
    }

    @Test
    public void testSnapshotCreated() throws Exception {
        StructType writeSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        spark.createDataFrame(
                Arrays.asList(RowFactory.create(1, "test", 1.0)),
                writeSchema
        ).write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Verify new snapshot was created
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
                assertTrue(rs.next());
                assertTrue("New snapshot should be > 1", rs.getLong(1) >= 2);
            }

            // Verify snapshot changes recorded
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT changes_made FROM ducklake_snapshot_changes WHERE snapshot_id = " +
                     "(SELECT MAX(snapshot_id) FROM ducklake_snapshot)")) {
                assertTrue(rs.next());
                String changes = rs.getString("changes_made");
                assertTrue("Changes should reference the table",
                        changes.contains("inserted_into_table"));
            }
        }
    }

    // ---------------------------------------------------------------
    // Catalog setup helper
    // ---------------------------------------------------------------

    private void createCatalog(String catPath, String dp, String tableName, String tablePath,
                               String[] colNames, String[] colTypes, long[] colIds) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catPath)) {
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
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dp + "')");

                // Snapshot 0: create schema
                st.execute("INSERT INTO ducklake_snapshot VALUES (0, datetime('now'), 0, 1, 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)");

                // Schema
                st.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', 1)");

                // Snapshot 1: create table
                long tableId = 1;
                long nextCatalogId = 2;
                st.execute("INSERT INTO ducklake_snapshot VALUES (1, datetime('now'), 1, " + nextCatalogId + ", 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (1, 'created_table:\"main\".\"" + tableName + "\"', NULL, NULL, NULL)");

                // Table
                st.execute("INSERT INTO ducklake_table VALUES (" + tableId + ", 'table-uuid-" + tableName + "', 1, NULL, 0, '" + tableName + "', '" + tablePath + "', 1)");

                // Columns
                for (int i = 0; i < colNames.length; i++) {
                    st.execute("INSERT INTO ducklake_column VALUES (" + colIds[i] + ", 1, NULL, " + tableId + ", " + i + ", '" + colNames[i] + "', '" + colTypes[i] + "', NULL, NULL, 1, NULL, NULL, NULL)");
                }

                // Initialize table stats
                st.execute("INSERT INTO ducklake_table_stats VALUES (" + tableId + ", 0, 0, 0)");
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
