package io.ducklake.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests for DuckLake Structured Streaming write support.
 * Validates micro-batch writes, checkpoint recovery, and snapshot creation.
 */
public class DuckLakeStreamingTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;
    private String sourceDir;
    private String checkpointDir;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeStreamingTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.streaming.schemaInference", "false")
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
        tempDir = Files.createTempDirectory("ducklake-streaming-test-").toString();
        dataPath = tempDir + "/data/";
        sourceDir = tempDir + "/source/";
        checkpointDir = tempDir + "/checkpoint/";
        new File(dataPath).mkdirs();
        new File(dataPath + "main/stream_table/").mkdirs();
        new File(sourceDir).mkdirs();
        new File(checkpointDir).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createCatalog(catalogPath, dataPath,
                "stream_table", "main/stream_table/",
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
    public void testBasicStreamingWrite() throws Exception {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, true)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write source data (JSONL format)
        writeJsonFile(sourceDir + "batch1.json",
                "{\"id\":1,\"name\":\"alice\",\"value\":10.5}",
                "{\"id\":2,\"name\":\"bob\",\"value\":20.3}",
                "{\"id\":3,\"name\":\"charlie\",\"value\":30.7}");

        Dataset<Row> streamDF = spark.readStream()
                .schema(schema)
                .json(sourceDir);

        StreamingQuery query = streamDF.writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();

        query.awaitTermination();

        // Read back via batch
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .load();

        assertEquals(3, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals(10.5, rows.get(0).getDouble(2), 0.001);
        assertEquals(3, rows.get(2).getInt(0));
        assertEquals("charlie", rows.get(2).getString(1));
    }

    @Test
    public void testMultipleMicroBatches() throws Exception {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, true)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // First micro-batch
        writeJsonFile(sourceDir + "batch1.json",
                "{\"id\":1,\"name\":\"alice\",\"value\":10.0}",
                "{\"id\":2,\"name\":\"bob\",\"value\":20.0}");

        StreamingQuery query = spark.readStream()
                .schema(schema)
                .json(sourceDir)
                .writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();
        query.awaitTermination();

        // Verify first batch
        Dataset<Row> result1 = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .load();
        assertEquals(2, result1.count());

        // Second micro-batch — add new file
        writeJsonFile(sourceDir + "batch2.json",
                "{\"id\":3,\"name\":\"charlie\",\"value\":30.0}",
                "{\"id\":4,\"name\":\"diana\",\"value\":40.0}");

        StreamingQuery query2 = spark.readStream()
                .schema(schema)
                .json(sourceDir)
                .writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();
        query2.awaitTermination();

        // Verify all data
        Dataset<Row> result2 = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .load();
        assertEquals(4, result2.count());

        List<Row> rows = result2.orderBy("id").collectAsList();
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(4, rows.get(3).getInt(0));
    }

    @Test
    public void testStreamingCreatesSnapshots() throws Exception {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, true)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Get initial snapshot count
        int initialSnapshots = getSnapshotCount();

        // First batch
        writeJsonFile(sourceDir + "batch1.json",
                "{\"id\":1,\"name\":\"test1\",\"value\":1.0}");

        StreamingQuery q1 = spark.readStream()
                .schema(schema).json(sourceDir)
                .writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();
        q1.awaitTermination();

        int afterFirst = getSnapshotCount();
        assertTrue("First micro-batch should create a new snapshot",
                afterFirst > initialSnapshots);

        // Second batch
        writeJsonFile(sourceDir + "batch2.json",
                "{\"id\":2,\"name\":\"test2\",\"value\":2.0}");

        StreamingQuery q2 = spark.readStream()
                .schema(schema).json(sourceDir)
                .writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();
        q2.awaitTermination();

        int afterSecond = getSnapshotCount();
        assertTrue("Second micro-batch should create another snapshot",
                afterSecond > afterFirst);

        // Verify snapshot changes mention streaming epoch
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT commit_message FROM ducklake_snapshot_changes ORDER BY snapshot_id DESC LIMIT 1")) {
            assertTrue(rs.next());
            assertTrue("Commit message should reference streaming epoch",
                    rs.getString("commit_message").contains("Streaming write epoch"));
        }
    }

    @Test
    public void testCheckpointRecovery() throws Exception {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, true)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write first batch
        writeJsonFile(sourceDir + "batch1.json",
                "{\"id\":1,\"name\":\"first\",\"value\":1.0}",
                "{\"id\":2,\"name\":\"second\",\"value\":2.0}");

        StreamingQuery q1 = spark.readStream()
                .schema(schema).json(sourceDir)
                .writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();
        q1.awaitTermination();

        // Verify checkpoint directory has content
        File cpDir = new File(checkpointDir);
        assertTrue("Checkpoint directory should exist", cpDir.exists());
        assertTrue("Checkpoint should have content",
                cpDir.listFiles() != null && cpDir.listFiles().length > 0);

        // Simulate restart: add new data and restart query with same checkpoint
        writeJsonFile(sourceDir + "batch2.json",
                "{\"id\":3,\"name\":\"third\",\"value\":3.0}");

        StreamingQuery q2 = spark.readStream()
                .schema(schema).json(sourceDir)
                .writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();
        q2.awaitTermination();

        // Should have 3 rows total — checkpoint prevents re-processing batch1
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .load();

        assertEquals("Checkpoint recovery should not duplicate data", 3, result.count());

        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals(3, rows.get(2).getInt(0));
    }

    @Test
    public void testStreamingTableStats() throws Exception {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, true)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write streaming data
        writeJsonFile(sourceDir + "batch1.json",
                "{\"id\":1,\"name\":\"alice\",\"value\":10.5}",
                "{\"id\":5,\"name\":\"eve\",\"value\":50.0}",
                "{\"id\":3,\"name\":null,\"value\":30.0}");

        StreamingQuery query = spark.readStream()
                .schema(schema).json(sourceDir)
                .coalesce(1)
                .writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();
        query.awaitTermination();

        // Verify table stats
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT record_count, next_row_id FROM ducklake_table_stats WHERE table_id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("Table stats should exist", rs.next());
                    assertEquals(3, rs.getLong("record_count"));
                    assertEquals(3, rs.getLong("next_row_id"));
                }
            }

            // Verify column stats for 'id' column
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT MIN(min_value) as min_v, MAX(max_value) as max_v, " +
                    "SUM(value_count) as vals FROM ducklake_file_column_stats " +
                    "WHERE table_id = 1 AND column_id = 0")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("1", rs.getString("min_v"));
                    assertEquals("5", rs.getString("max_v"));
                    assertEquals(3, rs.getLong("vals"));
                }
            }

            // Verify 'name' column has 1 null
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT SUM(null_count) as nulls FROM ducklake_file_column_stats " +
                    "WHERE table_id = 1 AND column_id = 1")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getLong("nulls"));
                }
            }
        }
    }

    @Test
    public void testStreamingAndBatchReadConsistency() throws Exception {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, true)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write some data via batch first
        List<Row> batchData = Arrays.asList(
                RowFactory.create(1, "batch_row", 100.0));
        spark.createDataFrame(batchData, schema)
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .mode(SaveMode.Append).save();

        // Now write via streaming
        writeJsonFile(sourceDir + "stream1.json",
                "{\"id\":2,\"name\":\"stream_row\",\"value\":200.0}");

        StreamingQuery query = spark.readStream()
                .schema(schema).json(sourceDir)
                .writeStream()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .option("checkpointLocation", checkpointDir)
                .outputMode("append")
                .trigger(Trigger.Once())
                .start();
        query.awaitTermination();

        // Both batch and streaming rows should be visible
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "stream_table")
                .load();

        assertEquals(2, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals("batch_row", rows.get(0).getString(1));
        assertEquals("stream_row", rows.get(1).getString(1));
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private void writeJsonFile(String path, String... lines) throws IOException {
        try (BufferedWriter w = new BufferedWriter(new FileWriter(path))) {
            for (String line : lines) {
                w.write(line);
                w.newLine();
            }
        }
    }

    private int getSnapshotCount() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ducklake_snapshot")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private void createCatalog(String catPath, String dp, String tableName, String tablePath,
                               String[] colNames, String[] colTypes, long[] colIds) throws Exception {
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

                long tableId = 1;
                long nextCatalogId = 2;
                st.execute("INSERT INTO ducklake_snapshot VALUES (1, datetime('now'), 1, " + nextCatalogId + ", 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (1, 'created_table:\"main\".\"" + tableName + "\"', NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_table VALUES (" + tableId + ", 'table-uuid-" + tableName + "', 1, NULL, 0, '" + tableName + "', '" + tablePath + "', 1)");

                for (int i = 0; i < colNames.length; i++) {
                    st.execute("INSERT INTO ducklake_column VALUES (" + colIds[i] + ", 1, NULL, " + tableId + ", " + i + ", '" + colNames[i] + "', '" + colTypes[i] + "', NULL, NULL, 1, NULL, NULL, NULL)");
                }

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
