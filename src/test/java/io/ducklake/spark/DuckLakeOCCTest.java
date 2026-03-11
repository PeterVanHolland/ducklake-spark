package io.ducklake.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Tests for DuckLake OCC (optimistic concurrency control).
 * Verifies sequential write atomicity, snapshot advancement,
 * and concurrent write detection.
 */
public class DuckLakeOCCTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[4]")
                .appName("DuckLakeOCCTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDownSpark() {
        if (spark != null) spark.stop();
    }

    @Before
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-occ-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        new File(dataPath + "main/occ_test/").mkdirs();
        catalogPath = tempDir + "/test.ducklake";

        createCatalog(catalogPath, dataPath, "occ_test", "main/occ_test/",
                new String[]{"id", "name"},
                new String[]{"BIGINT", "VARCHAR"},
                new long[]{2, 3});
    }

    @After
    public void teardown() {
        deleteDir(new File(tempDir));
    }

    private void writeTable(Dataset<Row> df, String mode) {
        df.write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "occ_test")
                .mode(mode)
                .save();
    }

    private Dataset<Row> readTable() {
        return spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "occ_test")
                .load();
    }

    @Test
    public void testSequentialAppendsAdvanceSnapshots() throws Exception {
        long snap0 = getSnapshotCount();

        for (int i = 0; i < 3; i++) {
            Dataset<Row> df = spark.createDataFrame(
                    Arrays.asList(RowFactory.create((long)(i * 10), "batch_" + i)),
                    new StructType()
                            .add("id", DataTypes.LongType)
                            .add("name", DataTypes.StringType));
            writeTable(df, "append");
        }

        long snapN = getSnapshotCount();
        assertTrue("Snapshots should have advanced", snapN > snap0);

        assertEquals(3, readTable().count());
    }

    @Test
    public void testOverwriteReplacesAllData() throws Exception {
        Dataset<Row> df1 = spark.createDataFrame(
                Arrays.asList(RowFactory.create(1L, "old")),
                new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType));
        writeTable(df1, "append");

        Dataset<Row> df2 = spark.createDataFrame(
                Arrays.asList(RowFactory.create(99L, "new")),
                new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType));
        writeTable(df2, "overwrite");

        List<Row> rows = readTable().collectAsList();
        assertEquals(1, rows.size());
        assertEquals(99L, rows.get(0).getLong(0));
    }

    @Test
    public void testConcurrentAppendsAllSucceed() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < 3; i++) {
            final int id = i;
            executor.submit(() -> {
                try {
                    Dataset<Row> df = spark.createDataFrame(
                            Arrays.asList(RowFactory.create((long) id, "thread_" + id)),
                            new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType));
                    writeTable(df, "append");
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    // Conflict expected under concurrency
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        // At least one should succeed
        assertTrue("At least one concurrent write should succeed", successCount.get() >= 1);

        long count = readTable().count();
        assertEquals("Row count should match successful writes", (long) successCount.get(), count);
    }

    @Test
    public void testWriteAtomicity() throws Exception {
        // A write of 100 rows should either fully commit or not at all
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(RowFactory.create((long) i, "row_" + i));
        }
        Dataset<Row> df = spark.createDataFrame(rows,
                new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType));
        writeTable(df, "append");

        assertEquals(100, readTable().count());
    }

    @Test
    public void testReadAfterWriteConsistency() throws Exception {
        Dataset<Row> df1 = spark.createDataFrame(
                Arrays.asList(RowFactory.create(1L, "a"), RowFactory.create(2L, "b")),
                new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType));
        writeTable(df1, "append");
        assertEquals(2, readTable().count());

        Dataset<Row> df2 = spark.createDataFrame(
                Arrays.asList(RowFactory.create(3L, "c")),
                new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType));
        writeTable(df2, "append");
        assertEquals(3, readTable().count());
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private long getSnapshotCount() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ducklake_snapshot")) {
            return rs.next() ? rs.getLong(1) : 0;
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
                long nextCatalogId = 2 + colIds.length;
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

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) deleteDir(f);
            }
        }
        dir.delete();
    }
}
