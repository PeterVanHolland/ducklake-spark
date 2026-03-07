package io.ducklake.spark;

import io.ducklake.spark.maintenance.DuckLakeMaintenance;
import io.ducklake.spark.writer.DuckLakeUpdateExecutor;

import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.*;
import org.junit.*;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Cross-framework integration tests for DuckLake.
 *
 * <p>Verifies interoperability between the Spark DuckLake connector and DuckDB
 * (via JDBC + the ducklake extension), type fidelity across all supported types,
 * concurrent read access, large-dataset handling, and end-to-end lakehouse
 * workflows including time travel.
 *
 * <p>Tests that require DuckDB's ducklake extension are skipped gracefully when
 * the extension cannot be installed (e.g., no network or unsupported platform).
 */
public class DuckLakeIntegrationTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    /** Lazily evaluated: whether DuckDB ducklake extension can be installed. */
    private static Boolean duckLakeAvailable;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-integration-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);

        Thread.currentThread().setContextClassLoader(DuckLakeIntegrationTest.class.getClassLoader());

        spark = SparkSession.builder()
                .master("local[4]")
                .appName("DuckLakeIntegrationTest")
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
    // DuckDB JDBC helpers
    // ---------------------------------------------------------------

    /**
     * Check if DuckDB's ducklake extension can be installed and loaded.
     * Result is cached for the lifetime of the test class.
     */
    private static synchronized boolean isDuckLakeAvailable() {
        if (duckLakeAvailable != null) return duckLakeAvailable;
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
                conn.createStatement().execute("INSTALL ducklake; LOAD ducklake;");
                duckLakeAvailable = true;
            }
        } catch (Exception e) {
            System.err.println("DuckDB ducklake not available: " + e.getMessage());
            duckLakeAvailable = false;
        }
        return duckLakeAvailable;
    }

    /** Get a fresh DuckDB JDBC connection with ducklake loaded. */
    private Connection newDuckDBConnection() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        conn.createStatement().execute("LOAD ducklake;");
        return conn;
    }

    // ---------------------------------------------------------------
    // Spark SQL helpers
    // ---------------------------------------------------------------

    private void insertRows(String tableName, int startId, int endId) {
        StringBuilder sb = new StringBuilder();
        for (int i = startId; i <= endId; i++) {
            if (i > startId) sb.append(", ");
            sb.append("(").append(i).append(", 'name_").append(i)
              .append("', ").append(i * 10.0).append(")");
        }
        spark.sql("INSERT INTO ducklake.main." + tableName + " VALUES " + sb);
    }

    private long getLatestSnapshotId() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    // ===============================================================
    // 1. Spark write → DuckDB read
    // ===============================================================

    @Ignore("DuckDB ducklake extension supports catalog versions 0.1-0.3 while the Spark connector creates v0.4. Enable when version alignment is achieved.")
    @Test
    public void testSparkWriteDuckDBRead() throws Exception {
        Assume.assumeTrue("DuckDB ducklake extension not available", isDuckLakeAvailable());

        spark.sql("CREATE TABLE ducklake.main.spark_to_duck (id INT, name STRING, value DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.spark_to_duck VALUES " +
                "(1, 'alice', 10.5), (2, 'bob', 20.3), (3, 'charlie', 30.7)");

        try (Connection conn = newDuckDBConnection()) {
            conn.createStatement().execute(
                    "ATTACH '" + catalogPath + "' AS dl (TYPE ducklake)");

            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT id, name, value FROM dl.main.spark_to_duck ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("alice", rs.getString("name"));
                assertEquals(10.5, rs.getDouble("value"), 0.001);

                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("bob", rs.getString("name"));
                assertEquals(20.3, rs.getDouble("value"), 0.001);

                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertEquals("charlie", rs.getString("name"));
                assertEquals(30.7, rs.getDouble("value"), 0.001);

                assertFalse("Should have exactly 3 rows", rs.next());
            }
        }
    }

    @Ignore("DuckDB ducklake extension supports catalog versions 0.1-0.3 while the Spark connector creates v0.4. Enable when version alignment is achieved.")
    @Test
    public void testSparkWriteDuckDBReadMultipleBatches() throws Exception {
        Assume.assumeTrue("DuckDB ducklake extension not available", isDuckLakeAvailable());

        spark.sql("CREATE TABLE ducklake.main.multi_batch (id INT, name STRING, value DOUBLE)");
        insertRows("multi_batch", 1, 5);
        insertRows("multi_batch", 6, 10);
        insertRows("multi_batch", 11, 15);

        try (Connection conn = newDuckDBConnection()) {
            conn.createStatement().execute(
                    "ATTACH '" + catalogPath + "' AS dl (TYPE ducklake)");

            // Verify total count
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT COUNT(*) as cnt FROM dl.main.multi_batch")) {
                assertTrue(rs.next());
                assertEquals(15, rs.getInt("cnt"));
            }

            // Verify ordering and values from different batches
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT id, name FROM dl.main.multi_batch ORDER BY id")) {
                for (int i = 1; i <= 15; i++) {
                    assertTrue("Row " + i + " should exist", rs.next());
                    assertEquals(i, rs.getInt("id"));
                    assertEquals("name_" + i, rs.getString("name"));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Ignore("DuckDB ducklake extension supports catalog versions 0.1-0.3 while the Spark connector creates v0.4. Enable when version alignment is achieved.")
    @Test
    public void testSparkWriteDuckDBReadWithNulls() throws Exception {
        Assume.assumeTrue("DuckDB ducklake extension not available", isDuckLakeAvailable());

        spark.sql("CREATE TABLE ducklake.main.nulls_cross (id INT, name STRING, value DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.nulls_cross VALUES " +
                "(1, null, 10.0), (2, 'bob', null), (3, null, null)");

        try (Connection conn = newDuckDBConnection()) {
            conn.createStatement().execute(
                    "ATTACH '" + catalogPath + "' AS dl (TYPE ducklake)");

            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT id, name, value FROM dl.main.nulls_cross ORDER BY id")) {
                // Row 1: name is null
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertNull(rs.getString("name"));
                assertEquals(10.0, rs.getDouble("value"), 0.001);

                // Row 2: value is null
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("bob", rs.getString("name"));
                rs.getDouble("value");
                assertTrue("value should be null", rs.wasNull());

                // Row 3: both null
                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertNull(rs.getString("name"));
                rs.getDouble("value");
                assertTrue("value should be null", rs.wasNull());

                assertFalse(rs.next());
            }
        }
    }

    // ===============================================================
    // 2. DuckDB write → Spark read
    // ===============================================================

    @Ignore("DuckDB creates catalogs in DuckDB storage format; Spark connector expects SQLite backend. Enable when a common metadata backend is supported.")
    @Test
    public void testDuckDBWriteSparkRead() throws Exception {
        Assume.assumeTrue("DuckDB ducklake extension not available", isDuckLakeAvailable());

        String duckCatPath = tempDir + "/duckdb_catalog.ducklake";
        String duckDataPath = tempDir + "/duckdb_data/";
        new File(duckDataPath).mkdirs();

        // Write via DuckDB
        try (Connection conn = newDuckDBConnection()) {
            conn.createStatement().execute(
                    "ATTACH '" + duckCatPath + "' AS dl (TYPE ducklake, DATA_PATH '" + duckDataPath + "')");
            conn.createStatement().execute(
                    "CREATE TABLE dl.main.from_duck (id INTEGER, name VARCHAR, score DOUBLE)");
            conn.createStatement().execute(
                    "INSERT INTO dl.main.from_duck VALUES " +
                    "(1, 'alpha', 91.5), (2, 'beta', 85.0), (3, 'gamma', 77.3), (4, 'delta', 93.8)");
        }

        // Read via Spark (DataSource API — independent of catalog config)
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", duckCatPath)
                .option("table", "from_duck")
                .load();

        assertEquals(4, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();

        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alpha", rows.get(0).getString(1));
        assertEquals(91.5, rows.get(0).getDouble(2), 0.001);

        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("beta", rows.get(1).getString(1));
        assertEquals(85.0, rows.get(1).getDouble(2), 0.001);

        assertEquals(3, rows.get(2).getInt(0));
        assertEquals("gamma", rows.get(2).getString(1));
        assertEquals(77.3, rows.get(2).getDouble(2), 0.001);

        assertEquals(4, rows.get(3).getInt(0));
        assertEquals("delta", rows.get(3).getString(1));
        assertEquals(93.8, rows.get(3).getDouble(2), 0.001);
    }

    // ===============================================================
    // 3. Type round-trip (Spark write → Spark read)
    // ===============================================================

    @Test
    public void testTypeRoundTripPrimitives() {
        spark.sql("CREATE TABLE ducklake.main.types_prim (" +
                "int_col INT, bigint_col BIGINT, float_col FLOAT, double_col DOUBLE, " +
                "bool_col BOOLEAN, str_col STRING)");

        spark.sql("INSERT INTO ducklake.main.types_prim VALUES " +
                "(42, 9999999999, cast(3.14 as float), 2.718281828, true, 'hello world')");
        spark.sql("INSERT INTO ducklake.main.types_prim VALUES " +
                "(-1, -9999999999, cast(-0.5 as float), -99.99, false, '')");

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.types_prim ORDER BY int_col");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());

        // Row 1: negative / edge values
        Row r0 = rows.get(0);
        assertEquals(-1, r0.getInt(0));
        assertEquals(-9999999999L, r0.getLong(1));
        assertEquals(-0.5f, r0.getFloat(2), 0.01);
        assertEquals(-99.99, r0.getDouble(3), 0.001);
        assertFalse(r0.getBoolean(4));
        assertEquals("", r0.getString(5));

        // Row 2: positive values
        Row r1 = rows.get(1);
        assertEquals(42, r1.getInt(0));
        assertEquals(9999999999L, r1.getLong(1));
        assertEquals(3.14f, r1.getFloat(2), 0.01);
        assertEquals(2.718281828, r1.getDouble(3), 0.00001);
        assertTrue(r1.getBoolean(4));
        assertEquals("hello world", r1.getString(5));
    }

    @Test
    public void testTypeRoundTripDecimalDateTimestampBinary() {
        spark.sql("CREATE TABLE ducklake.main.types_ext (" +
                "id INT, dec_col DECIMAL(10,2), date_col DATE, " +
                "ts_col TIMESTAMP, bin_col BINARY)");

        spark.sql("INSERT INTO ducklake.main.types_ext VALUES (" +
                "1, " +
                "cast(12345.67 as decimal(10,2)), " +
                "DATE '2024-06-15', " +
                "TIMESTAMP '2024-06-15 14:30:00', " +
                "unhex('DEADBEEF'))");

        spark.sql("INSERT INTO ducklake.main.types_ext VALUES (" +
                "2, " +
                "cast(-99.99 as decimal(10,2)), " +
                "DATE '2000-01-01', " +
                "TIMESTAMP '2000-01-01 00:00:00', " +
                "unhex('00FF'))");

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.types_ext ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());

        Row r0 = rows.get(0);
        assertEquals(1, r0.getInt(0));
        assertEquals(new BigDecimal("12345.67"), r0.getDecimal(1));
        assertEquals(java.sql.Date.valueOf("2024-06-15"), r0.getDate(2));
        assertEquals(java.sql.Timestamp.valueOf("2024-06-15 14:30:00"), r0.getTimestamp(3));
        assertArrayEquals(
                new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF},
                (byte[]) r0.get(4));

        Row r1 = rows.get(1);
        assertEquals(2, r1.getInt(0));
        assertEquals(new BigDecimal("-99.99"), r1.getDecimal(1));
        assertEquals(java.sql.Date.valueOf("2000-01-01"), r1.getDate(2));
        assertEquals(java.sql.Timestamp.valueOf("2000-01-01 00:00:00"), r1.getTimestamp(3));
        assertArrayEquals(new byte[]{(byte) 0x00, (byte) 0xFF}, (byte[]) r1.get(4));
    }

    @Test
    public void testTypeRoundTripComplexTypes() {
        spark.sql("CREATE TABLE ducklake.main.types_complex (" +
                "id INT, " +
                "int_arr ARRAY<INT>, " +
                "str_struct STRUCT<name: STRING, age: INT>, " +
                "str_map MAP<STRING, INT>)");

        spark.sql("INSERT INTO ducklake.main.types_complex VALUES (" +
                "1, " +
                "array(10, 20, 30), " +
                "named_struct('name', 'alice', 'age', 30), " +
                "map('a', 1, 'b', 2))");

        // Verify array elements via SQL (avoids Scala interop)
        Dataset<Row> arrResult = spark.sql(
                "SELECT int_arr[0], int_arr[1], int_arr[2], size(int_arr) " +
                "FROM ducklake.main.types_complex");
        Row arrRow = arrResult.collectAsList().get(0);
        assertEquals(10, arrRow.getInt(0));
        assertEquals(20, arrRow.getInt(1));
        assertEquals(30, arrRow.getInt(2));
        assertEquals(3, arrRow.getInt(3));

        // Verify struct fields
        Dataset<Row> structResult = spark.sql(
                "SELECT str_struct.name, str_struct.age " +
                "FROM ducklake.main.types_complex");
        Row structRow = structResult.collectAsList().get(0);
        assertEquals("alice", structRow.getString(0));
        assertEquals(30, structRow.getInt(1));

        // Verify map entries
        Dataset<Row> mapResult = spark.sql(
                "SELECT str_map['a'], str_map['b'] " +
                "FROM ducklake.main.types_complex");
        Row mapRow = mapResult.collectAsList().get(0);
        assertEquals(1, mapRow.getInt(0));
        assertEquals(2, mapRow.getInt(1));
    }

    // ===============================================================

    @Test
    public void testArrayWithNulls() {
        spark.sql("CREATE TABLE ducklake.main.arr_nulls (id INT, vals ARRAY<STRING>)");

        spark.sql("INSERT INTO ducklake.main.arr_nulls VALUES " +
                "(1, array('hello', null, 'world')), " +
                "(2, array(null, null)), " +
                "(3, null)");

        Dataset<Row> result = spark.sql(
                "SELECT id, vals[0], vals[1], vals[2], size(vals) " +
                "FROM ducklake.main.arr_nulls ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        // Row 1: ['hello', null, 'world']
        assertEquals("hello", rows.get(0).getString(1));
        assertTrue(rows.get(0).isNullAt(2));
        assertEquals("world", rows.get(0).getString(3));
        assertEquals(3, rows.get(0).getInt(4));

        // Row 2: [null, null]
        assertTrue(rows.get(1).isNullAt(1));
        assertTrue(rows.get(1).isNullAt(2));
        assertEquals(2, rows.get(1).getInt(4));

        // Row 3: null array -> size returns -1
        assertEquals(-1, rows.get(2).getInt(4));
    }

    @Test
    public void testStructWithMultipleFields() {
        spark.sql("CREATE TABLE ducklake.main.struct_multi (" +
                "id INT, " +
                "info STRUCT<name: STRING, score: DOUBLE, active: BOOLEAN>)");

        spark.sql("INSERT INTO ducklake.main.struct_multi VALUES " +
                "(1, named_struct('name', 'alice', 'score', 95.5, 'active', true)), " +
                "(2, named_struct('name', 'bob', 'score', 82.3, 'active', false)), " +
                "(3, null)");

        Dataset<Row> result = spark.sql(
                "SELECT id, info.name, info.score, info.active " +
                "FROM ducklake.main.struct_multi ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        assertEquals("alice", rows.get(0).getString(1));
        assertEquals(95.5, rows.get(0).getDouble(2), 0.001);
        assertTrue(rows.get(0).getBoolean(3));

        assertEquals("bob", rows.get(1).getString(1));
        assertEquals(82.3, rows.get(1).getDouble(2), 0.001);
        assertFalse(rows.get(1).getBoolean(3));

        // Row 3: null struct
        assertTrue(rows.get(2).isNullAt(1));
        assertTrue(rows.get(2).isNullAt(2));
        assertTrue(rows.get(2).isNullAt(3));
    }

    @Test
    public void testMapWithVariousTypes() {
        spark.sql("CREATE TABLE ducklake.main.map_types (" +
                "id INT, " +
                "scores MAP<STRING, DOUBLE>)");

        spark.sql("INSERT INTO ducklake.main.map_types VALUES " +
                "(1, map('math', 95.0, 'english', 88.5)), " +
                "(2, map('science', 72.0)), " +
                "(3, null)");

        Dataset<Row> result = spark.sql(
                "SELECT id, scores['math'], scores['english'], scores['science'] " +
                "FROM ducklake.main.map_types ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        assertEquals(95.0, rows.get(0).getDouble(1), 0.001);
        assertEquals(88.5, rows.get(0).getDouble(2), 0.001);
        assertTrue(rows.get(0).isNullAt(3)); // 'science' not in row 1

        assertTrue(rows.get(1).isNullAt(1)); // 'math' not in row 2
        assertTrue(rows.get(1).isNullAt(2)); // 'english' not in row 2
        assertEquals(72.0, rows.get(1).getDouble(3), 0.001);

        // Row 3: null map
        assertTrue(rows.get(2).isNullAt(1));
    }

    @Test
    public void testNestedComplexTypes() {
        // Array of structs
        spark.sql("CREATE TABLE ducklake.main.nested_complex (" +
                "id INT, " +
                "people ARRAY<STRUCT<name: STRING, age: INT>>)");

        spark.sql("INSERT INTO ducklake.main.nested_complex VALUES " +
                "(1, array(named_struct('name', 'alice', 'age', 30), " +
                "named_struct('name', 'bob', 'age', 25)))");

        Dataset<Row> result = spark.sql(
                "SELECT people[0].name, people[0].age, people[1].name, people[1].age " +
                "FROM ducklake.main.nested_complex");
        Row row = result.collectAsList().get(0);
        assertEquals("alice", row.getString(0));
        assertEquals(30, row.getInt(1));
        assertEquals("bob", row.getString(2));
        assertEquals(25, row.getInt(3));
    }

    @Test
    public void testMultipleRowsComplexTypes() {
        spark.sql("CREATE TABLE ducklake.main.multi_complex (" +
                "id INT, " +
                "tags ARRAY<STRING>, " +
                "meta MAP<STRING, INT>)");

        // Insert multiple rows with varying sizes
        spark.sql("INSERT INTO ducklake.main.multi_complex VALUES " +
                "(1, array('a', 'b', 'c'), map('x', 1, 'y', 2)), " +
                "(2, array('d'), map('z', 3)), " +
                "(3, array(), map())");

        Dataset<Row> result = spark.sql(
                "SELECT id, size(tags), size(meta) " +
                "FROM ducklake.main.multi_complex ORDER BY id");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        assertEquals(3, rows.get(0).getInt(1));
        assertEquals(2, rows.get(0).getInt(2));

        assertEquals(1, rows.get(1).getInt(1));
        assertEquals(1, rows.get(1).getInt(2));

        assertEquals(0, rows.get(2).getInt(1));
        assertEquals(0, rows.get(2).getInt(2));
    }

    // 4. Concurrent read access
    // ===============================================================

    @Test
    public void testConcurrentReads() throws Exception {
        spark.sql("CREATE TABLE ducklake.main.concurrent_tbl (id INT, name STRING, value DOUBLE)");

        // Insert 100 rows
        StringBuilder values = new StringBuilder();
        for (int i = 1; i <= 100; i++) {
            if (i > 1) values.append(", ");
            values.append("(").append(i).append(", 'name_").append(i)
                  .append("', ").append(i * 1.5).append(")");
        }
        spark.sql("INSERT INTO ducklake.main.concurrent_tbl VALUES " + values);

        // Two SparkSessions (sharing SparkContext) reading simultaneously
        SparkSession session1 = spark.newSession();
        SparkSession session2 = spark.newSession();

        ExecutorService executor = Executors.newFixedThreadPool(2);

        Future<Long> future1 = executor.submit(() ->
                session1.sql("SELECT * FROM ducklake.main.concurrent_tbl WHERE id <= 50").count());
        Future<Long> future2 = executor.submit(() ->
                session2.sql("SELECT * FROM ducklake.main.concurrent_tbl WHERE id > 50").count());

        assertEquals("Session 1 should see 50 rows", 50L, (long) future1.get(60, TimeUnit.SECONDS));
        assertEquals("Session 2 should see 50 rows", 50L, (long) future2.get(60, TimeUnit.SECONDS));

        // Also verify both sessions see the full dataset
        Future<Long> full1 = executor.submit(() ->
                session1.sql("SELECT COUNT(*) FROM ducklake.main.concurrent_tbl")
                        .collectAsList().get(0).getLong(0));
        Future<Long> full2 = executor.submit(() ->
                session2.sql("SELECT COUNT(*) FROM ducklake.main.concurrent_tbl")
                        .collectAsList().get(0).getLong(0));

        assertEquals(100L, (long) full1.get(60, TimeUnit.SECONDS));
        assertEquals(100L, (long) full2.get(60, TimeUnit.SECONDS));

        executor.shutdown();
        assertTrue("Executor should terminate", executor.awaitTermination(60, TimeUnit.SECONDS));
    }

    // ===============================================================
    // 5. Large dataset (100K rows)
    // ===============================================================

    @Test
    public void testLargeDataset() {
        spark.sql("CREATE TABLE ducklake.main.large_table (id INT, name STRING, value DOUBLE)");

        // Generate 100K rows efficiently via range + INSERT AS SELECT
        spark.range(100000).createOrReplaceTempView("gen_source");
        spark.sql("INSERT INTO ducklake.main.large_table " +
                "SELECT cast(id as int), concat('name_', id), cast(id * 1.5 as double) FROM gen_source");

        // Verify count
        long count = spark.sql("SELECT COUNT(*) FROM ducklake.main.large_table")
                .collectAsList().get(0).getLong(0);
        assertEquals("Should have 100K rows", 100000L, count);

        // Verify specific sample rows
        Dataset<Row> sample = spark.sql(
                "SELECT * FROM ducklake.main.large_table WHERE id IN (0, 50000, 99999) ORDER BY id");
        List<Row> rows = sample.collectAsList();
        assertEquals(3, rows.size());

        assertEquals(0, rows.get(0).getInt(0));
        assertEquals("name_0", rows.get(0).getString(1));
        assertEquals(0.0, rows.get(0).getDouble(2), 0.001);

        assertEquals(50000, rows.get(1).getInt(0));
        assertEquals("name_50000", rows.get(1).getString(1));
        assertEquals(75000.0, rows.get(1).getDouble(2), 0.001);

        assertEquals(99999, rows.get(2).getInt(0));
        assertEquals("name_99999", rows.get(2).getString(1));
        assertEquals(149998.5, rows.get(2).getDouble(2), 0.001);

        // Verify aggregation correctness
        Dataset<Row> agg = spark.sql(
                "SELECT MIN(id), MAX(id), SUM(cast(id as bigint)) FROM ducklake.main.large_table");
        Row aggRow = agg.collectAsList().get(0);
        assertEquals(0, aggRow.getInt(0));
        assertEquals(99999, aggRow.getInt(1));
        // Sum of 0..99999 = 99999 * 100000 / 2 = 4999950000
        assertEquals(4999950000L, aggRow.getLong(2));
    }

    // ===============================================================
    // 6. End-to-end workflow with time travel
    // ===============================================================

    @Test
    public void testEndToEndWorkflow() throws Exception {
        String table = "e2e_workflow";

        // CREATE TABLE
        spark.sql("CREATE TABLE ducklake.main." + table +
                " (id INT, name STRING, value DOUBLE)");

        // INSERT batch 1 (rows 1-10)
        insertRows(table, 1, 10);
        long snapAfterInsert1 = getLatestSnapshotId();
        assertEquals(10, spark.sql("SELECT COUNT(*) FROM ducklake.main." + table)
                .collectAsList().get(0).getLong(0));

        // INSERT batch 2 (rows 11-20)
        insertRows(table, 11, 20);
        long snapAfterInsert2 = getLatestSnapshotId();
        assertEquals(20, spark.sql("SELECT COUNT(*) FROM ducklake.main." + table)
                .collectAsList().get(0).getLong(0));

        // UPDATE: set name='updated' for rows 1-5
        DuckLakeUpdateExecutor updater = new DuckLakeUpdateExecutor(
                catalogPath, dataPath, table, "main");
        Map<String, Object> updates = new HashMap<>();
        updates.put("name", "updated");
        updater.updateWhere(
                new Filter[]{new LessThanOrEqual("id", 5)},
                updates);
        long snapAfterUpdate = getLatestSnapshotId();

        // Verify update
        List<Row> updatedRows = spark.sql(
                "SELECT name FROM ducklake.main." + table +
                " WHERE id <= 5 ORDER BY id").collectAsList();
        assertEquals(5, updatedRows.size());
        for (Row r : updatedRows) {
            assertEquals("updated", r.getString(0));
        }

        // DELETE: remove rows where id > 15
        spark.sql("DELETE FROM ducklake.main." + table + " WHERE id > 15");
        long snapAfterDelete = getLatestSnapshotId();
        assertEquals(15, spark.sql("SELECT COUNT(*) FROM ducklake.main." + table)
                .collectAsList().get(0).getLong(0));

        // COMPACT
        DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, table, "main");

        // Verify data integrity after compaction
        Dataset<Row> afterCompact = spark.sql(
                "SELECT * FROM ducklake.main." + table + " ORDER BY id");
        List<Row> compactRows = afterCompact.collectAsList();
        assertEquals(15, compactRows.size());

        // First 5 rows should have "updated" name
        for (int i = 0; i < 5; i++) {
            assertEquals(i + 1, compactRows.get(i).getInt(0));
            assertEquals("updated", compactRows.get(i).getString(1));
        }
        // Rows 6-15 should have original names
        for (int i = 5; i < 15; i++) {
            assertEquals(i + 1, compactRows.get(i).getInt(0));
            assertEquals("name_" + (i + 1), compactRows.get(i).getString(1));
        }

        // TIME TRAVEL: read at snapshot after first insert (10 rows)
        Dataset<Row> atInsert1 = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", table)
                .option("snapshot_version", String.valueOf(snapAfterInsert1))
                .load();
        assertEquals("After insert1 should have 10 rows", 10, atInsert1.count());

        // TIME TRAVEL: read at snapshot after second insert (20 rows)
        Dataset<Row> atInsert2 = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", table)
                .option("snapshot_version", String.valueOf(snapAfterInsert2))
                .load();
        assertEquals("After insert2 should have 20 rows", 20, atInsert2.count());

        // TIME TRAVEL: read at snapshot after delete (15 rows)
        Dataset<Row> atDelete = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", table)
                .option("snapshot_version", String.valueOf(snapAfterDelete))
                .load();
        assertEquals("After delete should have 15 rows", 15, atDelete.count());
    }

    // ---------------------------------------------------------------
    // Catalog setup helper (minimal — "main" schema only, no tables)
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
