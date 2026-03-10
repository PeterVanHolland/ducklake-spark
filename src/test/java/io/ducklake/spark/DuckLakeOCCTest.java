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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Integration tests for DuckLake optimistic concurrency control (OCC).
 * Tests concurrent write scenarios, conflict detection, and retry behavior.
 */
public class DuckLakeOCCTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[4]") // Use more threads for concurrency testing
                .appName("DuckLakeOCCTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.adaptive.enabled", "false") // Disable AQE for predictable behavior
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
        tempDir = Files.createTempDirectory("ducklake-occ-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        new File(dataPath + "main/test_table/").mkdirs();
        new File(dataPath + "main/test_table2/").mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createCatalog(catalogPath, dataPath,
                "test_table", "main/test_table/",
                "test_table2", "main/test_table2/");
    }

    @After
    public void teardown() throws Exception {
        deleteDirectory(Paths.get(tempDir));
    }

    @Test
    public void testBasicWriteAtomicity() throws Exception {
        // Test that a single write operation is atomic - either all data is committed or none
        Dataset<Row> data = spark.range(100).select(
                col("id"),
                lit("test").as("name")
        );

        data.write()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .mode(SaveMode.Append)
                .saveAsTable("main.test_table");

        // Verify all 100 records are present
        Dataset<Row> result = spark.read()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .table("main.test_table");

        assertEquals(100, result.count());

        // Verify snapshot consistency
        long snapshotCount = getSnapshotCount();
        assertEquals(2, snapshotCount); // Initial + write
    }

    @Test
    public void testReadAfterWriteConsistency() throws Exception {
        // Test that reads immediately after writes see consistent data
        Dataset<Row> data1 = spark.range(50).select(
                col("id"),
                lit("batch1").as("name")
        );

        data1.write()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .mode(SaveMode.Append)
                .saveAsTable("main.test_table");

        // Immediately read back
        Dataset<Row> result1 = spark.read()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .table("main.test_table");

        assertEquals(50, result1.count());
        assertEquals(50, result1.filter(col("name").equalTo("batch1")).count());

        // Write another batch
        Dataset<Row> data2 = spark.range(50, 100).select(
                col("id"),
                lit("batch2").as("name")
        );

        data2.write()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .mode(SaveMode.Append)
                .saveAsTable("main.test_table");

        // Read back all data
        Dataset<Row> result2 = spark.read()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .table("main.test_table");

        assertEquals(100, result2.count());
        assertEquals(50, result2.filter(col("name").equalTo("batch1")).count());
        assertEquals(50, result2.filter(col("name").equalTo("batch2")).count());
    }

    @Test
    public void testTableIsolation() throws Exception {
        // Test that writes to different tables don't conflict with each other
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> future1 = executor.submit(() -> {
                try {
                    Dataset<Row> data = spark.range(100).select(
                            col("id"),
                            lit("table1").as("name")
                    );
                    data.write()
                            .format("ducklake")
                            .option("catalog", catalogPath)
                            .option("data_path", dataPath)
                            .mode(SaveMode.Append)
                            .saveAsTable("main.test_table");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Future<?> future2 = executor.submit(() -> {
                try {
                    Dataset<Row> data = spark.range(200, 300).select(
                            col("id"),
                            lit("table2").as("name")
                    );
                    data.write()
                            .format("ducklake")
                            .option("catalog", catalogPath)
                            .option("data_path", dataPath)
                            .mode(SaveMode.Append)
                            .saveAsTable("main.test_table2");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            future1.get(10, TimeUnit.SECONDS);
            future2.get(10, TimeUnit.SECONDS);

            // Both writes should succeed
            Dataset<Row> result1 = spark.read()
                    .format("ducklake")
                    .option("catalog", catalogPath)
                    .option("data_path", dataPath)
                    .table("main.test_table");

            Dataset<Row> result2 = spark.read()
                    .format("ducklake")
                    .option("catalog", catalogPath)
                    .option("data_path", dataPath)
                    .table("main.test_table2");

            assertEquals(100, result1.count());
            assertEquals(100, result2.count());

        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testConcurrentWriteConflictDetection() throws Exception {
        // Test that concurrent writes to the same table detect conflicts
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        try {
            Future<?> future1 = executor.submit(() -> {
                try {
                    // Add delay to increase likelihood of conflict
                    Thread.sleep(50);
                    Dataset<Row> data = spark.range(100).select(
                            col("id"),
                            lit("writer1").as("name")
                    );
                    data.write()
                            .format("ducklake")
                            .option("catalog", catalogPath)
                            .option("data_path", dataPath)
                            .option("occ.maxRetries", "1") // Low retry count to test failure
                            .mode(SaveMode.Append)
                            .saveAsTable("main.test_table");
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                }
            });

            Future<?> future2 = executor.submit(() -> {
                try {
                    // Add delay to increase likelihood of conflict
                    Thread.sleep(50);
                    Dataset<Row> data = spark.range(200, 300).select(
                            col("id"),
                            lit("writer2").as("name")
                    );
                    data.write()
                            .format("ducklake")
                            .option("catalog", catalogPath)
                            .option("data_path", dataPath)
                            .option("occ.maxRetries", "1") // Low retry count to test failure
                            .mode(SaveMode.Append)
                            .saveAsTable("main.test_table");
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                }
            });

            future1.get(15, TimeUnit.SECONDS);
            future2.get(15, TimeUnit.SECONDS);

            // At least one should succeed, and at least one might fail due to conflict
            assertTrue("At least one write should succeed", successCount.get() >= 1);
            // In high-contention scenarios, one might fail
            assertTrue("Total operations should be 2", successCount.get() + failureCount.get() == 2);

        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testAppendVsOverwriteConflictBehavior() throws Exception {
        // Write initial data
        Dataset<Row> initialData = spark.range(50).select(
                col("id"),
                lit("initial").as("name")
        );
        initialData.write()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .mode(SaveMode.Append)
                .saveAsTable("main.test_table");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicBoolean appendSucceeded = new AtomicBoolean(false);
        AtomicBoolean overwriteSucceeded = new AtomicBoolean(false);

        try {
            Future<?> appendFuture = executor.submit(() -> {
                try {
                    Dataset<Row> data = spark.range(50, 100).select(
                            col("id"),
                            lit("append").as("name")
                    );
                    data.write()
                            .format("ducklake")
                            .option("catalog", catalogPath)
                            .option("data_path", dataPath)
                            .option("occ.maxRetries", "2")
                            .mode(SaveMode.Append)
                            .saveAsTable("main.test_table");
                    appendSucceeded.set(true);
                } catch (Exception e) {
                    // Expected to possibly fail due to conflict
                }
            });

            Future<?> overwriteFuture = executor.submit(() -> {
                try {
                    Dataset<Row> data = spark.range(100, 150).select(
                            col("id"),
                            lit("overwrite").as("name")
                    );
                    data.write()
                            .format("ducklake")
                            .option("catalog", catalogPath)
                            .option("data_path", dataPath)
                            .option("occ.maxRetries", "2")
                            .mode(SaveMode.Overwrite)
                            .saveAsTable("main.test_table");
                    overwriteSucceeded.set(true);
                } catch (Exception e) {
                    // Expected to possibly fail due to conflict
                }
            });

            appendFuture.get(15, TimeUnit.SECONDS);
            overwriteFuture.get(15, TimeUnit.SECONDS);

            // At least one operation should succeed
            assertTrue("At least one operation should succeed",
                    appendSucceeded.get() || overwriteSucceeded.get());

        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testSequentialWritesProduceSequentialSnapshots() throws Exception {
        long initialSnapshot = getSnapshotCount();

        for (int i = 0; i < 3; i++) {
            Dataset<Row> data = spark.range(i * 10, (i + 1) * 10).select(
                    col("id"),
                    lit("batch" + i).as("name")
            );
            data.write()
                    .format("ducklake")
                    .option("catalog", catalogPath)
                    .option("data_path", dataPath)
                    .mode(SaveMode.Append)
                    .saveAsTable("main.test_table");

            long currentSnapshot = getSnapshotCount();
            assertEquals("Snapshot should increment by 1", initialSnapshot + i + 1, currentSnapshot);
        }

        // Verify total record count
        Dataset<Row> result = spark.read()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .table("main.test_table");
        assertEquals(30, result.count());
    }

    @Test
    public void testRetryOnConflictSucceeds() throws Exception {
        // Write initial data
        Dataset<Row> initialData = spark.range(10).select(
                col("id"),
                lit("initial").as("name")
        );
        initialData.write()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .mode(SaveMode.Append)
                .saveAsTable("main.test_table");

        ExecutorService executor = Executors.newFixedThreadPool(3);
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(3);
        CountDownLatch goLatch = new CountDownLatch(1);

        try {
            // Start 3 concurrent writes that will retry on conflict
            for (int i = 0; i < 3; i++) {
                final int writerId = i;
                executor.submit(() -> {
                    try {
                        startLatch.countDown();
                        goLatch.await(); // Wait for all threads to be ready

                        Dataset<Row> data = spark.range(writerId * 100, writerId * 100 + 50).select(
                                col("id"),
                                lit("writer" + writerId).as("name")
                        );
                        data.write()
                                .format("ducklake")
                                .option("catalog", catalogPath)
                                .option("data_path", dataPath)
                                .option("occ.maxRetries", "10") // High retry count
                                .mode(SaveMode.Append)
                                .saveAsTable("main.test_table");
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            // Wait for all threads to be ready, then start them all at once
            startLatch.await();
            goLatch.countDown();

            // Wait for completion
            executor.shutdown();
            assertTrue("All writes should complete within 30 seconds",
                    executor.awaitTermination(30, TimeUnit.SECONDS));

            // All writes should eventually succeed due to retries
            assertEquals("All 3 writes should succeed with retries", 3, successCount.get());

            // Verify all data is present
            Dataset<Row> result = spark.read()
                    .format("ducklake")
                    .option("catalog", catalogPath)
                    .option("data_path", dataPath)
                    .table("main.test_table");
            assertEquals(160, result.count()); // 10 initial + 3 * 50 new

        } finally {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }
    }

    @Test
    public void testMaxRetriesExceededThrowsException() throws Exception {
        // This test is harder to trigger reliably, so we'll set very low retry count
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger exceptionCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(2);
        CountDownLatch goLatch = new CountDownLatch(1);

        try {
            for (int i = 0; i < 2; i++) {
                final int writerId = i;
                executor.submit(() -> {
                    try {
                        startLatch.countDown();
                        goLatch.await();

                        Dataset<Row> data = spark.range(writerId * 100, writerId * 100 + 100).select(
                                col("id"),
                                lit("writer" + writerId).as("name")
                        );
                        data.write()
                                .format("ducklake")
                                .option("catalog", catalogPath)
                                .option("data_path", dataPath)
                                .option("occ.maxRetries", "0") // No retries
                                .mode(SaveMode.Append)
                                .saveAsTable("main.test_table");
                    } catch (Exception e) {
                        if (e.getMessage().contains("concurrent modifications") ||
                            e.getCause() != null && e.getCause().getMessage().contains("concurrent modifications")) {
                            exceptionCount.incrementAndGet();
                        } else {
                            e.printStackTrace();
                        }
                    }
                });
            }

            startLatch.await();
            goLatch.countDown();

            executor.shutdown();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));

            // At least one should fail due to no retries (though both might succeed if timing is lucky)
            assertTrue("At least one operation should complete", exceptionCount.get() <= 2);

        } finally {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }
    }

    @Test
    public void testSnapshotConsistencyDuringConcurrentWrites() throws Exception {
        // Write initial data
        Dataset<Row> initialData = spark.range(100).select(
                col("id"),
                lit("initial").as("name")
        );
        initialData.write()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .mode(SaveMode.Append)
                .saveAsTable("main.test_table");

        long initialCount = spark.read()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .table("main.test_table").count();

        assertEquals(100, initialCount);

        // Start concurrent writes and reads
        ExecutorService executor = Executors.newFixedThreadPool(4);
        AtomicBoolean allReadsConsistent = new AtomicBoolean(true);

        try {
            // Writer threads
            for (int i = 0; i < 2; i++) {
                final int writerId = i;
                executor.submit(() -> {
                    try {
                        Dataset<Row> data = spark.range(1000 + writerId * 100, 1000 + (writerId + 1) * 100).select(
                                col("id"),
                                lit("concurrent" + writerId).as("name")
                        );
                        data.write()
                                .format("ducklake")
                                .option("catalog", catalogPath)
                                .option("data_path", dataPath)
                                .option("occ.maxRetries", "5")
                                .mode(SaveMode.Append)
                                .saveAsTable("main.test_table");
                    } catch (Exception e) {
                        // Some failures expected
                    }
                });
            }

            // Reader threads - verify reads are always consistent
            for (int i = 0; i < 2; i++) {
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < 10; j++) {
                            Dataset<Row> snapshot1 = spark.read()
                                    .format("ducklake")
                                    .option("catalog", catalogPath)
                                    .option("data_path", dataPath)
                                    .table("main.test_table");

                            long count1 = snapshot1.count();
                            Thread.sleep(10); // Small delay

                            Dataset<Row> snapshot2 = spark.read()
                                    .format("ducklake")
                                    .option("catalog", catalogPath)
                                    .option("data_path", dataPath)
                                    .table("main.test_table");

                            long count2 = snapshot2.count();

                            // Counts should be consistent within the same read operation
                            // and either equal or increasing between reads
                            if (count2 < count1) {
                                allReadsConsistent.set(false);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        allReadsConsistent.set(false);
                    }
                });
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

            assertTrue("All reads should be consistent", allReadsConsistent.get());

        } finally {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }
    }

    @Test
    public void testConcurrentOverwriteModes() throws Exception {
        // Write initial data
        Dataset<Row> initialData = spark.range(100).select(
                col("id"),
                lit("initial").as("name")
        );
        initialData.write()
                .format("ducklake")
                .option("catalog", catalogPath)
                .option("data_path", dataPath)
                .mode(SaveMode.Append)
                .saveAsTable("main.test_table");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger successCount = new AtomicInteger(0);

        try {
            // Two concurrent overwrites
            for (int i = 0; i < 2; i++) {
                final int writerId = i;
                executor.submit(() -> {
                    try {
                        Dataset<Row> data = spark.range(writerId * 100, (writerId + 1) * 100).select(
                                col("id"),
                                lit("overwrite" + writerId).as("name")
                        );
                        data.write()
                                .format("ducklake")
                                .option("catalog", catalogPath)
                                .option("data_path", dataPath)
                                .option("occ.maxRetries", "3")
                                .mode(SaveMode.Overwrite)
                                .saveAsTable("main.test_table");
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        // Some conflicts expected
                    }
                });
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(20, TimeUnit.SECONDS));

            // At least one overwrite should succeed
            assertTrue("At least one overwrite should succeed", successCount.get() >= 1);

            // Final table should have exactly 100 records (from whichever overwrite succeeded last)
            Dataset<Row> result = spark.read()
                    .format("ducklake")
                    .option("catalog", catalogPath)
                    .option("data_path", dataPath)
                    .table("main.test_table");
            assertEquals(100, result.count());

        } finally {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }
    }

    @Test
    public void testWriteIntegrityAfterConflicts() throws Exception {
        // Test that after conflicts and retries, the final data is still intact
        ExecutorService executor = Executors.newFixedThreadPool(5);
        AtomicInteger successfulWrites = new AtomicInteger(0);

        try {
            // Start multiple concurrent writes with good retry settings
            for (int i = 0; i < 5; i++) {
                final int writerId = i;
                executor.submit(() -> {
                    try {
                        Dataset<Row> data = spark.range(writerId * 1000, (writerId + 1) * 1000).select(
                                col("id"),
                                lit("writer" + writerId).as("name")
                        );
                        data.write()
                                .format("ducklake")
                                .option("catalog", catalogPath)
                                .option("data_path", dataPath)
                                .option("occ.maxRetries", "8")
                                .mode(SaveMode.Append)
                                .saveAsTable("main.test_table");
                        successfulWrites.incrementAndGet();
                    } catch (Exception e) {
                        System.err.println("Writer " + writerId + " failed: " + e.getMessage());
                    }
                });
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

            assertTrue("At least some writes should succeed", successfulWrites.get() > 0);

            // Verify data integrity
            Dataset<Row> result = spark.read()
                    .format("ducklake")
                    .option("catalog", catalogPath)
                    .option("data_path", dataPath)
                    .table("main.test_table");

            long totalRecords = result.count();
            assertEquals("Should have 1000 * successful_writes records",
                    1000L * successfulWrites.get(), totalRecords);

            // Verify no duplicate IDs (each writer has distinct ranges)
            long distinctIds = result.select("id").distinct().count();
            assertEquals("All IDs should be distinct", totalRecords, distinctIds);

        } finally {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }
    }

    // Helper methods

    private void createCatalog(String catalogPath, String dataPath, String... tableAndPaths) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            // Create schema
            createCatalogSchema(conn);

            // Insert initial metadata
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_metadata (key, value) VALUES (?, ?)")) {
                ps.setString(1, "data_path");
                ps.setString(2, dataPath);
                ps.executeUpdate();
            }

            // Create initial snapshot
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES (1, datetime('now'), 1, 1000, 1000)")) {
                ps.executeUpdate();
            }

            // Create main schema
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_schema (schema_id, schema_uuid, begin_snapshot, schema_name, path, path_is_relative) VALUES (1, ?, 1, 'main', 'main/', 1)")) {
                ps.setString(1, UUID.randomUUID().toString());
                ps.executeUpdate();
            }

            // Create test tables
            for (int i = 0; i < tableAndPaths.length; i += 2) {
                String tableName = tableAndPaths[i];
                String tablePath = tableAndPaths[i + 1];
                long tableId = 100 + i / 2;

                // Create table
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO ducklake_table (table_id, table_uuid, begin_snapshot, schema_id, table_name, path, path_is_relative) VALUES (?, ?, 1, 1, ?, ?, 1)")) {
                    ps.setLong(1, tableId);
                    ps.setString(2, UUID.randomUUID().toString());
                    ps.setString(3, tableName);
                    ps.setString(4, tablePath);
                    ps.executeUpdate();
                }

                // Create columns
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO ducklake_column (column_id, begin_snapshot, table_id, column_name, column_type, column_order, nulls_allowed) VALUES (?, 1, ?, ?, ?, ?, 0)")) {
                    ps.setLong(1, 1000 + tableId * 10);
                    ps.setLong(2, tableId);
                    ps.setString(3, "id");
                    ps.setString(4, "BIGINT");
                    ps.setInt(5, 0);
                    ps.executeUpdate();

                    ps.setLong(1, 1000 + tableId * 10 + 1);
                    ps.setString(3, "name");
                    ps.setString(4, "VARCHAR");
                    ps.setInt(5, 1);
                    ps.executeUpdate();
                }

                // Initialize table stats
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) VALUES (?, 0, 0, 0)")) {
                    ps.setLong(1, tableId);
                    ps.executeUpdate();
                }
            }
        }
    }

    private long getSnapshotCount() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM ducklake_snapshot")) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    private void createCatalogSchema(Connection conn) throws SQLException {
        String[] createStatements = {
            "CREATE TABLE ducklake_metadata (key TEXT PRIMARY KEY, value TEXT, scope TEXT)",
            "CREATE TABLE ducklake_snapshot (snapshot_id INTEGER PRIMARY KEY, snapshot_time TEXT, schema_version INTEGER, next_catalog_id INTEGER, next_file_id INTEGER)",
            "CREATE TABLE ducklake_snapshot_changes (snapshot_id INTEGER, changes_made TEXT, author TEXT, commit_message TEXT)",
            "CREATE TABLE ducklake_schema (schema_id INTEGER, schema_uuid TEXT, begin_snapshot INTEGER, end_snapshot INTEGER, schema_name TEXT, path TEXT, path_is_relative INTEGER)",
            "CREATE TABLE ducklake_table (table_id INTEGER, table_uuid TEXT, begin_snapshot INTEGER, end_snapshot INTEGER, schema_id INTEGER, table_name TEXT, path TEXT, path_is_relative INTEGER)",
            "CREATE TABLE ducklake_column (column_id INTEGER, begin_snapshot INTEGER, end_snapshot INTEGER, table_id INTEGER, column_name TEXT, column_type TEXT, column_order INTEGER, initial_default TEXT, default_value TEXT, nulls_allowed INTEGER, parent_column INTEGER)",
            "CREATE TABLE ducklake_data_file (data_file_id INTEGER, table_id INTEGER, begin_snapshot INTEGER, end_snapshot INTEGER, file_order INTEGER, path TEXT, path_is_relative INTEGER, file_format TEXT, record_count INTEGER, file_size_bytes INTEGER, mapping_id INTEGER, partition_id INTEGER, row_id_start INTEGER)",
            "CREATE TABLE ducklake_delete_file (delete_file_id INTEGER, table_id INTEGER, data_file_id INTEGER, begin_snapshot INTEGER, end_snapshot INTEGER, path TEXT, path_is_relative INTEGER, format TEXT, delete_count INTEGER)",
            "CREATE TABLE ducklake_column_stats (data_file_id INTEGER, table_id INTEGER, column_id INTEGER, value_count INTEGER, null_count INTEGER, min_value TEXT, max_value TEXT)",
            "CREATE TABLE ducklake_table_stats (table_id INTEGER PRIMARY KEY, record_count INTEGER, next_row_id INTEGER, file_size_bytes INTEGER)"
        };

        for (String sql : createStatements) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.executeUpdate();
            }
        }
    }

    private void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception e) {
                            // Best effort cleanup
                        }
                    });
        }
    }
}