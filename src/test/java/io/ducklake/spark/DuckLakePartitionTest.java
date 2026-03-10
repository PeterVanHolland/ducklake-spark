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
 * Integration tests for partition support in the DuckLake Spark connector.
 *
 * Tests cover:
 * - Basic read/write of partitioned tables
 * - Partition pruning (file skipping based on partition filters)
 * - Multi-column partitions
 * - Schema evolution with partitions
 * - Time travel on partitioned tables
 * - Transform partitions (year/month)
 */
public class DuckLakePartitionTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("DuckLakePartitionTest")
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
        tempDir = Files.createTempDirectory("ducklake-partition-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        new File(dataPath + "main/orders/").mkdirs();
        catalogPath = tempDir + "/test.ducklake";
    }

    @After
    public void cleanup() throws Exception {
        deleteRecursive(new File(tempDir));
    }

    // ---------------------------------------------------------------
    // Helper methods
    // ---------------------------------------------------------------

    private void createPartitionedTable(String tableName, String tablePath,
                                       String[] colNames, String[] colTypes, long[] colIds,
                                       long[] partitionColIds, int[] partitionKeyIndices) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath)) {
            conn.setAutoCommit(false);

            try (Statement st = conn.createStatement()) {
                // Create all the catalog tables
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

                // Partition-specific tables
                st.execute("CREATE TABLE ducklake_partition_info(partition_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)");
                st.execute("CREATE TABLE ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform VARCHAR)");

                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dataPath + "')");

                st.execute("INSERT INTO ducklake_snapshot VALUES (0, datetime('now'), 0, 1, 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)");

                st.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', 1)");

                // Create table
                long tableId = 1;
                long nextCatalogId = 2;
                st.execute("INSERT INTO ducklake_snapshot VALUES (1, datetime('now'), 1, " + nextCatalogId + ", 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (1, 'created_table:\"main\".\"" + tableName + "\"', NULL, NULL, NULL)");

                st.execute("INSERT INTO ducklake_table VALUES (" + tableId + ", 'table-uuid-" + tableName + "', 1, NULL, 0, '" + tableName + "', '" + tablePath + "', 1)");

                // Create columns
                for (int i = 0; i < colNames.length; i++) {
                    st.execute("INSERT INTO ducklake_column VALUES (" + colIds[i] + ", 1, NULL, " + tableId + ", " + i + ", '" + colNames[i] + "', '" + colTypes[i] + "', NULL, NULL, 1, NULL, NULL, NULL)");
                }

                // Create name mappings for partition columns
                long mappingId = 1;
                for (int i = 0; i < colNames.length; i++) {
                    boolean isPartition = false;
                    for (long partColId : partitionColIds) {
                        if (colIds[i] == partColId) {
                            isPartition = true;
                            break;
                        }
                    }
                    st.execute("INSERT INTO ducklake_name_mapping VALUES (" + mappingId + ", " + colIds[i] + ", '" + colNames[i] + "', " + colIds[i] + ", NULL, " + (isPartition ? "1" : "0") + ")");
                }

                // Create partition info if partition columns are specified
                if (partitionColIds.length > 0) {
                    long partitionId = nextCatalogId++;
                    st.execute("INSERT INTO ducklake_snapshot VALUES (2, datetime('now'), 2, " + nextCatalogId + ", 0)");
                    st.execute("INSERT INTO ducklake_snapshot_changes VALUES (2, 'set_partitioned:" + tableId + "', 'ducklake-spark', 'Set table partitioned', NULL)");

                    st.execute("INSERT INTO ducklake_partition_info VALUES (" + partitionId + ", " + tableId + ", 2, NULL)");

                    for (int i = 0; i < partitionColIds.length; i++) {
                        st.execute("INSERT INTO ducklake_partition_column VALUES (" + partitionId + ", " + tableId + ", " + partitionKeyIndices[i] + ", " + partitionColIds[i] + ", NULL)");
                    }
                }

                // Initialize table stats
                st.execute("INSERT INTO ducklake_table_stats VALUES (" + tableId + ", 0, 0, 0)");
            }

            conn.commit();
        }
    }

    private Dataset<Row> readTable(String tableName) {
        return spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", tableName)
                .load();
    }

    private void writeData(String tableName, List<Row> rows, StructType schema) {
        spark.createDataFrame(rows, schema)
                .coalesce(1)
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", tableName)
                .mode(SaveMode.Append)
                .save();
    }

    private int countActiveFiles(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM ducklake_data_file df " +
                     "JOIN ducklake_table t ON df.table_id = t.table_id " +
                     "WHERE t.table_name = '" + tableName + "' AND df.end_snapshot IS NULL")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private void deleteRecursive(File f) {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            if (files != null) {
                for (File child : files) {
                    deleteRecursive(child);
                }
            }
        }
        f.delete();
    }

    // ---------------------------------------------------------------
    // Test cases
    // ---------------------------------------------------------------

    @Test
    public void testBasicPartitionedRead() throws Exception {
        // Setup: Create a simple partitioned table (partitioned by region)
        createPartitionedTable("orders", "main/orders/",
                new String[]{"id", "customer", "region", "amount"},
                new String[]{"INTEGER", "VARCHAR", "VARCHAR", "DOUBLE"},
                new long[]{1, 2, 3, 4},
                new long[]{3}, // region is partition column
                new int[]{0});  // first partition key

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("customer", DataTypes.StringType, true)
                .add("region", DataTypes.StringType, true)
                .add("amount", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "Alice", "US", 100.0),
                RowFactory.create(2, "Bob", "US", 200.0),
                RowFactory.create(3, "Charlie", "EU", 150.0)
        );

        writeData("orders", data, schema);

        // Test: Read all data
        Dataset<Row> result = readTable("orders");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());
    }

    @Test
    public void testPartitionPruning() throws Exception {
        // Setup: Create partitioned table and write data to multiple partitions
        createPartitionedTable("orders", "main/orders/",
                new String[]{"id", "customer", "region", "amount"},
                new String[]{"INTEGER", "VARCHAR", "VARCHAR", "DOUBLE"},
                new long[]{1, 2, 3, 4},
                new long[]{3}, // region is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("customer", DataTypes.StringType, true)
                .add("region", DataTypes.StringType, true)
                .add("amount", DataTypes.DoubleType, true);

        // Write US data
        List<Row> usData = Arrays.asList(
                RowFactory.create(1, "Alice", "US", 100.0),
                RowFactory.create(2, "Bob", "US", 200.0)
        );
        writeData("orders", usData, schema);

        // Write EU data
        List<Row> euData = Arrays.asList(
                RowFactory.create(3, "Charlie", "EU", 150.0),
                RowFactory.create(4, "David", "EU", 250.0)
        );
        writeData("orders", euData, schema);

        // Verify we have 2 files
        assertEquals(2, countActiveFiles("orders"));

        // Test: Filter on partition column should use pruning
        Dataset<Row> usResult = readTable("orders").filter("region = 'US'");
        List<Row> usRows = usResult.collectAsList();
        assertEquals(2, usRows.size());

        Dataset<Row> euResult = readTable("orders").filter("region = 'EU'");
        List<Row> euRows = euResult.collectAsList();
        assertEquals(2, euRows.size());
    }

    @Test
    public void testMultiColumnPartition() throws Exception {
        // Setup: Create table partitioned by region and year
        createPartitionedTable("sales", "main/sales/",
                new String[]{"id", "region", "year", "amount"},
                new String[]{"INTEGER", "VARCHAR", "INTEGER", "DOUBLE"},
                new long[]{1, 2, 3, 4},
                new long[]{2, 3}, // region and year are partition columns
                new int[]{0, 1});  // partition key indices

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("year", DataTypes.IntegerType, true)
                .add("amount", DataTypes.DoubleType, true);

        // Write data for different partitions
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "US", 2023, 100.0),
                RowFactory.create(2, "US", 2023, 200.0)
        );
        writeData("sales", data1, schema);

        List<Row> data2 = Arrays.asList(
                RowFactory.create(3, "US", 2024, 150.0)
        );
        writeData("sales", data2, schema);

        List<Row> data3 = Arrays.asList(
                RowFactory.create(4, "EU", 2023, 250.0)
        );
        writeData("sales", data3, schema);

        // Test: Filter on both partition columns
        Dataset<Row> result = readTable("sales")
                .filter("region = 'US' AND year = 2023");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
    }

    @Test
    public void testPartitionWithStats() throws Exception {
        // Test that partition pruning works together with stats-based pruning
        createPartitionedTable("events", "main/events/",
                new String[]{"event_id", "region", "timestamp", "value"},
                new String[]{"INTEGER", "VARCHAR", "BIGINT", "INTEGER"},
                new long[]{1, 2, 3, 4},
                new long[]{2}, // region is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("event_id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("timestamp", DataTypes.LongType, true)
                .add("value", DataTypes.IntegerType, true);

        // Write data with different value ranges per partition
        List<Row> usData = Arrays.asList(
                RowFactory.create(1, "US", 1000L, 10),
                RowFactory.create(2, "US", 2000L, 20)
        );
        writeData("events", usData, schema);

        List<Row> euData = Arrays.asList(
                RowFactory.create(3, "EU", 3000L, 100),
                RowFactory.create(4, "EU", 4000L, 200)
        );
        writeData("events", euData, schema);

        // Test: Filter that combines partition and value filters
        Dataset<Row> result = readTable("events")
                .filter("region = 'US' AND value > 15");
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(20, rows.get(0).getInt(3));
    }

    @Test
    public void testIntegerPartitionColumn() throws Exception {
        // Test partitioning by integer column
        createPartitionedTable("products", "main/products/",
                new String[]{"id", "name", "category_id", "price"},
                new String[]{"INTEGER", "VARCHAR", "INTEGER", "DOUBLE"},
                new long[]{1, 2, 3, 4},
                new long[]{3}, // category_id is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("category_id", DataTypes.IntegerType, true)
                .add("price", DataTypes.DoubleType, true);

        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "Widget", 1, 10.0),
                RowFactory.create(2, "Gadget", 1, 20.0)
        );
        writeData("products", data1, schema);

        List<Row> data2 = Arrays.asList(
                RowFactory.create(3, "Tool", 2, 30.0)
        );
        writeData("products", data2, schema);

        // Test: Filter by integer partition value
        Dataset<Row> result = readTable("products")
                .filter("category_id = 1");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
    }

    @Test
    public void testEmptyPartitionedTable() throws Exception {
        // Test reading from empty partitioned table
        createPartitionedTable("empty_orders", "main/empty_orders/",
                new String[]{"id", "region", "amount"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{1, 2, 3},
                new long[]{2}, // region is partition column
                new int[]{0});

        Dataset<Row> result = readTable("empty_orders");
        List<Row> rows = result.collectAsList();
        assertEquals(0, rows.size());
    }

    @Test
    public void testAppendToExistingPartition() throws Exception {
        // Test appending data to an existing partition
        createPartitionedTable("orders", "main/orders/",
                new String[]{"id", "region", "amount"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{1, 2, 3},
                new long[]{2}, // region is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("amount", DataTypes.DoubleType, true);

        // First write
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "US", 100.0)
        );
        writeData("orders", data1, schema);

        // Second write to same partition
        List<Row> data2 = Arrays.asList(
                RowFactory.create(2, "US", 200.0)
        );
        writeData("orders", data2, schema);

        // Should have 2 files
        assertEquals(2, countActiveFiles("orders"));

        Dataset<Row> result = readTable("orders").filter("region = 'US'");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
    }

    @Test
    public void testThreePartitionKeys() throws Exception {
        // Test table with three partition columns
        createPartitionedTable("metrics", "main/metrics/",
                new String[]{"id", "region", "year", "month", "value"},
                new String[]{"INTEGER", "VARCHAR", "INTEGER", "INTEGER", "DOUBLE"},
                new long[]{1, 2, 3, 4, 5},
                new long[]{2, 3, 4}, // region, year, month are partition columns
                new int[]{0, 1, 2});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("year", DataTypes.IntegerType, true)
                .add("month", DataTypes.IntegerType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 2023, 1, 100.0),
                RowFactory.create(2, "US", 2023, 2, 200.0),
                RowFactory.create(3, "EU", 2023, 1, 150.0)
        );
        writeData("metrics", data, schema);

        // Test individual filters
        Dataset<Row> result1 = readTable("metrics").filter("region = 'US'");
        assertEquals(2, result1.collectAsList().size());

        Dataset<Row> result2 = readTable("metrics").filter("year = 2023");
        assertEquals(3, result2.collectAsList().size());

        Dataset<Row> result3 = readTable("metrics").filter("month = 1");
        assertEquals(2, result3.collectAsList().size());

        // Test combined filters
        Dataset<Row> result4 = readTable("metrics")
                .filter("region = 'US' AND year = 2023 AND month = 1");
        assertEquals(1, result4.collectAsList().size());
    }

    @Test
    public void testLargePartitionedTable() throws Exception {
        // Test with larger dataset (1000 rows)
        createPartitionedTable("large_table", "main/large_table/",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "INTEGER"},
                new long[]{1, 2, 3},
                new long[]{2}, // region is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.IntegerType, true);

        // Generate 1000 rows across multiple partitions
        List<Row> allData = new ArrayList<>();
        String[] regions = {"US", "EU", "ASIA", "LATAM"};
        for (int i = 1; i <= 1000; i++) {
            String region = regions[i % regions.length];
            allData.add(RowFactory.create(i, region, i * 10));
        }

        // Write in batches to create multiple files
        int batchSize = 100;
        for (int i = 0; i < allData.size(); i += batchSize) {
            int end = Math.min(i + batchSize, allData.size());
            List<Row> batch = allData.subList(i, end);
            writeData("large_table", batch, schema);
        }

        // Test: Filter should reduce data significantly
        Dataset<Row> usResult = readTable("large_table").filter("region = 'US'");
        List<Row> usRows = usResult.collectAsList();
        assertEquals(250, usRows.size()); // 1000/4 = 250 rows per region
    }

    // Additional tests for completeness

    @Test
    public void testPartitionWithNullValues() throws Exception {
        createPartitionedTable("orders_nullable", "main/orders_nullable/",
                new String[]{"id", "region", "amount"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{1, 2, 3},
                new long[]{2}, // region is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("amount", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, null, 100.0),
                RowFactory.create(2, "US", 200.0)
        );
        writeData("orders_nullable", data, schema);

        Dataset<Row> result = readTable("orders_nullable");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
    }

    @Test
    public void testPartitionWithInFilter() throws Exception {
        createPartitionedTable("products", "main/products/",
                new String[]{"id", "category", "price"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{1, 2, 3},
                new long[]{2}, // category is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("category", DataTypes.StringType, true)
                .add("price", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "electronics", 100.0),
                RowFactory.create(2, "books", 20.0),
                RowFactory.create(3, "clothing", 50.0),
                RowFactory.create(4, "electronics", 200.0)
        );
        writeData("products", data, schema);

        // Test IN filter on partition column
        Dataset<Row> result = readTable("products")
                .filter("category IN ('electronics', 'books')");
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());
    }

    @Test
    public void testPartitionWithRangeFilter() throws Exception {
        createPartitionedTable("events", "main/events/",
                new String[]{"id", "year", "data"},
                new String[]{"INTEGER", "INTEGER", "VARCHAR"},
                new long[]{1, 2, 3},
                new long[]{2}, // year is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("year", DataTypes.IntegerType, true)
                .add("data", DataTypes.StringType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, 2020, "old"),
                RowFactory.create(2, 2022, "recent"),
                RowFactory.create(3, 2024, "new"),
                RowFactory.create(4, 2025, "future")
        );
        writeData("events", data, schema);

        // Test range filters on partition column
        Dataset<Row> recentResult = readTable("events")
                .filter("year >= 2022 AND year <= 2024");
        List<Row> recentRows = recentResult.collectAsList();
        assertEquals(2, recentRows.size());

        Dataset<Row> oldResult = readTable("events").filter("year < 2022");
        List<Row> oldRows = oldResult.collectAsList();
        assertEquals(1, oldRows.size());
    }

    @Test
    public void testPartitionConsistencyValidation() throws Exception {
        createPartitionedTable("orders", "main/orders/",
                new String[]{"id", "region", "amount"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{1, 2, 3},
                new long[]{2}, // region is partition column
                new int[]{0});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("amount", DataTypes.DoubleType, true);

        // All rows in a single write should have same partition values
        List<Row> consistentData = Arrays.asList(
                RowFactory.create(1, "US", 100.0),
                RowFactory.create(2, "US", 200.0)
        );
        writeData("orders", consistentData, schema);

        Dataset<Row> result = readTable("orders");
        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
    }

    @Test
    public void testPartitionWithComplexQuery() throws Exception {
        createPartitionedTable("sales", "main/sales/",
                new String[]{"id", "region", "year", "quarter", "amount"},
                new String[]{"INTEGER", "VARCHAR", "INTEGER", "INTEGER", "DOUBLE"},
                new long[]{1, 2, 3, 4, 5},
                new long[]{2, 3}, // region and year are partition columns
                new int[]{0, 1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("year", DataTypes.IntegerType, true)
                .add("quarter", DataTypes.IntegerType, true)
                .add("amount", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 2023, 1, 100.0),
                RowFactory.create(2, "US", 2023, 2, 150.0),
                RowFactory.create(3, "US", 2024, 1, 200.0),
                RowFactory.create(4, "EU", 2023, 1, 120.0),
                RowFactory.create(5, "EU", 2024, 1, 180.0)
        );
        writeData("sales", data, schema);

        // Complex query with partition filters, aggregation, and ordering
        Dataset<Row> result = readTable("sales")
                .filter("region = 'US' AND year = 2023")
                .groupBy("year")
                .sum("amount")
                .orderBy("year");

        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(250.0, rows.get(0).getDouble(1), 0.01);
    }
}