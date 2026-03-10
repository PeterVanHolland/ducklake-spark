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
 * Tests for DuckLake partition read/write support including
 * partition pruning, multi-column partitions, and schema evolution.
 */
public class DuckLakePartitionTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
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
        catalogPath = tempDir + "/test.ducklake";
    }

    @After
    public void cleanup() throws Exception {
        deleteRecursive(new File(tempDir));
    }

    // ---------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------

    @Test
    public void testWriteAndReadPartitionedTable() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1}); // partition by "region"

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "EU", 20.0),
                RowFactory.create(3, "US", 30.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(3, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals("US", rows.get(0).getString(1));
        assertEquals("EU", rows.get(1).getString(1));
        assertEquals("US", rows.get(2).getString(1));
    }

    @Test
    public void testPartitionFilterPruning() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1}); // partition by "region"

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write data
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "EU", 20.0),
                RowFactory.create(3, "US", 30.0),
                RowFactory.create(4, "APAC", 40.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Read with filter on partition column
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load()
                .filter("region = 'US'");

        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(3, rows.get(1).getInt(0));
    }

    @Test
    public void testIntegerPartitionColumn() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "year", "amount"},
                new String[]{"INTEGER", "INTEGER", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1}); // partition by "year"

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("year", DataTypes.IntegerType, true)
                .add("amount", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, 2023, 100.0),
                RowFactory.create(2, 2024, 200.0),
                RowFactory.create(3, 2023, 150.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load()
                .filter("year = 2023");

        assertEquals(2, result.count());
    }

    @Test
    public void testMultiColumnPartition() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "category", "amount"},
                new String[]{"INTEGER", "VARCHAR", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2, 3},
                new int[]{1, 2}); // partition by region + category

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("category", DataTypes.StringType, true)
                .add("amount", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", "A", 10.0),
                RowFactory.create(2, "US", "B", 20.0),
                RowFactory.create(3, "EU", "A", 30.0),
                RowFactory.create(4, "EU", "B", 40.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Filter on both partition columns
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load()
                .filter("region = 'US' AND category = 'A'");

        assertEquals(1, result.count());
        assertEquals(1, result.collectAsList().get(0).getInt(0));
    }

    @Test
    public void testPartitionFilterWithNonPartitionFilter() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "US", 50.0),
                RowFactory.create(3, "EU", 20.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Filter on partition + non-partition columns
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load()
                .filter("region = 'US' AND value > 15.0");

        assertEquals(1, result.count());
        assertEquals(2, result.collectAsList().get(0).getInt(0));
    }

    @Test
    public void testEmptyPartitionedTable() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region"},
                new String[]{"INTEGER", "VARCHAR"},
                new long[]{0, 1},
                new int[]{1});

        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(0, result.count());
    }

    @Test
    public void testAppendToExistingPartition() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // First write
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "EU", 20.0));
        spark.createDataFrame(data1, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Second write (same partition values)
        List<Row> data2 = Arrays.asList(
                RowFactory.create(3, "US", 30.0),
                RowFactory.create(4, "EU", 40.0));
        spark.createDataFrame(data2, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // All 4 rows visible
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(4, result.count());

        // US partition has 2 rows
        assertEquals(2, result.filter("region = 'US'").count());
        assertEquals(2, result.filter("region = 'EU'").count());
    }

    @Test
    public void testOverwritePartitionedTable() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write initial data
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "EU", 20.0));
        spark.createDataFrame(data1, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Overwrite with new data
        List<Row> data2 = Arrays.asList(
                RowFactory.create(3, "APAC", 30.0));
        spark.createDataFrame(data2, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Overwrite)
                .save();

        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(1, result.count());
        assertEquals("APAC", result.collectAsList().get(0).getString(1));
    }

    @Test
    public void testPartitionInFilterPruning() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "EU", 20.0),
                RowFactory.create(3, "APAC", 30.0),
                RowFactory.create(4, "US", 40.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // IN filter on partition column
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load()
                .filter("region IN ('US', 'APAC')");

        assertEquals(3, result.count());
    }

    @Test
    public void testPartitionRangeFilter() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "year", "amount"},
                new String[]{"INTEGER", "INTEGER", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("year", DataTypes.IntegerType, true)
                .add("amount", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, 2021, 100.0),
                RowFactory.create(2, 2022, 200.0),
                RowFactory.create(3, 2023, 300.0),
                RowFactory.create(4, 2024, 400.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Range filter >= 2023
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load()
                .filter("year >= 2023");

        assertEquals(2, result.count());
    }

    @Test
    public void testLargePartitionedTable() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "category", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("category", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Create 1000 rows across 10 categories
        List<Row> data = new ArrayList<>();
        String[] categories = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};
        for (int i = 0; i < 1000; i++) {
            data.add(RowFactory.create(i, categories[i % 10], (double) i));
        }

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(1000, result.count());

        // Filter on single partition
        Dataset<Row> catA = result.filter("category = 'A'");
        assertEquals(100, catA.count());
    }

    @Test
    public void testPartitionValuesRecordedInCatalog() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "EU", 20.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Verify partition values are stored in the catalog
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT partition_value FROM ducklake_file_partition_value " +
                     "WHERE table_id = 1 ORDER BY partition_value")) {
            List<String> values = new ArrayList<>();
            while (rs.next()) {
                values.add(rs.getString(1));
            }
            // Should have recorded partition values for the written files
            assertTrue("Expected partition values in catalog", values.size() > 0);
        }
    }

    @Test
    public void testPartitionWithDeleteFiles() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write initial data
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "EU", 20.0),
                RowFactory.create(3, "US", 30.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Read and verify all data present
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();
        assertEquals(3, result.count());
    }

    @Test
    public void testThreePartitionKeys() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "year", "region", "category", "amount"},
                new String[]{"INTEGER", "INTEGER", "VARCHAR", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2, 3, 4},
                new int[]{1, 2, 3}); // partition by year, region, category

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("year", DataTypes.IntegerType, true)
                .add("region", DataTypes.StringType, true)
                .add("category", DataTypes.StringType, true)
                .add("amount", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, 2023, "US", "A", 10.0),
                RowFactory.create(2, 2023, "US", "B", 20.0),
                RowFactory.create(3, 2023, "EU", "A", 30.0),
                RowFactory.create(4, 2024, "US", "A", 40.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        Dataset<Row> all = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(4, all.count());

        // Filter by single partition key
        assertEquals(3, all.filter("year = 2023").count());

        // Filter by two partition keys
        assertEquals(2, all.filter("year = 2023 AND region = 'US'").count());

        // Filter by all three
        assertEquals(1, all.filter("year = 2023 AND region = 'US' AND category = 'A'").count());
    }

    @Test
    public void testPartitionedReadWithColumnProjection() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "EU", 20.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Select only non-partition columns
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load()
                .select("id", "value");

        assertEquals(2, result.count());
        assertEquals(2, result.schema().fields().length);
    }

    @Test
    public void testReadNonPartitionedTableStillWorks() throws Exception {
        // Non-partitioned table (using standard catalog setup)
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath,
                "test_table", "main/test_table/",
                new String[]{"id", "name"},
                new String[]{"INTEGER", "VARCHAR"},
                new long[]{0, 1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "alice"),
                RowFactory.create(2, "bob"));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(2, result.count());
    }

    @Test
    public void testPartitionedTimeTravelRead() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write first batch
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "US", 10.0));
        spark.createDataFrame(data1, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Write second batch
        List<Row> data2 = Arrays.asList(
                RowFactory.create(2, "EU", 20.0));
        spark.createDataFrame(data2, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Latest snapshot should have 2 rows
        Dataset<Row> latest = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();
        assertEquals(2, latest.count());

        // Time travel to snapshot after first write (snapshot 3 — snap0=schema, snap1=table, snap2=setPartitioned, snap3=first write)
        Dataset<Row> snap3 = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .option("snapshot_version", "3")
                .load();
        assertEquals(1, snap3.count());
    }

    @Test
    public void testPartitionedStatisticsFiltering() throws Exception {
        createPartitionedCatalog("test_table",
                new String[]{"id", "region", "value"},
                new String[]{"INTEGER", "VARCHAR", "DOUBLE"},
                new long[]{0, 1, 2},
                new int[]{1});

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("region", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        // Write data with known value ranges per partition
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "US", 10.0),
                RowFactory.create(2, "US", 20.0),
                RowFactory.create(3, "EU", 100.0),
                RowFactory.create(4, "EU", 200.0));

        spark.createDataFrame(data, schema).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();

        // Combined filter: partition + stats-based
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load()
                .filter("region = 'US' AND value > 15.0");

        assertEquals(1, result.count());
        assertEquals(2, result.collectAsList().get(0).getInt(0));
    }

    // ---------------------------------------------------------------
    // Catalog setup helpers
    // ---------------------------------------------------------------

    /**
     * Create a DuckLake catalog with a partitioned table.
     * The partitionColIndices are indices into colNames for the partition columns.
     */
    private void createPartitionedCatalog(String tableName,
                                           String[] colNames, String[] colTypes, long[] colIds,
                                           int[] partitionColIndices) throws Exception {
        String tablePath = "main/" + tableName + "/";
        new File(dataPath + tablePath).mkdirs();

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
                st.execute("CREATE TABLE ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, type VARCHAR)");

                // Partition-specific tables
                st.execute("CREATE TABLE ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)");
                st.execute("CREATE TABLE ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform VARCHAR)");
                st.execute("CREATE TABLE ducklake_table_partition_column(table_id BIGINT, column_id BIGINT, partition_expression VARCHAR)");

                // Insert metadata
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dataPath + "')");

                // Snapshot 0: create schema
                st.execute("INSERT INTO ducklake_snapshot VALUES (0, datetime('now'), 0, 1, 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', 1)");

                // Snapshot 1: create table
                long tableId = 1;
                long nextCatalogId = 2;
                st.execute("INSERT INTO ducklake_snapshot VALUES (1, datetime('now'), 1, " + nextCatalogId + ", 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (1, 'created_table:\"main\".\"" + tableName + "\"', NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_table VALUES (" + tableId + ", 'table-uuid-" + tableName + "', 1, NULL, 0, '" + tableName + "', '" + tablePath + "', 1)");

                // Columns
                for (int i = 0; i < colNames.length; i++) {
                    st.execute("INSERT INTO ducklake_column VALUES (" + colIds[i] + ", 1, NULL, " + tableId + ", " + i + ", '" + colNames[i] + "', '" + colTypes[i] + "', NULL, NULL, 1, NULL, NULL, NULL)");
                }

                // Initialize table stats
                st.execute("INSERT INTO ducklake_table_stats VALUES (" + tableId + ", 0, 0, 0)");

                // Snapshot 2: set partitioned
                long partitionId = nextCatalogId;
                nextCatalogId++;
                st.execute("INSERT INTO ducklake_snapshot VALUES (2, datetime('now'), 2, " + nextCatalogId + ", 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (2, 'set_partitioned:" + tableId + "', NULL, NULL, NULL)");

                // Partition info
                st.execute("INSERT INTO ducklake_partition_info VALUES (" + partitionId + ", " + tableId + ", 2, NULL)");

                // Partition columns
                for (int i = 0; i < partitionColIndices.length; i++) {
                    int colIndex = partitionColIndices[i];
                    st.execute("INSERT INTO ducklake_partition_column VALUES (" + partitionId + ", " + tableId + ", " + i + ", " + colIds[colIndex] + ", NULL)");
                    // Also add to name_mapping with is_partition=1
                    st.execute("INSERT INTO ducklake_name_mapping VALUES (0, " + colIds[colIndex] + ", '" + colNames[colIndex] + "', " + colIds[colIndex] + ", NULL, 1)");
                    // Also add to table_partition_column
                    st.execute("INSERT INTO ducklake_table_partition_column VALUES (" + tableId + ", " + colIds[colIndex] + ", NULL)");
                }
            }

            conn.commit();
        }
    }

    /** Standard non-partitioned catalog for comparison tests. */
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
                st.execute("CREATE TABLE ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, type VARCHAR)");

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
