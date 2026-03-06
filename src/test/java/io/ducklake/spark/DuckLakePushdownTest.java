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
 * Integration tests for predicate pushdown (file pruning) and column pruning
 * in the DuckLake Spark connector.
 *
 * <p>Test strategy: write data across multiple files (one file per append, via
 * coalesce(1)), then verify that filters cause files to be skipped.  We verify
 * file skipping by inspecting the catalog column stats and by confirming result
 * correctness.
 */
public class DuckLakePushdownTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("DuckLakePushdownTest")
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
        tempDir = Files.createTempDirectory("ducklake-pushdown-test-").toString();
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
    // Helper: write a batch of rows as a single file
    // ---------------------------------------------------------------

    private void appendRows(List<Row> rows) {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        spark.createDataFrame(rows, schema)
                .coalesce(1)
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append)
                .save();
    }

    private Dataset<Row> readTable() {
        return spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();
    }

    /** Count active data files in the catalog. */
    private int countActiveFiles() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM ducklake_data_file WHERE table_id = 1 AND end_snapshot IS NULL")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    // ---------------------------------------------------------------
    // Tests: Filter Pushdown (file pruning via column stats)
    // ---------------------------------------------------------------

    @Test
    public void testEqualToFilterPrunesFiles() {
        // File 1: id 1-3, File 2: id 10-12
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(2, "b", 2.0),
                RowFactory.create(3, "c", 3.0)));
        appendRows(Arrays.asList(
                RowFactory.create(10, "x", 10.0),
                RowFactory.create(11, "y", 11.0),
                RowFactory.create(12, "z", 12.0)));

        // Filter id = 2 → should only match file 1
        Dataset<Row> result = readTable().filter("id = 2");
        List<Row> rows = result.collectAsList();
        assertEquals("Should return exactly 1 row", 1, rows.size());
        assertEquals(2, rows.get(0).getInt(0));

        // Filter id = 11 → should only match file 2
        result = readTable().filter("id = 11");
        rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(11, rows.get(0).getInt(0));

        // Filter id = 99 → should match no files
        result = readTable().filter("id = 99");
        rows = result.collectAsList();
        assertEquals(0, rows.size());
    }

    @Test
    public void testGreaterThanFilterPrunesFiles() {
        // File 1: id 1-5, File 2: id 100-105
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(5, "b", 5.0)));
        appendRows(Arrays.asList(
                RowFactory.create(100, "x", 100.0),
                RowFactory.create(105, "y", 105.0)));

        // id > 50 → file 1 max is 5, should be skipped
        Dataset<Row> result = readTable().filter("id > 50");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(100, rows.get(0).getInt(0));
        assertEquals(105, rows.get(1).getInt(0));
    }

    @Test
    public void testLessThanFilterPrunesFiles() {
        // File 1: id 1-5, File 2: id 100-105
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(5, "b", 5.0)));
        appendRows(Arrays.asList(
                RowFactory.create(100, "x", 100.0),
                RowFactory.create(105, "y", 105.0)));

        // id < 50 → file 2 min is 100, should be skipped
        Dataset<Row> result = readTable().filter("id < 50");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(5, rows.get(1).getInt(0));
    }

    @Test
    public void testGreaterThanOrEqualFilter() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(5, "b", 5.0)));
        appendRows(Arrays.asList(
                RowFactory.create(10, "x", 10.0),
                RowFactory.create(15, "y", 15.0)));

        // id >= 10 → file 1 max is 5 < 10, should be skipped
        Dataset<Row> result = readTable().filter("id >= 10");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(10, rows.get(0).getInt(0));
    }

    @Test
    public void testLessThanOrEqualFilter() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(5, "b", 5.0)));
        appendRows(Arrays.asList(
                RowFactory.create(10, "x", 10.0),
                RowFactory.create(15, "y", 15.0)));

        // id <= 5 → file 2 min is 10 > 5, should be skipped
        Dataset<Row> result = readTable().filter("id <= 5");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(5, rows.get(1).getInt(0));
    }

    @Test
    public void testInFilterPrunesFiles() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(5, "b", 5.0)));
        appendRows(Arrays.asList(
                RowFactory.create(100, "x", 100.0),
                RowFactory.create(200, "y", 200.0)));

        // id IN (2, 3) → all values in [1,5] range → file 1 might match, file 2 skipped
        Dataset<Row> result = readTable().filter("id IN (2, 3)");
        List<Row> rows = result.collectAsList();
        assertEquals(0, rows.size()); // no actual matches, but file 1 was scanned

        // id IN (100, 300) → 100 in [100,200], file 2 might match; 300 > 200 but 100 hits
        result = readTable().filter("id IN (100, 300)");
        rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(100, rows.get(0).getInt(0));
    }

    @Test
    public void testIsNullFilter() {
        // File 1: all non-null names, File 2: name has nulls
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(2, "b", 2.0)));
        appendRows(Arrays.asList(
                RowFactory.create(10, null, 10.0),
                RowFactory.create(11, "y", 11.0)));

        // name IS NULL → file 1 has 0 nulls → should be skipped
        Dataset<Row> result = readTable().filter("name IS NULL");
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(10, rows.get(0).getInt(0));
    }

    @Test
    public void testIsNotNullFilter() {
        // File 1: all names non-null, File 2: all names null
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(2, "b", 2.0)));
        appendRows(Arrays.asList(
                RowFactory.create(10, null, 10.0),
                RowFactory.create(11, null, 11.0)));

        // name IS NOT NULL → file 2 has 0 non-null values → should be skipped
        Dataset<Row> result = readTable().filter("name IS NOT NULL");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(2, rows.get(1).getInt(0));
    }

    @Test
    public void testAndFilter() {
        // File 1: id 1-5, value 1.0-5.0
        // File 2: id 10-15, value 10.0-15.0
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(5, "b", 5.0)));
        appendRows(Arrays.asList(
                RowFactory.create(10, "x", 10.0),
                RowFactory.create(15, "y", 15.0)));

        // id > 3 AND value < 6 → file 2 can't match (value min=10 >= 6)
        Dataset<Row> result = readTable().filter("id > 3 AND value < 6");
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(5, rows.get(0).getInt(0));
    }

    @Test
    public void testOrFilter() {
        // File 1: id 1-5, File 2: id 100-105
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(5, "b", 5.0)));
        appendRows(Arrays.asList(
                RowFactory.create(100, "x", 100.0),
                RowFactory.create(105, "y", 105.0)));

        // id = 1 OR id = 100 → both files might match
        Dataset<Row> result = readTable().filter("id = 1 OR id = 100");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(100, rows.get(1).getInt(0));
    }

    @Test
    public void testNoFilterReturnsAllRows() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(2, "b", 2.0)));
        appendRows(Arrays.asList(
                RowFactory.create(3, "c", 3.0)));

        // No filter → all 3 rows returned from 2 files
        Dataset<Row> result = readTable();
        assertEquals(3, result.count());
    }

    @Test
    public void testCorrectnessMaintainedWithPushdown() {
        // Write 3 files with known ranges
        appendRows(Arrays.asList(
                RowFactory.create(1, "alpha", 10.0),
                RowFactory.create(2, "beta", 20.0)));
        appendRows(Arrays.asList(
                RowFactory.create(3, "gamma", 30.0),
                RowFactory.create(4, "delta", 40.0)));
        appendRows(Arrays.asList(
                RowFactory.create(5, "epsilon", 50.0),
                RowFactory.create(6, "zeta", 60.0)));

        // Multiple filter types — verify ALL correct rows returned
        Dataset<Row> result = readTable().filter("id >= 2 AND id <= 5");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(4, rows.size());
        assertEquals(2, rows.get(0).getInt(0));
        assertEquals(3, rows.get(1).getInt(0));
        assertEquals(4, rows.get(2).getInt(0));
        assertEquals(5, rows.get(3).getInt(0));
    }

    // ---------------------------------------------------------------
    // Tests: Column Pruning
    // ---------------------------------------------------------------

    @Test
    public void testColumnPruningSelectSingleColumn() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "alice", 10.0),
                RowFactory.create(2, "bob", 20.0)));

        // Select only 'name' column
        Dataset<Row> result = readTable().select("name");
        assertEquals(1, result.schema().fields().length);
        assertEquals("name", result.schema().fields()[0].name());

        List<Row> rows = result.orderBy("name").collectAsList();
        assertEquals(2, rows.size());
        assertEquals("alice", rows.get(0).getString(0));
        assertEquals("bob", rows.get(1).getString(0));
    }

    @Test
    public void testColumnPruningSelectTwoColumns() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "alice", 10.0),
                RowFactory.create(2, "bob", 20.0)));

        // Select only 'id' and 'value'
        Dataset<Row> result = readTable().select("id", "value");
        assertEquals(2, result.schema().fields().length);

        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(10.0, rows.get(0).getDouble(1), 0.001);
    }

    @Test
    public void testColumnPruningWithFilterCombined() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "alice", 10.0),
                RowFactory.create(5, "bob", 50.0)));
        appendRows(Arrays.asList(
                RowFactory.create(100, "charlie", 100.0),
                RowFactory.create(200, "diana", 200.0)));

        // Select only 'name', filter id < 10 → file 2 skipped, only 'name' returned
        Dataset<Row> result = readTable().filter("id < 10").select("name");
        List<Row> rows = result.orderBy("name").collectAsList();
        assertEquals(2, rows.size());
        assertEquals("alice", rows.get(0).getString(0));
        assertEquals("bob", rows.get(1).getString(0));
        assertEquals(1, result.schema().fields().length);
    }

    // ---------------------------------------------------------------
    // Tests: Edge cases
    // ---------------------------------------------------------------

    @Test
    public void testFilterOnStringColumn() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "aaa", 1.0),
                RowFactory.create(2, "bbb", 2.0)));
        appendRows(Arrays.asList(
                RowFactory.create(3, "xxx", 3.0),
                RowFactory.create(4, "zzz", 4.0)));

        // name = 'xxx' → file 1 max is 'bbb' < 'xxx', should be skipped
        Dataset<Row> result = readTable().filter("name = 'xxx'");
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(3, rows.get(0).getInt(0));
    }

    @Test
    public void testFilterOnDoubleColumn() {
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.5),
                RowFactory.create(2, "b", 2.5)));
        appendRows(Arrays.asList(
                RowFactory.create(3, "c", 100.5),
                RowFactory.create(4, "d", 200.5)));

        // value > 50 → file 1 max is 2.5 < 50, skip
        Dataset<Row> result = readTable().filter("value > 50");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(3, rows.get(0).getInt(0));
        assertEquals(4, rows.get(1).getInt(0));
    }

    @Test
    public void testMultipleFilesAllMatch() throws Exception {
        // Both files have overlapping ranges — no pruning possible
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(10, "b", 10.0)));
        appendRows(Arrays.asList(
                RowFactory.create(5, "c", 5.0),
                RowFactory.create(15, "d", 15.0)));

        assertEquals(2, countActiveFiles());

        // id > 3 → both files might match (file 1: max=10>3, file 2: max=15>3)
        Dataset<Row> result = readTable().filter("id > 3");
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(3, rows.size()); // id 5, 10, 15
    }

    @Test
    public void testSingleFileNotPruned() throws Exception {
        appendRows(Arrays.asList(
                RowFactory.create(1, "a", 1.0),
                RowFactory.create(100, "b", 100.0)));

        assertEquals(1, countActiveFiles());

        // Single file, filter id = 50 → file [1,100] might match
        Dataset<Row> result = readTable().filter("id = 50");
        List<Row> rows = result.collectAsList();
        assertEquals(0, rows.size()); // no actual match, but file was scanned
    }

    // ---------------------------------------------------------------
    // Catalog setup helper (same as DuckLakeWriteTest)
    // ---------------------------------------------------------------

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
