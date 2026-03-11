package io.ducklake.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests for data inlining: storing small writes directly in the catalog
 * database instead of creating Parquet files.
 */
public class DuckLakeInliningTest {

    private SparkSession spark;
    private String tempDir;
    private String catalogPath;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-inline-test-").toString();
        catalogPath = tempDir + "/test.ducklake";

        // Bootstrap a DuckLake catalog using DuckDB JDBC
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                "jdbc:duckdb:")) {
            conn.createStatement().execute(
                    "INSTALL ducklake; LOAD ducklake;");
            conn.createStatement().execute(
                    "ATTACH 'ducklake:" + catalogPath +
                    "?data_path=" + tempDir + "/data/' AS dl;");
            conn.createStatement().execute("CREATE TABLE dl.main.test_inline (id INTEGER, name VARCHAR)");
            conn.createStatement().execute("DETACH dl");
        }

        spark = SparkSession.builder()
                .master("local[*]")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog", catalogPath)
                .config("spark.sql.catalog.ducklake.data_path", tempDir + "/data/")
                .config("spark.sql.catalog.ducklake.inline.row_limit", "100")
                .getOrCreate();
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
        deleteDir(new File(tempDir));
    }

    @Test
    public void testSmallWriteCreatesNoParquetFiles() {
        // Insert a very small dataset (3 rows) with inlining enabled
        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "alice"),
                        RowFactory.create(2, "bob"),
                        RowFactory.create(3, "charlie")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("name", DataTypes.StringType)
        );

        df.writeTo("ducklake.main.test_inline").append();

        // Verify data is readable
        Dataset<Row> result = spark.table("ducklake.main.test_inline");
        assertEquals(3, result.count());
    }

    @Test
    public void testInlinedDataReadBack() {
        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(10, "inline_test"),
                        RowFactory.create(20, "inline_read")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("name", DataTypes.StringType)
        );

        df.writeTo("ducklake.main.test_inline").append();

        Dataset<Row> result = spark.table("ducklake.main.test_inline");
        List<Row> rows = result.sort("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(10, rows.get(0).getInt(0));
        assertEquals("inline_test", rows.get(0).getString(1));
        assertEquals(20, rows.get(1).getInt(0));
        assertEquals("inline_read", rows.get(1).getString(1));
    }

    @Test
    public void testOverwriteWithInlinedData() {
        // First write
        Dataset<Row> df1 = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "old_data")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("name", DataTypes.StringType)
        );
        df1.writeTo("ducklake.main.test_inline").append();

        // Overwrite
        Dataset<Row> df2 = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(99, "new_data")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("name", DataTypes.StringType)
        );
        df2.writeTo("ducklake.main.test_inline").overwrite(functions.lit(true));

        Dataset<Row> result = spark.table("ducklake.main.test_inline");
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(99, rows.get(0).getInt(0));
        assertEquals("new_data", rows.get(0).getString(1));
    }

    @Test
    public void testMultipleInlinedWrites() {
        for (int batch = 0; batch < 5; batch++) {
            Dataset<Row> df = spark.createDataFrame(
                    Arrays.asList(
                            RowFactory.create(batch * 10, "batch_" + batch)
                    ),
                    new StructType()
                            .add("id", DataTypes.IntegerType)
                            .add("name", DataTypes.StringType)
            );
            df.writeTo("ducklake.main.test_inline").append();
        }

        Dataset<Row> result = spark.table("ducklake.main.test_inline");
        assertEquals(5, result.count());
    }

    // ---------------------------------------------------------------

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteDir(f);
                }
            }
        }
        dir.delete();
    }
}
