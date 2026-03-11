package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.catalog.DuckLakeAddFiles;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests for add_files: registering existing Parquet files into DuckLake tables.
 */
public class DuckLakeAddFilesTest {

    private SparkSession spark;
    private String tempDir;
    private String catalogPath;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-addfiles-test-").toString();
        catalogPath = tempDir + "/test.ducklake";

        // Bootstrap catalog
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
            conn.createStatement().execute("INSTALL ducklake; LOAD ducklake;");
            conn.createStatement().execute(
                    "ATTACH 'ducklake:" + catalogPath +
                    "?data_path=" + tempDir + "/data/' AS dl;");
            conn.createStatement().execute("CREATE TABLE dl.main.target (id INTEGER, name VARCHAR)");
            conn.createStatement().execute("DETACH dl");
        }

        spark = SparkSession.builder()
                .master("local[*]")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog", catalogPath)
                .config("spark.sql.catalog.ducklake.data_path", tempDir + "/data/")
                .getOrCreate();
    }

    @After
    public void tearDown() {
        if (spark != null) spark.stop();
        deleteDir(new File(tempDir));
    }

    @Test
    public void testAddSingleFile() throws Exception {
        // Create a Parquet file externally
        String externalDir = tempDir + "/external/";
        new File(externalDir).mkdirs();
        String parquetPath = externalDir + "data.parquet";

        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "alice"),
                        RowFactory.create(2, "bob")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("name", DataTypes.StringType)
        );
        df.write().parquet(parquetPath);

        // Find the actual parquet file inside the directory
        File[] parquetFiles = new File(parquetPath).listFiles(
                (dir, name) -> name.endsWith(".parquet") && !name.startsWith("_") && !name.startsWith("."));
        assertNotNull(parquetFiles);
        assertTrue(parquetFiles.length > 0);

        // Add the file to the DuckLake table
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            TableInfo table = backend.getTable("target", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            DuckLakeAddFiles addFiles = new DuckLakeAddFiles(backend);
            long newSnap = addFiles.addFiles(table.tableId,
                    new String[]{ parquetFiles[0].getAbsolutePath() },
                    columns, false, tempDir + "/data/");

            assertTrue(newSnap > 0);
        }

        // Verify data is readable
        Dataset<Row> result = spark.table("ducklake.main.target");
        assertEquals(2, result.count());
    }

    @Test
    public void testAddMultipleFiles() throws Exception {
        String externalDir = tempDir + "/external_multi/";
        new File(externalDir).mkdirs();

        // Create two Parquet files
        for (int i = 0; i < 2; i++) {
            String path = externalDir + "part" + i + ".parquet";
            Dataset<Row> df = spark.createDataFrame(
                    Arrays.asList(
                            RowFactory.create(i * 10, "batch_" + i)
                    ),
                    new StructType()
                            .add("id", DataTypes.IntegerType)
                            .add("name", DataTypes.StringType)
            );
            df.coalesce(1).write().parquet(path);
        }

        // Collect actual parquet files
        List<String> allFiles = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            File dir = new File(externalDir + "part" + i + ".parquet");
            File[] files = dir.listFiles(
                    (d, name) -> name.endsWith(".parquet") && !name.startsWith("_") && !name.startsWith("."));
            if (files != null) {
                for (File f : files) allFiles.add(f.getAbsolutePath());
            }
        }
        assertFalse(allFiles.isEmpty());

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            TableInfo table = backend.getTable("target", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            DuckLakeAddFiles addFiles = new DuckLakeAddFiles(backend);
            addFiles.addFiles(table.tableId, allFiles.toArray(new String[0]),
                    columns, false, tempDir + "/data/");
        }

        Dataset<Row> result = spark.table("ducklake.main.target");
        assertEquals(2, result.count());
    }

    @Test(expected = java.io.IOException.class)
    public void testAddFileSchemaMismatch() throws Exception {
        String externalDir = tempDir + "/external_bad/";
        new File(externalDir).mkdirs();
        String parquetPath = externalDir + "bad.parquet";

        // Create file with different schema
        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(RowFactory.create("wrong", 42, true)),
                new StructType()
                        .add("x", DataTypes.StringType)
                        .add("y", DataTypes.IntegerType)
                        .add("z", DataTypes.BooleanType)
        );
        df.coalesce(1).write().parquet(parquetPath);

        File[] files = new File(parquetPath).listFiles(
                (d, name) -> name.endsWith(".parquet") && !name.startsWith("_") && !name.startsWith("."));

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            TableInfo table = backend.getTable("target", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            DuckLakeAddFiles addFiles = new DuckLakeAddFiles(backend);
            addFiles.addFiles(table.tableId, new String[]{ files[0].getAbsolutePath() },
                    columns, false, tempDir + "/data/");
        }
    }

    // ---------------------------------------------------------------

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
