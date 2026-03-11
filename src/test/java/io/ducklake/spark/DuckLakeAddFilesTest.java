package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.catalog.DuckLakeAddFiles;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests for add_files: registering existing Parquet files into DuckLake tables.
 */
public class DuckLakeAddFilesTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setUpSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeAddFilesTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDownSpark() {
        if (spark != null) spark.stop();
    }

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-addfiles-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        new File(dataPath + "main/target/").mkdirs();
        catalogPath = tempDir + "/test.ducklake";

        createCatalog(catalogPath, dataPath, "target", "main/target/",
                new String[]{"id", "name"},
                new String[]{"INTEGER", "VARCHAR"},
                new long[]{2, 3});
    }

    @After
    public void tearDown() {
        deleteDir(new File(tempDir));
    }

    @Test
    public void testAddSingleFile() throws Exception {
        // Create a Parquet file externally via Spark
        String externalDir = tempDir + "/external/";
        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "alice"),
                        RowFactory.create(2, "bob")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("name", DataTypes.StringType)
        );
        df.coalesce(1).write().parquet(externalDir);

        File[] parquetFiles = findParquetFiles(externalDir);
        assertNotNull(parquetFiles);
        assertTrue(parquetFiles.length > 0);

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            TableInfo table = backend.getTable("target", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            DuckLakeAddFiles addFiles = new DuckLakeAddFiles(backend);
            long newSnap = addFiles.addFiles(table.tableId,
                    new String[]{ parquetFiles[0].getAbsolutePath() },
                    columns, false, dataPath);

            assertTrue(newSnap > 0);
        }

        // Verify via DataSource read
        Dataset<Row> result = spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "target")
                .load();
        assertEquals(2, result.count());
    }

    @Test
    public void testAddMultipleFiles() throws Exception {
        String externalDir = tempDir + "/external_multi/";

        for (int i = 0; i < 2; i++) {
            String path = externalDir + "part" + i + "/";
            Dataset<Row> df = spark.createDataFrame(
                    Arrays.asList(RowFactory.create(i * 10, "batch_" + i)),
                    new StructType()
                            .add("id", DataTypes.IntegerType)
                            .add("name", DataTypes.StringType)
            );
            df.coalesce(1).write().parquet(path);
        }

        List<String> allFiles = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            File[] files = findParquetFiles(externalDir + "part" + i + "/");
            if (files != null) {
                for (File f : files) allFiles.add(f.getAbsolutePath());
            }
        }
        assertFalse(allFiles.isEmpty());

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            TableInfo table = backend.getTable("target", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            DuckLakeAddFiles addFiles = new DuckLakeAddFiles(backend);
            addFiles.addFiles(table.tableId, allFiles.toArray(new String[0]),
                    columns, false, dataPath);
        }

        Dataset<Row> result = spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "target")
                .load();
        assertEquals(2, result.count());
    }

    @Test(expected = java.io.IOException.class)
    public void testAddFileSchemaMismatch() throws Exception {
        String externalDir = tempDir + "/external_bad/";

        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(RowFactory.create("wrong", 42, true)),
                new StructType()
                        .add("x", DataTypes.StringType)
                        .add("y", DataTypes.IntegerType)
                        .add("z", DataTypes.BooleanType)
        );
        df.coalesce(1).write().parquet(externalDir);

        File[] files = findParquetFiles(externalDir);

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            TableInfo table = backend.getTable("target", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            DuckLakeAddFiles addFiles = new DuckLakeAddFiles(backend);
            addFiles.addFiles(table.tableId, new String[]{ files[0].getAbsolutePath() },
                    columns, false, dataPath);
        }
    }

    // ---------------------------------------------------------------

    private File[] findParquetFiles(String dir) {
        return new File(dir).listFiles(
                (d, name) -> name.endsWith(".parquet") && !name.startsWith("_") && !name.startsWith("."));
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
