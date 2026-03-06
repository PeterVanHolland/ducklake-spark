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
 * Integration tests for schema evolution on read.
 * Verifies that the reader correctly handles:
 * - Columns added after files were written (fill with defaults)
 * - Columns dropped (excluded from output)
 * - Columns renamed (matched by field_id)
 * - Time travel with schema changes
 */
public class DuckLakeSchemaEvolutionTest {

    private static SparkSession spark;
    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @BeforeClass
    public static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeSchemaEvolutionTest")
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
        tempDir = Files.createTempDirectory("ducklake-schema-evo-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
    }

    @After
    public void cleanup() throws Exception {
        deleteRecursive(new File(tempDir));
    }

    // ---------------------------------------------------------------
    // Test 1: Add column with default value
    // ---------------------------------------------------------------

    @Test
    public void testAddColumnWithDefault() throws Exception {
        catalogPath = tempDir + "/add_col.ducklake";
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath, "test_table", "main/test_table/",
                new String[]{"id", "name"},
                new String[]{"INTEGER", "VARCHAR"},
                new long[]{10, 11});

        // Write batch 1 with 2 columns
        StructType schema1 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true);
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "alice"),
                RowFactory.create(2, "bob"));
        spark.createDataFrame(data1, schema1).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Add column 'score' (col_id=12) with initial_default='0.0'
        addColumn(catalogPath, 1, 12, "score", "DOUBLE", 2, "0.0");

        // Write batch 2 with 3 columns
        StructType schema2 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("score", DataTypes.DoubleType, true);
        List<Row> data2 = Arrays.asList(
                RowFactory.create(3, "charlie", 95.5),
                RowFactory.create(4, "diana", 88.0));
        spark.createDataFrame(data2, schema2).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Read all -- old rows should have score=0.0 (default)
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(4, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();

        // Old rows: score = 0.0 (initialDefault)
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals(0.0, rows.get(0).getDouble(2), 0.001);

        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("bob", rows.get(1).getString(1));
        assertEquals(0.0, rows.get(1).getDouble(2), 0.001);

        // New rows: actual values
        assertEquals(3, rows.get(2).getInt(0));
        assertEquals("charlie", rows.get(2).getString(1));
        assertEquals(95.5, rows.get(2).getDouble(2), 0.001);

        assertEquals(4, rows.get(3).getInt(0));
        assertEquals("diana", rows.get(3).getString(1));
        assertEquals(88.0, rows.get(3).getDouble(2), 0.001);
    }

    // ---------------------------------------------------------------
    // Test 2: Add column without default (null for old files)
    // ---------------------------------------------------------------

    @Test
    public void testAddColumnWithoutDefault() throws Exception {
        catalogPath = tempDir + "/add_col_null.ducklake";
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath, "test_table", "main/test_table/",
                new String[]{"id", "value"},
                new String[]{"INTEGER", "DOUBLE"},
                new long[]{20, 21});

        // Write batch 1
        StructType schema1 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("value", DataTypes.DoubleType, true);
        spark.createDataFrame(
                Arrays.asList(RowFactory.create(1, 10.0), RowFactory.create(2, 20.0)),
                schema1).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Add column 'label' with no default
        addColumn(catalogPath, 1, 22, "label", "VARCHAR", 2, null);

        // Write batch 2 with new column
        StructType schema2 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("value", DataTypes.DoubleType, true)
                .add("label", DataTypes.StringType, true);
        spark.createDataFrame(
                Arrays.asList(RowFactory.create(3, 30.0, "new")),
                schema2).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(3, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();

        // Old rows: label is null
        assertTrue(rows.get(0).isNullAt(2));
        assertTrue(rows.get(1).isNullAt(2));

        // New row: has value
        assertEquals("new", rows.get(2).getString(2));
    }

    // ---------------------------------------------------------------
    // Test 3: Drop column -- excluded from output
    // ---------------------------------------------------------------

    @Test
    public void testDropColumn() throws Exception {
        catalogPath = tempDir + "/drop_col.ducklake";
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath, "test_table", "main/test_table/",
                new String[]{"id", "name", "extra"},
                new String[]{"INTEGER", "VARCHAR", "VARCHAR"},
                new long[]{30, 31, 32});

        // Write with 3 columns
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("extra", DataTypes.StringType, true);
        spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "alice", "x"),
                        RowFactory.create(2, "bob", "y")),
                schema).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Drop 'extra' column
        dropColumn(catalogPath, 1, 32);

        // Read -- should only have id and name
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(2, result.count());
        assertEquals(2, result.schema().fields().length);
        assertEquals("id", result.schema().fields()[0].name());
        assertEquals("name", result.schema().fields()[1].name());

        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("bob", rows.get(1).getString(1));
    }

    // ---------------------------------------------------------------
    // Test 4: Rename column -- matched by field_id
    // ---------------------------------------------------------------

    @Test
    public void testRenameColumn() throws Exception {
        catalogPath = tempDir + "/rename_col.ducklake";
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath, "test_table", "main/test_table/",
                new String[]{"id", "old_name"},
                new String[]{"INTEGER", "VARCHAR"},
                new long[]{40, 41});

        // Write with column named 'old_name'
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("old_name", DataTypes.StringType, true);
        spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "alice"),
                        RowFactory.create(2, "bob")),
                schema).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Rename 'old_name' -> 'new_name' (same column_id=41)
        renameColumn(catalogPath, 1, 41, "old_name", "new_name");

        // Read -- column should appear as 'new_name' with correct values
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(2, result.count());
        assertEquals("new_name", result.schema().fields()[1].name());

        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals("bob", rows.get(1).getString(1));
    }

    // ---------------------------------------------------------------
    // Test 5: Time travel with schema evolution
    // ---------------------------------------------------------------

    @Test
    public void testTimeTravelWithSchemaEvolution() throws Exception {
        catalogPath = tempDir + "/timetravel_evo.ducklake";
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath, "test_table", "main/test_table/",
                new String[]{"id", "name"},
                new String[]{"INTEGER", "VARCHAR"},
                new long[]{50, 51});

        // Write batch 1 (schema v1: id, name)
        StructType schema1 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true);
        spark.createDataFrame(
                Arrays.asList(RowFactory.create(1, "alice")),
                schema1).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Record snapshot after first write
        long snapshotAfterWrite1;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
            snapshotAfterWrite1 = rs.getLong(1);
        }

        // Add column 'score' with default 0.0
        addColumn(catalogPath, 1, 52, "score", "DOUBLE", 2, "0.0");

        // Write batch 2 with new column
        StructType schema2 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("score", DataTypes.DoubleType, true);
        spark.createDataFrame(
                Arrays.asList(RowFactory.create(2, "bob", 77.0)),
                schema2).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Read latest: 2 rows, 3 columns
        Dataset<Row> latest = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(2, latest.count());
        assertEquals(3, latest.schema().fields().length);

        // Time travel to before column was added
        Dataset<Row> old = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .option("snapshot_version", String.valueOf(snapshotAfterWrite1))
                .load();

        assertEquals(1, old.count());
        assertEquals(2, old.schema().fields().length);
        assertEquals("id", old.schema().fields()[0].name());
        assertEquals("name", old.schema().fields()[1].name());

        List<Row> oldRows = old.collectAsList();
        assertEquals(1, oldRows.get(0).getInt(0));
        assertEquals("alice", oldRows.get(0).getString(1));
    }

    // ---------------------------------------------------------------
    // Test 6: String default value
    // ---------------------------------------------------------------

    @Test
    public void testStringDefaultValue() throws Exception {
        catalogPath = tempDir + "/string_default.ducklake";
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath, "test_table", "main/test_table/",
                new String[]{"id"},
                new String[]{"INTEGER"},
                new long[]{60});

        // Write batch 1
        StructType schema1 = new StructType()
                .add("id", DataTypes.IntegerType, false);
        spark.createDataFrame(
                Arrays.asList(RowFactory.create(1), RowFactory.create(2)),
                schema1).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Add string column with default
        addColumn(catalogPath, 1, 61, "status", "VARCHAR", 1, "active");

        // Read -- old rows should have status='active'
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(2, result.count());
        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals("active", rows.get(0).getString(1));
        assertEquals("active", rows.get(1).getString(1));
    }

    // ---------------------------------------------------------------
    // Test 7: Integer default value
    // ---------------------------------------------------------------

    @Test
    public void testIntegerDefaultValue() throws Exception {
        catalogPath = tempDir + "/int_default.ducklake";
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath, "test_table", "main/test_table/",
                new String[]{"name"},
                new String[]{"VARCHAR"},
                new long[]{70});

        // Write batch 1
        StructType schema1 = new StructType()
                .add("name", DataTypes.StringType, true);
        spark.createDataFrame(
                Arrays.asList(RowFactory.create("alice")),
                schema1).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Add integer column with default 42
        addColumn(catalogPath, 1, 71, "priority", "INTEGER", 1, "42");

        // Read -- old rows should have priority=42
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(42, rows.get(0).getInt(1));
    }

    // ---------------------------------------------------------------
    // Test 8: Add column then drop it -- complex evolution
    // ---------------------------------------------------------------

    @Test
    public void testAddThenDropColumn() throws Exception {
        catalogPath = tempDir + "/add_drop.ducklake";
        new File(dataPath + "main/test_table/").mkdirs();
        createCatalog(catalogPath, dataPath, "test_table", "main/test_table/",
                new String[]{"id", "name"},
                new String[]{"INTEGER", "VARCHAR"},
                new long[]{80, 81});

        // Write batch 1
        StructType schema1 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true);
        spark.createDataFrame(
                Arrays.asList(RowFactory.create(1, "alice")),
                schema1).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Add 'score' column
        addColumn(catalogPath, 1, 82, "score", "DOUBLE", 2, null);

        // Write batch 2 with score
        StructType schema2 = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("score", DataTypes.DoubleType, true);
        spark.createDataFrame(
                Arrays.asList(RowFactory.create(2, "bob", 90.0)),
                schema2).coalesce(1).write()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .mode(SaveMode.Append).save();

        // Drop 'score' column
        dropColumn(catalogPath, 1, 82);

        // Read -- should only have id and name
        Dataset<Row> result = spark.read()
                .format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath)
                .option("table", "test_table")
                .load();

        assertEquals(2, result.count());
        assertEquals(2, result.schema().fields().length);

        List<Row> rows = result.orderBy("id").collectAsList();
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals("bob", rows.get(1).getString(1));
    }

    // ---------------------------------------------------------------
    // Catalog setup and DDL helpers
    // ---------------------------------------------------------------

    private void createCatalog(String catPath, String dp, String tableName, String tablePath,
                               String[] colNames, String[] colTypes, long[] colIds) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catPath)) {
            conn.setAutoCommit(false);

            try (Statement st = conn.createStatement()) {
                // Core metadata tables
                st.execute("CREATE TABLE ducklake_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT)");
                st.execute("CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TEXT, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT, snapshot_changes TEXT)");
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

                // Insert metadata
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dp + "')");

                // Compute next_catalog_id
                long maxId = 1; // table_id = 1
                for (long id : colIds) {
                    if (id > maxId) maxId = id;
                }
                long nextCatalogId = maxId + 1;

                // Snapshot 0: create schema
                st.execute("INSERT INTO ducklake_snapshot VALUES (0, datetime('now'), 0, 1, 0, NULL)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:main', NULL, NULL, NULL)");

                // Schema
                st.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', 1)");

                // Snapshot 1: create table
                long tableId = 1;
                st.execute("INSERT INTO ducklake_snapshot VALUES (1, datetime('now'), 1, " + nextCatalogId + ", 0, NULL)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (1, 'created_table:" + tableName + "', NULL, NULL, NULL)");

                // Table
                st.execute("INSERT INTO ducklake_table VALUES (" + tableId + ", 'table-uuid-" + tableName + "', 1, NULL, 0, '" + tableName + "', '" + tablePath + "', 1)");

                // Columns
                for (int i = 0; i < colNames.length; i++) {
                    st.execute("INSERT INTO ducklake_column VALUES (" + colIds[i] + ", 1, NULL, " + tableId + ", " + i + ", '" + colNames[i] + "', '" + colTypes[i] + "', NULL, NULL, 1, NULL, NULL, NULL)");
                }

                // Initialize table stats
                st.execute("INSERT INTO ducklake_table_stats VALUES (" + tableId + ", 0, 0, 0)");
            }

            conn.commit();
        }
    }

    private void addColumn(String catPath, long tableId, long columnId,
                           String colName, String colType, int colOrder,
                           String initialDefault) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catPath)) {
            conn.setAutoCommit(false);

            long currentSnap;
            long schemaVersion;
            long nextCatalogId;
            long nextFileId;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1")) {
                rs.next();
                currentSnap = rs.getLong("snapshot_id");
                schemaVersion = rs.getLong("schema_version");
                nextCatalogId = rs.getLong("next_catalog_id");
                nextFileId = rs.getLong("next_file_id");
            }

            long newSnap = currentSnap + 1;
            long newNextCatalogId = Math.max(nextCatalogId, columnId + 1);

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_snapshot VALUES (?, datetime('now'), ?, ?, ?, NULL)")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, schemaVersion + 1);
                ps.setLong(3, newNextCatalogId);
                ps.setLong(4, nextFileId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_snapshot_changes VALUES (?, ?, 'test', ?, NULL)")) {
                ps.setLong(1, newSnap);
                ps.setString(2, "added_column:" + colName);
                ps.setString(3, "Add column " + colName);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_column VALUES (?, ?, NULL, ?, ?, ?, ?, ?, NULL, 1, NULL, NULL, NULL)")) {
                ps.setLong(1, columnId);
                ps.setLong(2, newSnap);
                ps.setLong(3, tableId);
                ps.setInt(4, colOrder);
                ps.setString(5, colName);
                ps.setString(6, colType);
                if (initialDefault != null) {
                    ps.setString(7, initialDefault);
                } else {
                    ps.setNull(7, java.sql.Types.VARCHAR);
                }
                ps.executeUpdate();
            }

            conn.commit();
        }
    }

    private void dropColumn(String catPath, long tableId, long columnId) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catPath)) {
            conn.setAutoCommit(false);

            long currentSnap;
            long schemaVersion;
            long nextCatalogId;
            long nextFileId;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1")) {
                rs.next();
                currentSnap = rs.getLong("snapshot_id");
                schemaVersion = rs.getLong("schema_version");
                nextCatalogId = rs.getLong("next_catalog_id");
                nextFileId = rs.getLong("next_file_id");
            }

            long newSnap = currentSnap + 1;
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_snapshot VALUES (?, datetime('now'), ?, ?, ?, NULL)")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, schemaVersion + 1);
                ps.setLong(3, nextCatalogId);
                ps.setLong(4, nextFileId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_snapshot_changes VALUES (?, ?, 'test', 'Drop column', NULL)")) {
                ps.setLong(1, newSnap);
                ps.setString(2, "dropped_column:" + columnId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? WHERE column_id = ? AND table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, columnId);
                ps.setLong(3, tableId);
                ps.executeUpdate();
            }

            conn.commit();
        }
    }

    private void renameColumn(String catPath, long tableId, long columnId,
                              String oldName, String newName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catPath)) {
            conn.setAutoCommit(false);

            long currentSnap;
            long schemaVersion;
            long nextCatalogId;
            long nextFileId;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1")) {
                rs.next();
                currentSnap = rs.getLong("snapshot_id");
                schemaVersion = rs.getLong("schema_version");
                nextCatalogId = rs.getLong("next_catalog_id");
                nextFileId = rs.getLong("next_file_id");
            }

            // Get column metadata
            String colType;
            int colOrder;
            String initialDefault;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT column_type, column_order, initial_default FROM ducklake_column " +
                    "WHERE column_id = ? AND table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, columnId);
                ps.setLong(2, tableId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    colType = rs.getString("column_type");
                    colOrder = rs.getInt("column_order");
                    initialDefault = rs.getString("initial_default");
                }
            }

            long newSnap = currentSnap + 1;
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_snapshot VALUES (?, datetime('now'), ?, ?, ?, NULL)")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, schemaVersion + 1);
                ps.setLong(3, nextCatalogId);
                ps.setLong(4, nextFileId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_snapshot_changes VALUES (?, ?, 'test', 'Rename column', NULL)")) {
                ps.setLong(1, newSnap);
                ps.setString(2, "renamed_column:" + oldName + "->" + newName);
                ps.executeUpdate();
            }

            // End old column record
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? WHERE column_id = ? AND table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, columnId);
                ps.setLong(3, tableId);
                ps.executeUpdate();
            }

            // Insert new column record with same column_id but new name
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_column VALUES (?, ?, NULL, ?, ?, ?, ?, ?, NULL, 1, NULL, NULL, NULL)")) {
                ps.setLong(1, columnId);
                ps.setLong(2, newSnap);
                ps.setLong(3, tableId);
                ps.setInt(4, colOrder);
                ps.setString(5, newName);
                ps.setString(6, colType);
                if (initialDefault != null) {
                    ps.setString(7, initialDefault);
                } else {
                    ps.setNull(7, java.sql.Types.VARCHAR);
                }
                ps.executeUpdate();
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
