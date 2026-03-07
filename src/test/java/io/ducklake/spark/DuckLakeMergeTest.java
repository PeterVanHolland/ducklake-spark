package io.ducklake.spark;

import io.ducklake.spark.maintenance.DuckLakeMerge;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests for DuckLake MERGE INTO operations.
 */
public class DuckLakeMergeTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-merge-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);

        Thread.currentThread().setContextClassLoader(
                DuckLakeMergeTest.class.getClassLoader());

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeMergeTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.catalog.ducklake",
                        "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog",
                        catalogPath)
                .config("spark.sql.catalog.ducklake.data_path",
                        dataPath)
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
    // Helper: create and populate target table
    // ---------------------------------------------------------------

    private void createAndPopulateTable(String tableName, int numRows) {
        spark.sql("CREATE TABLE IF NOT EXISTS ducklake.main."
                + tableName
                + " (id INT, name STRING, value DOUBLE)");

        StringBuilder values = new StringBuilder();
        for (int i = 1; i <= numRows; i++) {
            if (i > 1) values.append(", ");
            values.append("(").append(i)
                  .append(", 'name_").append(i)
                  .append("', ").append(i * 10.0)
                  .append(")");
        }
        spark.sql("INSERT INTO ducklake.main." + tableName
                + " VALUES " + values);
    }

    /**
     * Build a source DataFrame from rows of (id, name, value).
     */
    private Dataset<Row> createSourceDF(Object[]... rows) {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);

        List<Row> rowList = new ArrayList<>();
        for (Object[] r : rows) {
            rowList.add(RowFactory.create(r));
        }
        return spark.createDataFrame(rowList, schema);
    }

    // ---------------------------------------------------------------
    // Basic upsert: update matched, insert unmatched
    // ---------------------------------------------------------------

    @Test
    public void testBasicUpsert() {
        createAndPopulateTable("merge_upsert", 5);

        // Source: id=3 exists (update), id=10 is new (insert)
        Dataset<Row> source = createSourceDF(
                new Object[]{3, "updated_3", 300.0},
                new Object[]{10, "new_10", 100.0});

        DuckLakeMerge.into(spark, catalogPath, "merge_upsert", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_upsert ORDER BY id");
        List<Row> rows = result.collectAsList();

        // 5 original + 1 new = 6 total (id=3 updated, not duplicated)
        assertEquals(6, rows.size());

        // Verify id=3 was updated
        Row row3 = rows.get(2);
        assertEquals(3, row3.getInt(0));
        assertEquals("updated_3", row3.getString(1));
        assertEquals(300.0, row3.getDouble(2), 0.001);

        // Verify id=10 was inserted
        Row row10 = rows.get(5);
        assertEquals(10, row10.getInt(0));
        assertEquals("new_10", row10.getString(1));
        assertEquals(100.0, row10.getDouble(2), 0.001);

        // Verify other rows unchanged
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("name_1", rows.get(0).getString(1));
    }

    // ---------------------------------------------------------------
    // Delete matched rows
    // ---------------------------------------------------------------

    @Test
    public void testDeleteMatched() {
        createAndPopulateTable("merge_del", 5);

        // Source contains ids 2 and 4 — delete them from target
        Dataset<Row> source = createSourceDF(
                new Object[]{2, "x", 0.0},
                new Object[]{4, "x", 0.0});

        DuckLakeMerge.into(spark, catalogPath, "merge_del", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().delete()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_del ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(3, rows.get(1).getInt(0));
        assertEquals(5, rows.get(2).getInt(0));
    }

    // ---------------------------------------------------------------
    // Update with condition (WHEN MATCHED AND condition)
    // ---------------------------------------------------------------

    @Test
    public void testUpdateWithCondition() {
        createAndPopulateTable("merge_cond", 5);

        // Source: id=2 (value=200 > 100, should update),
        //         id=4 (value=50 <= 100, should NOT update)
        Dataset<Row> source = createSourceDF(
                new Object[]{2, "updated_2", 200.0},
                new Object[]{4, "updated_4", 50.0});

        DuckLakeMerge.into(spark, catalogPath, "merge_cond", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched("source.value > 100").updateAll()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_cond ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertEquals(5, rows.size());

        // id=2 should be updated (source.value=200 > 100)
        assertEquals("updated_2", rows.get(1).getString(1));
        assertEquals(200.0, rows.get(1).getDouble(2), 0.001);

        // id=4 should be unchanged (source.value=50 <= 100)
        assertEquals("name_4", rows.get(3).getString(1));
        assertEquals(40.0, rows.get(3).getDouble(2), 0.001);
    }

    // ---------------------------------------------------------------
    // Insert only (no matches)
    // ---------------------------------------------------------------

    @Test
    public void testInsertOnly() {
        createAndPopulateTable("merge_ins", 3);

        // Source has only new rows (no matching ids)
        Dataset<Row> source = createSourceDF(
                new Object[]{10, "new_10", 100.0},
                new Object[]{20, "new_20", 200.0});

        DuckLakeMerge.into(spark, catalogPath, "merge_ins", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_ins ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertEquals(5, rows.size());
        assertEquals(10, rows.get(3).getInt(0));
        assertEquals(20, rows.get(4).getInt(0));

        // Original rows unchanged
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, rows.get(i).getInt(0));
            assertEquals("name_" + (i + 1), rows.get(i).getString(1));
        }
    }

    // ---------------------------------------------------------------
    // Update only (all match)
    // ---------------------------------------------------------------

    @Test
    public void testUpdateOnly() {
        createAndPopulateTable("merge_upd", 3);

        // Source matches all target rows
        Dataset<Row> source = createSourceDF(
                new Object[]{1, "upd_1", 100.0},
                new Object[]{2, "upd_2", 200.0},
                new Object[]{3, "upd_3", 300.0});

        DuckLakeMerge.into(spark, catalogPath, "merge_upd", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().updateAll()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_upd ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertEquals(3, rows.size());
        assertEquals("upd_1", rows.get(0).getString(1));
        assertEquals("upd_2", rows.get(1).getString(1));
        assertEquals("upd_3", rows.get(2).getString(1));
        assertEquals(100.0, rows.get(0).getDouble(2), 0.001);
        assertEquals(200.0, rows.get(1).getDouble(2), 0.001);
        assertEquals(300.0, rows.get(2).getDouble(2), 0.001);
    }

    // ---------------------------------------------------------------
    // Multiple source rows matching same target row (error)
    // ---------------------------------------------------------------

    @Test
    public void testMultipleSourceMatchesFails() {
        createAndPopulateTable("merge_dup", 3);

        // Two source rows with the same id=2
        Dataset<Row> source = createSourceDF(
                new Object[]{2, "dup_a", 100.0},
                new Object[]{2, "dup_b", 200.0});

        try {
            DuckLakeMerge.into(spark, catalogPath, "merge_dup", "main")
                    .using(source, "source")
                    .on("target.id = source.id")
                    .whenMatched().updateAll()
                    .execute();
            fail("Should have thrown exception for duplicate source keys");
        } catch (RuntimeException e) {
            assertTrue("Error message should mention multiple source rows",
                    e.getMessage().contains("multiple source rows"));
        }
    }

    // ---------------------------------------------------------------
    // Empty source (no-op)
    // ---------------------------------------------------------------

    @Test
    public void testEmptySource() {
        createAndPopulateTable("merge_empty", 3);

        // Empty source DataFrame
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true);
        Dataset<Row> source = spark.createDataFrame(
                Collections.emptyList(), schema);

        DuckLakeMerge.into(spark, catalogPath, "merge_empty", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        // Table should be unchanged
        assertEquals(3, spark.sql(
                "SELECT * FROM ducklake.main.merge_empty").count());
    }

    // ---------------------------------------------------------------
    // Merge with schema evolution
    // ---------------------------------------------------------------

    @Test
    public void testMergePreservesColumnsNotInSource() {
        createAndPopulateTable("merge_schema", 3);

        // Source only has id and name (no value column)
        StructType srcSchema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true);

        List<Row> srcRows = Arrays.asList(
                RowFactory.create(2, "updated_2"),
                RowFactory.create(10, "new_10"));
        Dataset<Row> source = spark.createDataFrame(srcRows, srcSchema);

        DuckLakeMerge.into(spark, catalogPath, "merge_schema", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_schema ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertEquals(4, rows.size());

        // id=2: name updated, value preserved from original
        assertEquals("updated_2", rows.get(1).getString(1));
        assertEquals(20.0, rows.get(1).getDouble(2), 0.001);

        // id=10: inserted, value should be null
        assertEquals("new_10", rows.get(3).getString(1));
        assertTrue(rows.get(3).isNullAt(2));
    }

    // ---------------------------------------------------------------
    // Merge creates new snapshot
    // ---------------------------------------------------------------

    @Test
    public void testMergeCreatesNewSnapshot() throws Exception {
        createAndPopulateTable("merge_snap", 3);

        long snapBefore;
        try (Connection conn = DriverManager.getConnection(
                "jdbc:sqlite:" + catalogPath)) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
                rs.next();
                snapBefore = rs.getLong(1);
            }
        }

        Dataset<Row> source = createSourceDF(
                new Object[]{1, "merged", 999.0});

        DuckLakeMerge.into(spark, catalogPath, "merge_snap", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().updateAll()
                .execute();

        try (Connection conn = DriverManager.getConnection(
                "jdbc:sqlite:" + catalogPath)) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
                rs.next();
                assertTrue("Merge should create new snapshot",
                        rs.getLong(1) > snapBefore);
            }

            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT changes_made FROM ducklake_snapshot_changes "
                     + "WHERE snapshot_id = "
                     + "(SELECT MAX(snapshot_id) FROM ducklake_snapshot)")) {
                assertTrue(rs.next());
                String changes = rs.getString("changes_made");
                assertTrue("Should record merge changes",
                        changes.contains("deleted_from_table")
                        || changes.contains("inserted_into_table"));
            }
        }
    }

    // ---------------------------------------------------------------
    // Merge updates table stats correctly
    // ---------------------------------------------------------------

    @Test
    public void testMergeUpdatesTableStats() throws Exception {
        createAndPopulateTable("merge_stats", 5);

        // Upsert: update id=3, insert id=10 → 6 total
        Dataset<Row> source = createSourceDF(
                new Object[]{3, "upd", 300.0},
                new Object[]{10, "new", 100.0});

        DuckLakeMerge.into(spark, catalogPath, "merge_stats", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        try (Connection conn = DriverManager.getConnection(
                "jdbc:sqlite:" + catalogPath)) {
            long tableId;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT table_id FROM ducklake_table "
                    + "WHERE table_name = 'merge_stats' "
                    + "AND end_snapshot IS NULL")) {
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                tableId = rs.getLong(1);
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT record_count FROM ducklake_table_stats "
                    + "WHERE table_id = ?")) {
                ps.setLong(1, tableId);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                assertEquals(6, rs.getLong("record_count"));
            }
        }
    }

    // ---------------------------------------------------------------
    // Delete + insert combo via merge
    // ---------------------------------------------------------------

    @Test
    public void testDeleteMatchedInsertUnmatched() {
        createAndPopulateTable("merge_del_ins", 5);

        // id=2,4 match (delete), id=10 doesn't match (insert)
        Dataset<Row> source = createSourceDF(
                new Object[]{2, "x", 0.0},
                new Object[]{4, "x", 0.0},
                new Object[]{10, "new_10", 100.0});

        DuckLakeMerge.into(spark, catalogPath, "merge_del_ins", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().delete()
                .whenNotMatched().insertAll()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_del_ins ORDER BY id");
        List<Row> rows = result.collectAsList();

        // 5 - 2 deleted + 1 inserted = 4
        assertEquals(4, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(3, rows.get(1).getInt(0));
        assertEquals(5, rows.get(2).getInt(0));
        assertEquals(10, rows.get(3).getInt(0));
    }

    // ---------------------------------------------------------------
    // Multiple WHEN MATCHED clauses with conditions
    // ---------------------------------------------------------------

    @Test
    public void testMultipleWhenMatchedClauses() {
        createAndPopulateTable("merge_multi_when", 5);

        // Source matches id=1 (value=500>100, update),
        //                id=3 (value=50<=100, delete)
        Dataset<Row> source = createSourceDF(
                new Object[]{1, "updated_1", 500.0},
                new Object[]{3, "updated_3", 50.0});

        DuckLakeMerge.into(spark, catalogPath,
                    "merge_multi_when", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched("source.value > 100").updateAll()
                .whenMatched().delete()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_multi_when "
                + "ORDER BY id");
        List<Row> rows = result.collectAsList();

        // id=1 updated, id=3 deleted
        assertEquals(4, rows.size());

        // id=1 should be updated
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("updated_1", rows.get(0).getString(1));
        assertEquals(500.0, rows.get(0).getDouble(2), 0.001);

        // id=3 should be gone
        assertEquals(2, rows.get(1).getInt(0));
        assertEquals(4, rows.get(2).getInt(0));
        assertEquals(5, rows.get(3).getInt(0));
    }

    // ---------------------------------------------------------------
    // Merge after delete (interop with existing delete)
    // ---------------------------------------------------------------

    @Test
    public void testMergeAfterDelete() {
        createAndPopulateTable("merge_after_del", 5);

        // Delete id=2 first
        spark.sql("DELETE FROM ducklake.main.merge_after_del "
                + "WHERE id = 2");

        // Now merge: id=3 matches (update), id=2 doesn't match
        // (was deleted, should insert)
        Dataset<Row> source = createSourceDF(
                new Object[]{2, "reinserted_2", 222.0},
                new Object[]{3, "updated_3", 333.0});

        DuckLakeMerge.into(spark, catalogPath,
                    "merge_after_del", "main")
                .using(source, "source")
                .on("target.id = source.id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        Dataset<Row> result = spark.sql(
                "SELECT * FROM ducklake.main.merge_after_del "
                + "ORDER BY id");
        List<Row> rows = result.collectAsList();

        // 5 - 1 deleted + 1 re-inserted = 5,
        // id=3 updated in place
        assertEquals(5, rows.size());

        // id=2 should be re-inserted
        Row row2 = findRowById(rows, 2);
        assertNotNull("id=2 should exist", row2);
        assertEquals("reinserted_2", row2.getString(1));
        assertEquals(222.0, row2.getDouble(2), 0.001);

        // id=3 should be updated
        Row row3 = findRowById(rows, 3);
        assertNotNull("id=3 should exist", row3);
        assertEquals("updated_3", row3.getString(1));
        assertEquals(333.0, row3.getDouble(2), 0.001);
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private Row findRowById(List<Row> rows, int id) {
        for (Row row : rows) {
            if (row.getInt(0) == id) return row;
        }
        return null;
    }

    // ---------------------------------------------------------------
    // Catalog setup helper (minimal)
    // ---------------------------------------------------------------

    private static void createMinimalCatalog(String catPath,
                                              String dp) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection(
                "jdbc:sqlite:" + catPath)) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE ducklake_metadata("
                    + "key VARCHAR NOT NULL, "
                    + "value VARCHAR NOT NULL, "
                    + "scope VARCHAR, scope_id BIGINT)");
                st.execute("CREATE TABLE ducklake_snapshot("
                    + "snapshot_id BIGINT PRIMARY KEY, "
                    + "snapshot_time TEXT, "
                    + "schema_version BIGINT, "
                    + "next_catalog_id BIGINT, "
                    + "next_file_id BIGINT)");
                st.execute("CREATE TABLE ducklake_snapshot_changes("
                    + "snapshot_id BIGINT PRIMARY KEY, "
                    + "changes_made VARCHAR, "
                    + "author VARCHAR, "
                    + "commit_message VARCHAR, "
                    + "commit_extra_info VARCHAR)");
                st.execute("CREATE TABLE ducklake_schema("
                    + "schema_id BIGINT PRIMARY KEY, "
                    + "schema_uuid TEXT, "
                    + "begin_snapshot BIGINT, "
                    + "end_snapshot BIGINT, "
                    + "schema_name VARCHAR, "
                    + "path VARCHAR, "
                    + "path_is_relative BOOLEAN)");
                st.execute("CREATE TABLE ducklake_table("
                    + "table_id BIGINT, table_uuid TEXT, "
                    + "begin_snapshot BIGINT, "
                    + "end_snapshot BIGINT, "
                    + "schema_id BIGINT, "
                    + "table_name VARCHAR, "
                    + "path VARCHAR, "
                    + "path_is_relative BOOLEAN)");
                st.execute("CREATE TABLE ducklake_column("
                    + "column_id BIGINT, "
                    + "begin_snapshot BIGINT, "
                    + "end_snapshot BIGINT, "
                    + "table_id BIGINT, "
                    + "column_order BIGINT, "
                    + "column_name VARCHAR, "
                    + "column_type VARCHAR, "
                    + "initial_default VARCHAR, "
                    + "default_value VARCHAR, "
                    + "nulls_allowed BOOLEAN, "
                    + "parent_column BIGINT, "
                    + "default_value_type VARCHAR, "
                    + "default_value_dialect VARCHAR)");
                st.execute("CREATE TABLE ducklake_data_file("
                    + "data_file_id BIGINT PRIMARY KEY, "
                    + "table_id BIGINT, "
                    + "begin_snapshot BIGINT, "
                    + "end_snapshot BIGINT, "
                    + "file_order BIGINT, "
                    + "path VARCHAR, "
                    + "path_is_relative BOOLEAN, "
                    + "file_format VARCHAR, "
                    + "record_count BIGINT, "
                    + "file_size_bytes BIGINT, "
                    + "footer_size BIGINT, "
                    + "row_id_start BIGINT, "
                    + "partition_id BIGINT, "
                    + "encryption_key VARCHAR, "
                    + "mapping_id BIGINT, "
                    + "partial_max BIGINT)");
                st.execute("CREATE TABLE ducklake_file_column_stats("
                    + "data_file_id BIGINT, "
                    + "table_id BIGINT, "
                    + "column_id BIGINT, "
                    + "column_size_bytes BIGINT, "
                    + "value_count BIGINT, "
                    + "null_count BIGINT, "
                    + "min_value VARCHAR, "
                    + "max_value VARCHAR, "
                    + "contains_nan BOOLEAN, "
                    + "extra_stats VARCHAR)");
                st.execute("CREATE TABLE ducklake_table_stats("
                    + "table_id BIGINT, "
                    + "record_count BIGINT, "
                    + "next_row_id BIGINT, "
                    + "file_size_bytes BIGINT)");
                st.execute("CREATE TABLE ducklake_delete_file("
                    + "delete_file_id BIGINT PRIMARY KEY, "
                    + "table_id BIGINT, "
                    + "begin_snapshot BIGINT, "
                    + "end_snapshot BIGINT, "
                    + "data_file_id BIGINT, "
                    + "path VARCHAR, "
                    + "path_is_relative BOOLEAN, "
                    + "format VARCHAR, "
                    + "delete_count BIGINT, "
                    + "file_size_bytes BIGINT, "
                    + "footer_size BIGINT, "
                    + "encryption_key VARCHAR, "
                    + "partial_max BIGINT)");
                st.execute("CREATE TABLE ducklake_name_mapping("
                    + "mapping_id BIGINT, "
                    + "column_id BIGINT, "
                    + "source_name VARCHAR, "
                    + "target_field_id BIGINT, "
                    + "parent_column BIGINT, "
                    + "is_partition BOOLEAN)");
                st.execute("CREATE TABLE ducklake_inlined_data_tables("
                    + "table_id BIGINT, "
                    + "table_name VARCHAR, "
                    + "schema_version BIGINT)");
                st.execute("CREATE TABLE ducklake_file_partition_value("
                    + "data_file_id BIGINT, "
                    + "table_id BIGINT, "
                    + "partition_key_index BIGINT, "
                    + "partition_value VARCHAR)");

                st.execute("INSERT INTO ducklake_metadata (key, value) "
                    + "VALUES ('version', '0.4')");
                st.execute("INSERT INTO ducklake_metadata (key, value) "
                    + "VALUES ('data_path', '" + dp + "')");

                st.execute("INSERT INTO ducklake_snapshot VALUES "
                    + "(0, datetime('now'), 0, 1, 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES "
                    + "(0, 'created_schema:\"main\"', NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_schema VALUES "
                    + "(0, 'schema-uuid-0', 0, NULL, 'main', "
                    + "'main/', 1)");
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
