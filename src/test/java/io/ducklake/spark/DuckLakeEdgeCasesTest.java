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
 * Comprehensive edge-case tests for DuckLake Spark connector.
 * Covers boundary values, naming, unicode, wide tables, error paths,
 * multi-table isolation, and filter edge cases.
 */
public class DuckLakeEdgeCasesTest {

    private static SparkSession spark;
    private static String tempDir;
    private static String catalogPath;
    private static String dataPath;

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-edge-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        catalogPath = tempDir + "/test.ducklake";
        createMinimalCatalog(catalogPath, dataPath);
        Thread.currentThread().setContextClassLoader(DuckLakeEdgeCasesTest.class.getClassLoader());
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DuckLakeEdgeCasesTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
                .config("spark.sql.catalog.ducklake.catalog", catalogPath)
                .config("spark.sql.catalog.ducklake.data_path", dataPath)
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        if (spark != null) { spark.stop(); spark = null; }
        if (tempDir != null) deleteRecursive(new File(tempDir));
    }

    // === Empty / Single Row ===

    @Test public void testEmptyTableSchemaPreserved() {
        spark.sql("CREATE TABLE ducklake.main.edge_empty (id INT, name STRING, value DOUBLE)");
        Dataset<Row> df = spark.sql("SELECT * FROM ducklake.main.edge_empty");
        assertEquals(0, df.count());
        assertEquals(3, df.schema().fields().length);
    }

    @Test public void testSingleRowTable() {
        spark.sql("CREATE TABLE ducklake.main.edge_single (id INT, name STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_single VALUES (1, 'only')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_single").count());
    }

    @Test public void testSingleNullRow() {
        spark.sql("CREATE TABLE ducklake.main.edge_nullrow (id INT, name STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_nullrow VALUES (null, null)");
        Dataset<Row> df = spark.sql("SELECT * FROM ducklake.main.edge_nullrow");
        assertEquals(1, df.count());
        assertTrue(df.collectAsList().get(0).isNullAt(0));
    }

    @Test public void testAllNullColumn() {
        spark.sql("CREATE TABLE ducklake.main.edge_allnull (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_allnull VALUES (1, null), (2, null), (3, null)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_allnull WHERE val IS NULL").count());
    }

    @Test public void testDeleteAllRows() {
        spark.sql("CREATE TABLE ducklake.main.edge_delall (id INT, name STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_delall VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        spark.sql("DELETE FROM ducklake.main.edge_delall WHERE id >= 1");
        assertEquals(0, spark.sql("SELECT * FROM ducklake.main.edge_delall").count());
    }

    @Test public void testInsertAfterDeleteAll() {
        spark.sql("CREATE TABLE ducklake.main.edge_insdel (id INT, name STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_insdel VALUES (1, 'a'), (2, 'b')");
        spark.sql("DELETE FROM ducklake.main.edge_insdel WHERE id >= 1");
        spark.sql("INSERT INTO ducklake.main.edge_insdel VALUES (10, 'new')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_insdel").count());
    }

    @Test public void testEmptyTableScan() {
        spark.sql("CREATE TABLE ducklake.main.edge_emptyscan (id INT, val DOUBLE)");
        assertEquals(0, spark.sql("SELECT * FROM ducklake.main.edge_emptyscan WHERE id > 0").count());
    }

    @Test public void testEmptyTableFilter() {
        spark.sql("CREATE TABLE ducklake.main.edge_emptyfilt (id INT, val STRING)");
        assertEquals(0, spark.sql("SELECT * FROM ducklake.main.edge_emptyfilt WHERE val = 'hello'").count());
    }

    // === Wide Tables ===

    @Test public void test20Columns() {
        StringBuilder cols = new StringBuilder(), vals = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            if (i > 0) { cols.append(", "); vals.append(", "); }
            cols.append("c").append(i).append(" INT"); vals.append(i);
        }
        spark.sql("CREATE TABLE ducklake.main.edge_20col (" + cols + ")");
        spark.sql("INSERT INTO ducklake.main.edge_20col VALUES (" + vals + ")");
        assertEquals(20, spark.sql("SELECT * FROM ducklake.main.edge_20col").schema().fields().length);
    }

    @Test public void test50ColumnsMixedTypes() {
        StringBuilder cols = new StringBuilder(), vals = new StringBuilder();
        String[] types = {"INT", "STRING", "DOUBLE", "BOOLEAN", "BIGINT"};
        for (int i = 0; i < 50; i++) {
            if (i > 0) { cols.append(", "); vals.append(", "); }
            String t = types[i % 5]; cols.append("c").append(i).append(" ").append(t);
            switch (t) { case "INT": vals.append(i); break; case "STRING": vals.append("'s").append(i).append("'"); break;
                case "DOUBLE": vals.append(i + 0.5); break; case "BOOLEAN": vals.append(i % 2 == 0); break;
                case "BIGINT": vals.append((long) i * 1000000L); break; }
        }
        spark.sql("CREATE TABLE ducklake.main.edge_50col (" + cols + ")");
        spark.sql("INSERT INTO ducklake.main.edge_50col VALUES (" + vals + ")");
        assertEquals(50, spark.sql("SELECT * FROM ducklake.main.edge_50col").schema().fields().length);
    }

    @Test public void testColumnSelectionOnWideTable() {
        spark.sql("CREATE TABLE ducklake.main.edge_widesel (a INT, b STRING, c DOUBLE, d BOOLEAN, e BIGINT, f INT, g STRING, h DOUBLE, i BOOLEAN, j BIGINT)");
        spark.sql("INSERT INTO ducklake.main.edge_widesel VALUES (1, 'b', 3.0, true, 5, 6, 'g', 8.0, false, 10)");
        Dataset<Row> df = spark.sql("SELECT c, g, j FROM ducklake.main.edge_widesel");
        assertEquals(3, df.schema().fields().length);
        Row r = df.collectAsList().get(0);
        assertEquals(3.0, r.getDouble(0), 0.001);
        assertEquals("g", r.getString(1));
    }

    // === Naming Edge Cases ===

    @Test public void testLongColumnName() {
        String n = "a_very_long_column_name_that_goes_on_and_on_for_testing";
        spark.sql("CREATE TABLE ducklake.main.edge_longcol (" + n + " INT)");
        spark.sql("INSERT INTO ducklake.main.edge_longcol VALUES (42)");
        assertEquals(n, spark.sql("SELECT * FROM ducklake.main.edge_longcol").schema().fields()[0].name());
    }

    @Test public void testColumnNameWithSpaces() {
        spark.sql("CREATE TABLE ducklake.main.edge_spacecol (`my column` INT, `another col` STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_spacecol VALUES (1, 'test')");
        assertEquals(1, spark.sql("SELECT `my column` FROM ducklake.main.edge_spacecol").collectAsList().get(0).getInt(0));
    }

    @Test public void testColumnNameCaseSensitivity() {
        spark.sql("CREATE TABLE ducklake.main.edge_casecol (MyCol INT, mycol2 STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_casecol VALUES (1, 'test')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_casecol").count());
    }

    @Test public void testLongTableName() {
        String t = "a_very_long_table_name_for_testing_edge_cases";
        spark.sql("CREATE TABLE ducklake.main." + t + " (id INT)");
        spark.sql("INSERT INTO ducklake.main." + t + " VALUES (1)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main." + t).count());
    }

    @Test public void testTableNameWithUnderscore() {
        spark.sql("CREATE TABLE ducklake.main.my_test_table_edge (id INT)");
        spark.sql("INSERT INTO ducklake.main.my_test_table_edge VALUES (1)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.my_test_table_edge").count());
    }

    // === Multiple Schemas ===

    @Test public void testMultipleSchemas() {
        spark.sql("CREATE NAMESPACE ducklake.schema_ea");
        spark.sql("CREATE NAMESPACE ducklake.schema_eb");
        spark.sql("CREATE TABLE ducklake.schema_ea.tbl (id INT)");
        spark.sql("CREATE TABLE ducklake.schema_eb.tbl (id INT)");
        spark.sql("INSERT INTO ducklake.schema_ea.tbl VALUES (1)");
        spark.sql("INSERT INTO ducklake.schema_eb.tbl VALUES (2)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.schema_ea.tbl").collectAsList().get(0).getInt(0));
        assertEquals(2, spark.sql("SELECT * FROM ducklake.schema_eb.tbl").collectAsList().get(0).getInt(0));
    }

    // === Error Paths ===

    @Test(expected = Exception.class)
    public void testNonexistentTable() { spark.sql("SELECT * FROM ducklake.main.no_such_table_xyz").collect(); }

    @Test(expected = Exception.class)
    public void testNonexistentSchema() { spark.sql("SELECT * FROM ducklake.no_such_schema.tbl").collect(); }

    @Test(expected = Exception.class)
    public void testReadDroppedTable() {
        spark.sql("CREATE TABLE ducklake.main.edge_drop_read (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_drop_read VALUES (1)");
        spark.sql("DROP TABLE ducklake.main.edge_drop_read");
        spark.sql("SELECT * FROM ducklake.main.edge_drop_read").collect();
    }

    // === Integer / Numeric Boundaries ===

    @Test public void testIntegerBoundaries() {
        spark.sql("CREATE TABLE ducklake.main.edge_intbound (val INT)");
        spark.sql("INSERT INTO ducklake.main.edge_intbound VALUES (" + Integer.MAX_VALUE + "), (" + Integer.MIN_VALUE + "), (0)");
        List<Row> rows = spark.sql("SELECT val FROM ducklake.main.edge_intbound ORDER BY val").collectAsList();
        assertEquals(3, rows.size());
        assertEquals(Integer.MIN_VALUE, rows.get(0).getInt(0));
    }

    @Test public void testBigintBoundaries() {
        spark.sql("CREATE TABLE ducklake.main.edge_bigbound (val BIGINT)");
        spark.sql("INSERT INTO ducklake.main.edge_bigbound VALUES (" + Long.MAX_VALUE + "), (" + Long.MIN_VALUE + "), (0)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_bigbound").count());
    }

    @Test public void testDateBoundaries() {
        spark.sql("CREATE TABLE ducklake.main.edge_datebound (val DATE)");
        spark.sql("INSERT INTO ducklake.main.edge_datebound VALUES (DATE '1970-01-01'), (DATE '2099-12-31'), (DATE '2000-02-29')");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_datebound").count());
    }

    // === String Edge Cases ===

    @Test public void testEmptyStringVsNull() {
        spark.sql("CREATE TABLE ducklake.main.edge_emptynull (val STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_emptynull VALUES (''), (null), ('notempty')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_emptynull WHERE val IS NULL").count());
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_emptynull WHERE val = ''").count());
    }

    @Test public void testZeroValues() {
        spark.sql("CREATE TABLE ducklake.main.edge_zeros (i INT, d DOUBLE, s STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_zeros VALUES (0, 0.0, '0')");
        Row r = spark.sql("SELECT * FROM ducklake.main.edge_zeros").collectAsList().get(0);
        assertEquals(0, r.getInt(0)); assertEquals(0.0, r.getDouble(1), 0.0);
    }

    @Test public void testNegativeZeroFloat() {
        spark.sql("CREATE TABLE ducklake.main.edge_negzero (val DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.edge_negzero VALUES (-0.0), (0.0)");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.edge_negzero").count());
    }

    @Test public void testVeryLargeVarchar() {
        spark.sql("CREATE TABLE ducklake.main.edge_bigstr (val STRING)");
        StringBuilder sb = new StringBuilder(); for (int i = 0; i < 10000; i++) sb.append("x");
        List<Row> rows = Collections.singletonList(RowFactory.create(sb.toString()));
        spark.createDataFrame(rows, new StructType().add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_bigstr").mode("append").save();
        assertEquals(10000, spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_bigstr").load()
                .collectAsList().get(0).getString(0).length());
    }

    @Test public void testStringWithNewlines() {
        spark.sql("CREATE TABLE ducklake.main.edge_newline (val STRING)");
        List<Row> rows = Collections.singletonList(RowFactory.create("line1\nline2\nline3"));
        spark.createDataFrame(rows, new StructType().add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_newline").mode("append").save();
        assertTrue(spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_newline").load()
                .collectAsList().get(0).getString(0).contains("\n"));
    }

    @Test public void testStringWithTabs() {
        spark.sql("CREATE TABLE ducklake.main.edge_tab (val STRING)");
        List<Row> rows = Collections.singletonList(RowFactory.create("col1\tcol2\tcol3"));
        spark.createDataFrame(rows, new StructType().add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_tab").mode("append").save();
        assertTrue(spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_tab").load()
                .collectAsList().get(0).getString(0).contains("\t"));
    }

    @Test public void testStringWithQuotes() {
        spark.sql("CREATE TABLE ducklake.main.edge_quotes (val STRING)");
        List<Row> rows = Collections.singletonList(RowFactory.create("it's a \"test\""));
        spark.createDataFrame(rows, new StructType().add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_quotes").mode("append").save();
        assertTrue(spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_quotes").load()
                .collectAsList().get(0).getString(0).contains("\"test\""));
    }

    @Test public void testStringWithBackslash() {
        spark.sql("CREATE TABLE ducklake.main.edge_bslash (val STRING)");
        List<Row> rows = Collections.singletonList(RowFactory.create("path\\to\\file"));
        spark.createDataFrame(rows, new StructType().add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_bslash").mode("append").save();
        assertTrue(spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_bslash").load()
                .collectAsList().get(0).getString(0).contains("\\"));
    }

    @Test public void testVeryLongString100K() {
        spark.sql("CREATE TABLE ducklake.main.edge_100k (val STRING)");
        StringBuilder sb = new StringBuilder(); for (int i = 0; i < 100000; i++) sb.append("a");
        List<Row> rows = Collections.singletonList(RowFactory.create(sb.toString()));
        spark.createDataFrame(rows, new StructType().add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_100k").mode("append").save();
        assertEquals(100000, spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_100k").load()
                .collectAsList().get(0).getString(0).length());
    }

    // === Unicode ===

    @Test public void testUnicodeVarcharValues() {
        spark.sql("CREATE TABLE ducklake.main.edge_unicode (val STRING)");
        List<Row> rows = Arrays.asList(RowFactory.create("日本語テスト"), RowFactory.create("Ñoño"),
                RowFactory.create("🦆🚀"), RowFactory.create("café résumé"));
        spark.createDataFrame(rows, new StructType().add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_unicode").mode("append").save();
        assertEquals(4, spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_unicode").load().count());
    }

    @Test public void testUnicodeFilter() {
        spark.sql("CREATE TABLE ducklake.main.edge_unifilt (id INT, val STRING)");
        List<Row> rows = Arrays.asList(RowFactory.create(1, "hello"), RowFactory.create(2, "日本語"), RowFactory.create(3, "🦆"));
        spark.createDataFrame(rows, new StructType().add("id", DataTypes.IntegerType).add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_unifilt").mode("append").save();
        assertEquals(1, spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_unifilt").load()
                .filter("val = '🦆'").count());
    }

    // === Filter Edge Cases ===

    @Test public void testFilterAllNullColumn() {
        spark.sql("CREATE TABLE ducklake.main.edge_filtnull (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_filtnull VALUES (1, null), (2, null), (3, null)");
        assertEquals(0, spark.sql("SELECT * FROM ducklake.main.edge_filtnull WHERE val = 'x'").count());
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_filtnull WHERE val IS NULL").count());
    }

    @Test public void testFilterReturnsNothing() {
        spark.sql("CREATE TABLE ducklake.main.edge_filtnone (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_filtnone VALUES (1), (2), (3)");
        assertEquals(0, spark.sql("SELECT * FROM ducklake.main.edge_filtnone WHERE id > 100").count());
    }

    @Test public void testFilterReturnsEverything() {
        spark.sql("CREATE TABLE ducklake.main.edge_filtall (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_filtall VALUES (1), (2), (3)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_filtall WHERE id > 0").count());
    }

    @Test public void testFilterOnNonPartitionColumn() {
        spark.sql("CREATE TABLE ducklake.main.edge_filtnonpart (id INT, cat STRING, val DOUBLE) PARTITIONED BY (cat)");
        spark.sql("INSERT INTO ducklake.main.edge_filtnonpart VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'a', 3.0)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_filtnonpart WHERE val > 2.5").count());
    }

    @Test public void testCombinedAndFilter() {
        spark.sql("CREATE TABLE ducklake.main.edge_andfilt (id INT, name STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_andfilt VALUES (1, 'a'), (2, 'b'), (3, 'a')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_andfilt WHERE id > 1 AND name = 'a'").count());
    }

    @Test public void testOrFilter() {
        spark.sql("CREATE TABLE ducklake.main.edge_orfilt (id INT, name STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_orfilt VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.edge_orfilt WHERE id = 1 OR name = 'c'").count());
    }

    @Test public void testNotFilter() {
        spark.sql("CREATE TABLE ducklake.main.edge_notfilt (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_notfilt VALUES (1), (2), (3), (4), (5)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_notfilt WHERE NOT id <= 2").count());
    }

    // === Multi-step Operations ===

    @Test public void testManySmallInserts() {
        spark.sql("CREATE TABLE ducklake.main.edge_many (id INT)");
        for (int i = 0; i < 10; i++) spark.sql("INSERT INTO ducklake.main.edge_many VALUES (" + i + ")");
        assertEquals(10, spark.sql("SELECT * FROM ducklake.main.edge_many").count());
    }

    @Test public void testInsertDeleteRepeat() {
        spark.sql("CREATE TABLE ducklake.main.edge_insdelrpt (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_insdelrpt VALUES (1), (2), (3)");
        spark.sql("DELETE FROM ducklake.main.edge_insdelrpt WHERE id = 2");
        spark.sql("INSERT INTO ducklake.main.edge_insdelrpt VALUES (4), (5)");
        spark.sql("DELETE FROM ducklake.main.edge_insdelrpt WHERE id = 1");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_insdelrpt").count());
    }

    @Test public void testUpdateSameRowMultipleTimes() {
        spark.sql("CREATE TABLE ducklake.main.edge_multupd (id INT, val INT)");
        spark.sql("INSERT INTO ducklake.main.edge_multupd VALUES (1, 0)");
        for (int i = 1; i <= 5; i++) {
            io.ducklake.spark.writer.DuckLakeUpdateExecutor updater =
                    new io.ducklake.spark.writer.DuckLakeUpdateExecutor(catalogPath, dataPath, "edge_multupd", "main");
            java.util.Map<String, Object> updates = new java.util.HashMap<>();
            updates.put("val", i);
            updater.updateWhere(new org.apache.spark.sql.sources.Filter[]{
                    new org.apache.spark.sql.sources.EqualTo("id", 1)}, updates);
        }
        assertEquals(5, spark.sql("SELECT val FROM ducklake.main.edge_multupd").collectAsList().get(0).getInt(0));
    }

    @Test public void testDeleteAllThenMultipleInserts() {
        spark.sql("CREATE TABLE ducklake.main.edge_delmulti (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_delmulti VALUES (1), (2), (3)");
        spark.sql("DELETE FROM ducklake.main.edge_delmulti WHERE id >= 0");
        spark.sql("INSERT INTO ducklake.main.edge_delmulti VALUES (10)");
        spark.sql("INSERT INTO ducklake.main.edge_delmulti VALUES (20)");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.edge_delmulti").count());
    }

    @Test public void testSchemaEvolutionMultipleSteps() {
        spark.sql("CREATE TABLE ducklake.main.edge_multievo (id INT, name STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_multievo VALUES (1, 'a')");
        spark.sql("ALTER TABLE ducklake.main.edge_multievo ADD COLUMNS (c3 DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.edge_multievo VALUES (2, 'b', 2.0)");
        spark.sql("ALTER TABLE ducklake.main.edge_multievo ADD COLUMNS (c4 BOOLEAN)");
        spark.sql("INSERT INTO ducklake.main.edge_multievo VALUES (3, 'c', 3.0, true)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_multievo").count());
        assertEquals(4, spark.sql("SELECT * FROM ducklake.main.edge_multievo").schema().fields().length);
    }

    // === Multi-table Isolation ===

    @Test public void testDeleteOnOneTablePreservesOther() {
        spark.sql("CREATE TABLE ducklake.main.edge_iso_a (id INT, val STRING)");
        spark.sql("CREATE TABLE ducklake.main.edge_iso_b (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_iso_a VALUES (1, 'a'), (2, 'b')");
        spark.sql("INSERT INTO ducklake.main.edge_iso_b VALUES (10, 'x'), (20, 'y')");
        spark.sql("DELETE FROM ducklake.main.edge_iso_a WHERE id = 1");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_iso_a").count());
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.edge_iso_b").count());
    }

    @Test public void testSchemaChangeOnOneTablePreservesOther() {
        spark.sql("CREATE TABLE ducklake.main.edge_isob_a (id INT, val STRING)");
        spark.sql("CREATE TABLE ducklake.main.edge_isob_b (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_isob_b VALUES (1, 'b')");
        spark.sql("ALTER TABLE ducklake.main.edge_isob_a ADD COLUMNS (extra INT)");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.edge_isob_b").schema().fields().length);
    }

    @Test public void testDropTablePreservesOther() {
        spark.sql("CREATE TABLE ducklake.main.edge_isoc_a (id INT)");
        spark.sql("CREATE TABLE ducklake.main.edge_isoc_b (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_isoc_b VALUES (2)");
        spark.sql("DROP TABLE ducklake.main.edge_isoc_a");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_isoc_b").count());
    }

    // === Snapshots ===

    @Test public void testReadAtEachSnapshot() {
        spark.sql("CREATE TABLE ducklake.main.edge_snaprd (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_snaprd VALUES (1)");
        spark.sql("INSERT INTO ducklake.main.edge_snaprd VALUES (2)");
        spark.sql("INSERT INTO ducklake.main.edge_snaprd VALUES (3)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_snaprd").count());
    }

    @Test public void testManySnapshots() {
        spark.sql("CREATE TABLE ducklake.main.edge_manysnap (id INT)");
        for (int i = 0; i < 20; i++) spark.sql("INSERT INTO ducklake.main.edge_manysnap VALUES (" + i + ")");
        assertEquals(20, spark.sql("SELECT * FROM ducklake.main.edge_manysnap").count());
    }

    @Test public void testCurrentSnapshotIncreases() throws Exception {
        spark.sql("CREATE TABLE ducklake.main.edge_snapinc (id INT)");
        long snap1 = getMaxSnapshot();
        spark.sql("INSERT INTO ducklake.main.edge_snapinc VALUES (1)");
        long snap2 = getMaxSnapshot();
        assertTrue(snap2 > snap1);
    }

    // === Decimal Edge Cases ===

    @Test public void testDecimalZeroScale() {
        spark.sql("CREATE TABLE ducklake.main.edge_dec0 (val DECIMAL(10, 0))");
        spark.sql("INSERT INTO ducklake.main.edge_dec0 VALUES (12345), (-67890), (0)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_dec0").count());
    }

    @Test public void testDecimalMaxScale() {
        spark.sql("CREATE TABLE ducklake.main.edge_decmax (val DECIMAL(38, 18))");
        spark.sql("INSERT INTO ducklake.main.edge_decmax VALUES (1.123456789012345678)");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_decmax").count());
    }

    @Test public void testDecimalNegativeValue() {
        spark.sql("CREATE TABLE ducklake.main.edge_decneg (val DECIMAL(10, 2))");
        spark.sql("INSERT INTO ducklake.main.edge_decneg VALUES (-99.99), (0.01), (99.99)");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_decneg").count());
    }

    // === Nested Types ===

    @Test public void testNestedStructInStruct() {
        spark.sql("CREATE TABLE ducklake.main.edge_neststruct (val STRUCT<a: INT, inner: STRUCT<b: STRING, c: DOUBLE>>)");
        spark.sql("INSERT INTO ducklake.main.edge_neststruct VALUES (named_struct('a', 1, 'inner', named_struct('b', 'hello', 'c', 3.14)))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_neststruct").count());
    }

    @Test public void testListOfDifferentScalarTypes() {
        spark.sql("CREATE TABLE ducklake.main.edge_listscalar (ints ARRAY<INT>, strs ARRAY<STRING>)");
        spark.sql("INSERT INTO ducklake.main.edge_listscalar VALUES (array(1, 2, 3), array('a', 'b'))");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_listscalar").count());
    }

    // === Rename Edge Cases ===

    @Test public void testRenameOnlyColumn() {
        spark.sql("CREATE TABLE ducklake.main.edge_renonly (val INT)");
        spark.sql("INSERT INTO ducklake.main.edge_renonly VALUES (42)");
        spark.sql("ALTER TABLE ducklake.main.edge_renonly RENAME COLUMN val TO new_val");
        assertEquals(42, spark.sql("SELECT new_val FROM ducklake.main.edge_renonly").collectAsList().get(0).getInt(0));
    }

    @Test public void testRenamePreservesDataAcrossFiles() {
        spark.sql("CREATE TABLE ducklake.main.edge_renfiles (id INT, val STRING)");
        spark.sql("INSERT INTO ducklake.main.edge_renfiles VALUES (1, 'a')");
        spark.sql("INSERT INTO ducklake.main.edge_renfiles VALUES (2, 'b')");
        spark.sql("ALTER TABLE ducklake.main.edge_renfiles RENAME COLUMN val TO new_val");
        spark.sql("INSERT INTO ducklake.main.edge_renfiles VALUES (3, 'c')");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_renfiles ORDER BY id").count());
    }

    // === Partition Edge Cases ===

    @Test public void testPartitionWithNullValues() {
        spark.sql("CREATE TABLE ducklake.main.edge_partnull (id INT, part STRING) PARTITIONED BY (part)");
        spark.sql("INSERT INTO ducklake.main.edge_partnull VALUES (1, 'a'), (2, null), (3, 'b')");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_partnull").count());
    }

    @Test public void testPartitionSinglePartitionValue() {
        spark.sql("CREATE TABLE ducklake.main.edge_partsingle (id INT, part STRING) PARTITIONED BY (part)");
        spark.sql("INSERT INTO ducklake.main.edge_partsingle VALUES (1, 'only'), (2, 'only'), (3, 'only')");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_partsingle WHERE part = 'only'").count());
    }

    @Test public void testPartitionManyUniqueValues() {
        spark.sql("CREATE TABLE ducklake.main.edge_partmany (id INT, part STRING) PARTITIONED BY (part)");
        StringBuilder vals = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            if (i > 0) vals.append(", ");
            vals.append("(").append(i).append(", 'p").append(i).append("')");
        }
        spark.sql("INSERT INTO ducklake.main.edge_partmany VALUES " + vals);
        assertEquals(20, spark.sql("SELECT * FROM ducklake.main.edge_partmany").count());
    }

    // === Column Selection ===

    @Test public void testSelectSingleColumn() {
        spark.sql("CREATE TABLE ducklake.main.edge_sel1 (a INT, b STRING, c DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.edge_sel1 VALUES (1, 'hello', 3.14)");
        assertEquals("hello", spark.sql("SELECT b FROM ducklake.main.edge_sel1").collectAsList().get(0).getString(0));
    }

    @Test public void testSelectColumnsOutOfOrder() {
        spark.sql("CREATE TABLE ducklake.main.edge_selord (a INT, b STRING, c DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.edge_selord VALUES (1, 'hello', 3.14)");
        Row r = spark.sql("SELECT c, a FROM ducklake.main.edge_selord").collectAsList().get(0);
        assertEquals(3.14, r.getDouble(0), 0.001);
        assertEquals(1, r.getInt(1));
    }

    @Test public void testSelectAllColumnsExplicit() {
        spark.sql("CREATE TABLE ducklake.main.edge_selall (a INT, b STRING, c DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.edge_selall VALUES (1, 'x', 2.0)");
        assertEquals(3, spark.sql("SELECT a, b, c FROM ducklake.main.edge_selall").schema().fields().length);
    }

    // === Multiple Reads Consistency ===

    @Test public void testMultipleReadsConsistent() {
        spark.sql("CREATE TABLE ducklake.main.edge_consist (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_consist VALUES (1), (2), (3)");
        assertEquals(spark.sql("SELECT * FROM ducklake.main.edge_consist").count(),
                spark.sql("SELECT * FROM ducklake.main.edge_consist").count());
    }

    @Test public void testFilteredReadSubsetOfFull() {
        spark.sql("CREATE TABLE ducklake.main.edge_subset (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_subset VALUES (1), (2), (3), (4), (5)");
        assertEquals(5, spark.sql("SELECT * FROM ducklake.main.edge_subset").count());
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_subset WHERE id <= 3").count());
    }

    // === Sorted Read / Delete from Specific File ===

    @Test public void testThreeFilesSortedRead() {
        spark.sql("CREATE TABLE ducklake.main.edge_sorted (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_sorted VALUES (3)");
        spark.sql("INSERT INTO ducklake.main.edge_sorted VALUES (1)");
        spark.sql("INSERT INTO ducklake.main.edge_sorted VALUES (2)");
        List<Row> rows = spark.sql("SELECT * FROM ducklake.main.edge_sorted ORDER BY id").collectAsList();
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(3, rows.get(2).getInt(0));
    }

    @Test public void testDeleteFromSpecificFile() {
        spark.sql("CREATE TABLE ducklake.main.edge_delfile (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_delfile VALUES (1), (2)");
        spark.sql("INSERT INTO ducklake.main.edge_delfile VALUES (3), (4)");
        spark.sql("DELETE FROM ducklake.main.edge_delfile WHERE id = 3");
        assertEquals(3, spark.sql("SELECT * FROM ducklake.main.edge_delfile").count());
    }

    // === All Numeric / Temporal Types ===

    @Test public void testAllNumericTypes() {
        spark.sql("CREATE TABLE ducklake.main.edge_allnum (ti TINYINT, si SMALLINT, i INT, bi BIGINT, f FLOAT, d DOUBLE)");
        spark.sql("INSERT INTO ducklake.main.edge_allnum VALUES (1, 100, 10000, 1000000000, 1.5, 2.5)");
        assertEquals(6, spark.sql("SELECT * FROM ducklake.main.edge_allnum").schema().fields().length);
    }

    @Test public void testAllTemporalTypes() {
        spark.sql("CREATE TABLE ducklake.main.edge_alltmp (d DATE, ts TIMESTAMP)");
        spark.sql("INSERT INTO ducklake.main.edge_alltmp VALUES (DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00')");
        assertEquals(1, spark.sql("SELECT * FROM ducklake.main.edge_alltmp").count());
    }

    // === Time Travel Across Tables ===

    @Test public void testTimeTravelIndependentPerTable() {
        spark.sql("CREATE TABLE ducklake.main.edge_ttA (id INT)");
        spark.sql("CREATE TABLE ducklake.main.edge_ttB (id INT)");
        spark.sql("INSERT INTO ducklake.main.edge_ttA VALUES (1)");
        spark.sql("INSERT INTO ducklake.main.edge_ttB VALUES (10), (20)");
        spark.sql("INSERT INTO ducklake.main.edge_ttA VALUES (2)");
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.edge_ttA").count());
        assertEquals(2, spark.sql("SELECT * FROM ducklake.main.edge_ttB").count());
    }

    // === Inlining Edge Cases ===

    @Test public void testInlinedSingleValue() {
        spark.sql("CREATE TABLE ducklake.main.edge_inline1 (id INT, val STRING)");
        List<Row> rows = Collections.singletonList(RowFactory.create(42, "single"));
        spark.createDataFrame(rows, new StructType().add("id", DataTypes.IntegerType).add("val", DataTypes.StringType))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_inline1").mode("append").save();
        assertEquals(1, spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_inline1").load().count());
    }

    @Test public void testInlinedWithNulls() {
        spark.sql("CREATE TABLE ducklake.main.edge_inline2 (id INT, val STRING)");
        List<Row> rows = Arrays.asList(RowFactory.create(1, (String) null), RowFactory.create(null, "test"));
        spark.createDataFrame(rows, new StructType().add("id", DataTypes.IntegerType, true).add("val", DataTypes.StringType, true))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_inline2").mode("append").save();
        assertEquals(2, spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_inline2").load().count());
    }

    @Test public void testInlinedBooleanValues() {
        spark.sql("CREATE TABLE ducklake.main.edge_inline3 (id INT, flag BOOLEAN)");
        List<Row> rows = Arrays.asList(RowFactory.create(1, true), RowFactory.create(2, false), RowFactory.create(3, null));
        spark.createDataFrame(rows, new StructType().add("id", DataTypes.IntegerType).add("flag", DataTypes.BooleanType, true))
                .write().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_inline3").mode("append").save();
        assertEquals(3, spark.read().format("io.ducklake.spark.DuckLakeDataSource")
                .option("catalog", catalogPath).option("table", "edge_inline3").load().count());
    }

    // === Helpers ===

    private long getMaxSnapshot() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + catalogPath);
             Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
            rs.next(); return rs.getLong(1);
        }
    }

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
        if (file.isDirectory()) { File[] c = file.listFiles(); if (c != null) for (File f : c) deleteRecursive(f); }
        file.delete();
    }
}
