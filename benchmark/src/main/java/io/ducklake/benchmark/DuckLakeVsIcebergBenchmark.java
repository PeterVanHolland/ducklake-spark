package io.ducklake.benchmark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * DuckLake-Spark vs Iceberg — Catalog overhead isolation.
 *
 * Measures raw Parquet I/O baseline (no catalog), then subtracts it
 * to show ONLY the catalog overhead for each operation.
 */
public class DuckLakeVsIcebergBenchmark {

    static SparkSession spark;
    static final int ROWS_100K = 100_000;
    static final int BATCH_SIZE = 1_000;
    static final int NUM_BATCHES = 100;

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "ducklake";
        String tempDir = args.length > 1 ? args[1] : Files.createTempDirectory("bench-").toString();

        if (mode.equals("baseline")) runBaseline(tempDir);
        else if (mode.equals("ducklake")) runDuckLake(tempDir);
        else if (mode.equals("iceberg")) runIceberg(tempDir);
    }

    // ===================================================================
    // Baseline: raw Parquet I/O through Spark (no catalog)
    // ===================================================================

    static void runBaseline(String tempDir) throws Exception {
        String parquetDir = tempDir + "/parquet/"; new File(parquetDir).mkdirs();

        spark = SparkSession.builder().master("local[4]").appName("Baseline")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "localhost")
            .config("spark.driver.memory", "3g").getOrCreate();

        // Warmup
        Dataset<Row> w = genData(1000); w.write().mode("overwrite").parquet(parquetDir + "warmup");
        spark.read().parquet(parquetDir + "warmup").count();

        System.out.println("BASELINE_START");

        // 1. Streaming: 100 x 1K batch writes to separate parquet dirs
        String streamDir = parquetDir + "stream/";
        long r = t(() -> {
            for (int b = 0; b < NUM_BATCHES; b++) {
                Dataset<Row> batch = genBatch(b, BATCH_SIZE);
                batch.write().mode("overwrite").parquet(streamDir + "batch_" + b);
            }
        });
        System.out.println("streaming_write_100x1k=" + r);

        // 2. Read 100 parquet dirs (scan after streaming equivalent)
        r = t(() -> spark.read().parquet(streamDir + "batch_*").filter("category = 7").count());
        System.out.println("scan_100_files=" + r);

        // 3. Single 100K write
        Dataset<Row> data = genData(ROWS_100K); data.cache(); data.count();
        r = t(() -> data.write().mode("overwrite").parquet(parquetDir + "baseline_w"));
        System.out.println("write_100k=" + r);

        // 4. Single 100K read
        r = t(() -> spark.read().parquet(parquetDir + "baseline_w").count());
        System.out.println("read_100k=" + r);

        // 5. Read with filter
        r = t(() -> spark.read().parquet(parquetDir + "baseline_w").filter("category = 7").count());
        System.out.println("read_100k_filtered=" + r);

        // 6. Read single small parquet (time travel baseline)
        spark.sql("SELECT 1 AS id, 'v1' AS val").write().mode("overwrite").parquet(parquetDir + "tiny");
        r = t(() -> spark.read().parquet(parquetDir + "tiny").count());
        System.out.println("read_tiny=" + r);

        data.unpersist();
        System.out.println("BASELINE_END");
        spark.stop();
    }

    // ===================================================================
    // DuckLake/SQLite
    // ===================================================================

    static void runDuckLake(String tempDir) throws Exception {
        String dataPath = tempDir + "/dl_data/"; new File(dataPath).mkdirs();
        String catPath = tempDir + "/catalog.ducklake";
        createSQLiteCatalog(catPath, dataPath);

        spark = SparkSession.builder().master("local[4]").appName("DuckLake")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "localhost")
            .config("spark.driver.memory", "3g")
            .config("spark.sql.catalog.dl", "io.ducklake.spark.catalog.DuckLakeCatalog")
            .config("spark.sql.catalog.dl.catalog", catPath)
            .config("spark.sql.catalog.dl.data_path", dataPath).getOrCreate();

        warmup(catPath);
        String p = "dl.main";

        clearCaches();
        System.out.println("DUCKLAKE_START");

        // 1. Streaming: 100x1K
        spark.sql("CREATE TABLE " + p + ".stream_t (id INT, name STRING, value DOUBLE, category INT)");
        long r = t(() -> {
            for (int b = 0; b < NUM_BATCHES; b++) {
                Dataset<Row> batch = genBatch(b, BATCH_SIZE);
                batch.write().format("io.ducklake.spark.DuckLakeDataSource")
                    .option("catalog", catPath).option("table", "stream_t").mode("append").save();
            }
        });
        System.out.println("streaming_write_100x1k=" + r);

        clearCaches();
        // 2. Scan 100 files
        r = t(() -> spark.sql("SELECT * FROM " + p + ".stream_t WHERE category = 7").count());
        System.out.println("scan_100_files=" + r);

        clearCaches();
        // 3. Add column 50x
        spark.sql("CREATE TABLE " + p + ".schema_t (id INT, name STRING, value DOUBLE, category INT)");
        Dataset<Row> base = genData(ROWS_100K); base.cache(); base.count();
        base.write().format("io.ducklake.spark.DuckLakeDataSource")
            .option("catalog", catPath).option("table", "schema_t").mode("append").save();
        r = t(() -> { for (int i = 0; i < 50; i++) spark.sql("ALTER TABLE " + p + ".schema_t ADD COLUMNS (extra_" + i + " DOUBLE)"); });
        System.out.println("add_column_50x=" + r);

        clearCaches();
        // 4. Rename column 50x
        r = t(() -> { for (int i = 0; i < 50; i++) spark.sql("ALTER TABLE " + p + ".schema_t RENAME COLUMN extra_" + i + " TO renamed_" + i); });
        System.out.println("rename_column_50x=" + r);
        base.unpersist();

        clearCaches();
        // 5. Time travel setup + read
        spark.sql("CREATE TABLE " + p + ".tt_t (id INT, val STRING)");
        for (int i = 0; i < 100; i++) spark.sql("INSERT INTO " + p + ".tt_t VALUES (" + i + ", 'v" + i + "')");
        r = t(() -> spark.read().format("io.ducklake.spark.DuckLakeDataSource")
            .option("catalog", catPath).option("table", "tt_t").option("asOfVersion", "50").load().count());
        System.out.println("time_travel_snap50=" + r);

        clearCaches();
        // 6. Write 100K
        spark.sql("CREATE TABLE " + p + ".baseline_w (id INT, name STRING, value DOUBLE, category INT)");
        Dataset<Row> bdata = genData(ROWS_100K); bdata.cache(); bdata.count();
        r = t(() -> bdata.write().format("io.ducklake.spark.DuckLakeDataSource")
            .option("catalog", catPath).option("table", "baseline_w").mode("append").save());
        System.out.println("write_100k=" + r);
        bdata.unpersist();

        clearCaches();
        // 7. Read 100K
        r = t(() -> spark.sql("SELECT * FROM " + p + ".baseline_w").count());
        System.out.println("read_100k=" + r);

        clearCaches();
        // 8. Read 100K filtered
        r = t(() -> spark.sql("SELECT * FROM " + p + ".baseline_w WHERE category = 7").count());
        System.out.println("read_100k_filtered=" + r);

        clearCaches();
        // 9. Create 20 tables
        r = t(() -> { for (int i = 0; i < 20; i++) spark.sql("CREATE TABLE " + p + ".ddl_" + i + " (id INT, name STRING, value DOUBLE)"); });
        System.out.println("create_20_tables=" + r);

        System.out.println("DUCKLAKE_END");
        spark.stop();
    }

    // ===================================================================
    // Iceberg
    // ===================================================================

    static void runIceberg(String tempDir) throws Exception {
        String icebergWh = tempDir + "/iceberg/"; new File(icebergWh).mkdirs();
        spark = SparkSession.builder().master("local[4]").appName("Iceberg")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "localhost")
            .config("spark.driver.memory", "3g")
            .config("spark.sql.catalog.ic", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ic.type", "hadoop")
            .config("spark.sql.catalog.ic.warehouse", icebergWh).getOrCreate();

        spark.sql("CREATE TABLE ic.default.warmup (id INT)");
        spark.sql("INSERT INTO ic.default.warmup VALUES (1)");
        spark.sql("SELECT * FROM ic.default.warmup").count();
        spark.sql("DROP TABLE ic.default.warmup");

        String p = "ic.default";

        clearCaches();
        System.out.println("ICEBERG_START");

        // 1. Streaming: 100x1K
        spark.sql("CREATE TABLE " + p + ".stream_t (id INT, name STRING, value DOUBLE, category INT)");
        long r = t(() -> {
            for (int b = 0; b < NUM_BATCHES; b++) {
                Dataset<Row> batch = genBatch(b, BATCH_SIZE);
                batch.createOrReplaceTempView("_batch");
                spark.sql("INSERT INTO " + p + ".stream_t SELECT * FROM _batch");
            }
        });
        System.out.println("streaming_write_100x1k=" + r);

        clearCaches();
        // 2. Scan 100 files
        r = t(() -> spark.sql("SELECT * FROM " + p + ".stream_t WHERE category = 7").count());
        System.out.println("scan_100_files=" + r);

        clearCaches();
        // 3. Add column 50x
        spark.sql("CREATE TABLE " + p + ".schema_t (id INT, name STRING, value DOUBLE, category INT)");
        Dataset<Row> base = genData(ROWS_100K); base.cache(); base.count();
        base.createOrReplaceTempView("_base"); spark.sql("INSERT INTO " + p + ".schema_t SELECT * FROM _base");
        r = t(() -> { for (int i = 0; i < 50; i++) spark.sql("ALTER TABLE " + p + ".schema_t ADD COLUMNS (extra_" + i + " DOUBLE)"); });
        System.out.println("add_column_50x=" + r);

        clearCaches();
        // 4. Rename column 50x
        r = t(() -> { for (int i = 0; i < 50; i++) spark.sql("ALTER TABLE " + p + ".schema_t RENAME COLUMN extra_" + i + " TO renamed_" + i); });
        System.out.println("rename_column_50x=" + r);
        base.unpersist();

        clearCaches();
        // 5. Time travel
        spark.sql("CREATE TABLE " + p + ".tt_t (id INT, val STRING)");
        for (int i = 0; i < 100; i++) spark.sql("INSERT INTO " + p + ".tt_t VALUES (" + i + ", 'v" + i + "')");
        Dataset<Row> snaps = spark.sql("SELECT snapshot_id FROM " + p + ".tt_t.snapshots ORDER BY committed_at");
        List<Row> snapList = snaps.collectAsList();
        long snapId = snapList.size() > 50 ? snapList.get(50).getLong(0) : snapList.get(snapList.size()/2).getLong(0);
        long fSnapId = snapId;
        r = t(() -> spark.read().format("iceberg").option("snapshot-id", fSnapId).load(p + ".tt_t").count());
        System.out.println("time_travel_snap50=" + r);

        clearCaches();
        // 6. Write 100K
        spark.sql("CREATE TABLE " + p + ".baseline_w (id INT, name STRING, value DOUBLE, category INT)");
        Dataset<Row> bdata = genData(ROWS_100K); bdata.cache(); bdata.count();
        r = t(() -> { bdata.createOrReplaceTempView("_bw"); spark.sql("INSERT INTO " + p + ".baseline_w SELECT * FROM _bw"); });
        System.out.println("write_100k=" + r);
        bdata.unpersist();

        clearCaches();
        // 7. Read 100K
        r = t(() -> spark.sql("SELECT * FROM " + p + ".baseline_w").count());
        System.out.println("read_100k=" + r);

        clearCaches();
        // 8. Read 100K filtered
        r = t(() -> spark.sql("SELECT * FROM " + p + ".baseline_w WHERE category = 7").count());
        System.out.println("read_100k_filtered=" + r);

        clearCaches();
        // 9. Create 20 tables
        r = t(() -> { for (int i = 0; i < 20; i++) spark.sql("CREATE TABLE " + p + ".ddl_" + i + " (id INT, name STRING, value DOUBLE)"); });
        System.out.println("create_20_tables=" + r);

        System.out.println("ICEBERG_END");
        spark.stop();
    }


    /** Clear all Spark caches between benchmarks for fair comparison. */
    static void clearCaches() {
        spark.catalog().clearCache();
        // Force GC to reclaim any cached metadata objects
        System.gc();
        try { Thread.sleep(100); } catch (InterruptedException e) {}
    }
    // Data generators
    static Dataset<Row> genData(int n) {
        List<Row> rows = new ArrayList<>(n); Random rng = new Random(42);
        for (int i = 0; i < n; i++)
            rows.add(RowFactory.create(i, "name_" + (i % 1000), rng.nextDouble() * 10000, i % 50));
        return spark.createDataFrame(rows, new StructType()
            .add("id", DataTypes.IntegerType).add("name", DataTypes.StringType)
            .add("value", DataTypes.DoubleType).add("category", DataTypes.IntegerType));
    }
    static Dataset<Row> genBatch(int batchId, int size) {
        List<Row> rows = new ArrayList<>(size); Random rng = new Random(batchId);
        int off = batchId * size;
        for (int i = 0; i < size; i++)
            rows.add(RowFactory.create(off+i, "name_" + ((off+i)%1000), rng.nextDouble()*10000, (off+i)%50));
        return spark.createDataFrame(rows, new StructType()
            .add("id", DataTypes.IntegerType).add("name", DataTypes.StringType)
            .add("value", DataTypes.DoubleType).add("category", DataTypes.IntegerType));
    }
    static long t(Runnable r) { long s = System.nanoTime(); r.run(); return (System.nanoTime()-s)/1_000_000; }
    static void warmup(String catPath) {
        spark.sql("CREATE TABLE dl.main.warmup (id INT)");
        spark.sql("INSERT INTO dl.main.warmup VALUES (1)");
        spark.sql("SELECT * FROM dl.main.warmup").count();
        spark.sql("DROP TABLE dl.main.warmup");
    }
    static void createSQLiteCatalog(String p, String dp) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + p)) {
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE ducklake_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT)");
                s.execute("CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TEXT, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)");
                s.execute("CREATE TABLE ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)");
                s.execute("CREATE TABLE ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid TEXT, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN)");
                s.execute("CREATE TABLE ducklake_table(table_id BIGINT, table_uuid TEXT, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN)");
                s.execute("CREATE TABLE ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)");
                s.execute("CREATE TABLE ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT)");
                s.execute("CREATE TABLE ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR)");
                s.execute("CREATE TABLE ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)");
                s.execute("CREATE TABLE ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT)");
                s.execute("CREATE TABLE ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN)");
                s.execute("CREATE TABLE ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)");
                s.execute("CREATE TABLE ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)");
                s.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                s.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dp + "')");
                s.execute("INSERT INTO ducklake_snapshot VALUES (0, datetime('now'), 0, 1, 0)");
                s.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)");
                s.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', 1)");
            }
            c.commit();
        }
    }
}
