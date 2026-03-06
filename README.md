<p align="center">
  <h1 align="center">🦆 DuckLake Spark</h1>
  <p align="center">
    Native Java Spark connector for <a href="https://ducklake.select/">DuckLake</a> catalogs
  </p>
</p>

<p align="center">
  <a href="https://github.com/PeterVanHolland/ducklake-spark/actions"><img src="https://github.com/PeterVanHolland/ducklake-spark/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
  <a href="https://www.java.com"><img src="https://img.shields.io/badge/Java-11%2B-blue.svg" alt="Java 11+"></a>
  <a href="https://spark.apache.org"><img src="https://img.shields.io/badge/Spark-3.5%2B-orange.svg" alt="Spark 3.5+"></a>
</p>

---

Read and write [DuckLake](https://ducklake.select/) tables directly from [Apache Spark](https://spark.apache.org/) — no DuckDB runtime required. Reads catalog metadata via JDBC (SQLite or PostgreSQL) and operates on the underlying Parquet data files through Spark's native Parquet reader/writer.

## Quick Start

### DataFrame API

```java
// Read
Dataset<Row> df = spark.read()
    .format("ducklake")
    .option("catalog", "/path/to/catalog.ducklake")
    .option("table", "my_table")
    .load();

// Write
df.write()
    .format("ducklake")
    .option("catalog", "/path/to/catalog.ducklake")
    .option("table", "my_table")
    .mode("append")
    .save();
```

### PySpark

```python
# Read
df = spark.read \
    .format("ducklake") \
    .option("catalog", "/path/to/catalog.ducklake") \
    .option("table", "my_table") \
    .load()

# Write
df.write \
    .format("ducklake") \
    .option("catalog", "/path/to/catalog.ducklake") \
    .option("table", "my_table") \
    .mode("append") \
    .save()
```

### CatalogPlugin (SQL DDL)

Register DuckLake as a Spark catalog and use standard SQL:

```sql
SET spark.sql.catalog.ducklake = io.ducklake.spark.catalog.DuckLakeCatalog;
SET spark.sql.catalog.ducklake.catalog = /path/to/catalog.ducklake;

CREATE TABLE ducklake.main.t (id INT, name STRING);
INSERT INTO ducklake.main.t VALUES (1, 'hello');
SELECT * FROM ducklake.main.t;
```

This gives you full DDL support — `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, `CREATE SCHEMA`, and namespace operations all work through Spark SQL.

## Features

| Feature | Status | Notes |
|---------|--------|-------|
| Batch Read/Write | ✅ | Append and overwrite modes |
| Structured Streaming Sink | ✅ | Micro-batch with exactly-once semantics |
| CatalogPlugin (SQL DDL) | ✅ | CREATE/DROP/ALTER TABLE, namespaces |
| Predicate Pushdown | ✅ | File-level pruning via zone maps (min/max stats) |
| Column Pruning | ✅ | Only reads requested columns from Parquet |
| Time Travel | ✅ | Query by snapshot version or timestamp |
| Schema Evolution | ✅ | field_id-based column tracking across renames |
| Row-level Delete/Update | ✅ | Position-delete files with row filtering |
| Compaction | ✅ | `rewriteDataFiles` — merge small files, apply deletion vectors |
| expire_snapshots | ✅ | Remove old snapshot metadata |
| vacuum | ✅ | Physically delete orphaned data files |
| Deletion Vectors | ✅ | Full read/write support for position-delete files |

## Configuration Reference

### DataSource Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `catalog` | Yes | — | Path to `.ducklake` SQLite file or PostgreSQL connection string (`jdbc:postgresql://...`) |
| `table` | Yes* | — | Table name to read/write (*not needed with CatalogPlugin) |
| `schema` | No | `main` | Schema (namespace) containing the table |
| `data_path` | No | from catalog | Override base path for Parquet data files |
| `snapshot_version` | No | latest | Read at a specific snapshot version (time travel) |
| `snapshot_time` | No | latest | Read at a specific timestamp, e.g. `2026-01-01T00:00:00` (time travel) |

### PostgreSQL Backend

```python
df = spark.read \
    .format("ducklake") \
    .option("catalog", "jdbc:postgresql://localhost/mydb") \
    .option("table", "events") \
    .load()
```

### Time Travel

```java
// Read at snapshot version 3
Dataset<Row> df = spark.read()
    .format("ducklake")
    .option("catalog", "/path/to/catalog.ducklake")
    .option("table", "my_table")
    .option("snapshot_version", "3")
    .load();

// Read at a specific point in time
Dataset<Row> df = spark.read()
    .format("ducklake")
    .option("catalog", "/path/to/catalog.ducklake")
    .option("table", "my_table")
    .option("snapshot_time", "2026-01-01T00:00:00")
    .load();
```

## Maintenance API

DuckLake Spark provides three maintenance operations for table lifecycle management:

```java
import io.ducklake.spark.maintenance.DuckLakeMaintenance;

// Compact small files and apply pending deletion vectors
DuckLakeMaintenance.rewriteDataFiles(spark, catalogPath, tableName, "main");

// Remove snapshot metadata older than 30 days (keeps latest)
DuckLakeMaintenance.expireSnapshots(spark, catalogPath, 30);

// Physically delete orphaned data files no longer referenced by any snapshot
DuckLakeMaintenance.vacuum(spark, catalogPath);
```

**Recommended workflow:** compact → expire snapshots → vacuum. Compaction rewrites files and creates clean snapshots; expiring removes old metadata; vacuum reclaims disk space.

## Architecture

```
┌─────────────┐     ┌──────────────────────┐     ┌─────────────┐
│  Spark SQL   │────▶│  DuckLakeDataSource   │────▶│  Parquet     │
│  (DataFrame) │     │  (DataSource V2)      │     │  Data Files  │
└─────────────┘     └──────────┬───────────┘     └─────────────┘
                               │
                    ┌──────────▼───────────┐
                    │  DuckLakeMetadata     │
                    │  Backend (JDBC)       │
                    │  ┌─────────────────┐  │
                    │  │ SQLite / PG     │  │
                    │  │ (catalog.ducklake)│  │
                    │  └─────────────────┘  │
                    └──────────────────────┘
```

**No DuckDB dependency.** The connector talks directly to the DuckLake catalog database (SQLite or PostgreSQL) over JDBC and reads/writes standard Parquet files. This means you can use DuckLake tables in Spark without installing DuckDB — the catalog is just a SQL database and the data is just Parquet.

### Key Components

| Class | Role |
|-------|------|
| `DuckLakeDataSource` | Spark `TableProvider` entry point (`format("ducklake")`) |
| `DuckLakeCatalog` | Spark `CatalogPlugin` for SQL DDL support |
| `DuckLakeMetadataBackend` | JDBC-based catalog reader/writer (SQLite & PostgreSQL) |
| `DuckLakeScanBuilder` | Plans scans with column pruning and filter pushdown |
| `DuckLakeScan` | Maps DuckLake data files to Spark `InputPartition`s |
| `DuckLakePartitionReader` | Reads Parquet files with delete file and schema evolution support |
| `DuckLakeWriteBuilder` | Plans writes with append/overwrite support |
| `DuckLakeBatchWrite` | Coordinates batch writes and catalog commits |
| `DuckLakeStreamingWrite` | Manages streaming micro-batch writes with snapshot-per-epoch |
| `DuckLakeMaintenance` | Compaction, snapshot expiry, and vacuum operations |
| `DuckLakeTypeMapping` | DuckDB ↔ Spark type conversion |

## Building

```bash
# Requirements: JDK 11+, Maven 3.6+
mvn clean package

# Run tests
mvn clean test

# The shaded JAR includes the SQLite JDBC driver
ls target/ducklake-spark-0.1.0-SNAPSHOT.jar
```

### Adding to Spark

```bash
# spark-submit
spark-submit --jars ducklake-spark-0.1.0-SNAPSHOT.jar my_app.py

# spark-shell
spark-shell --jars ducklake-spark-0.1.0-SNAPSHOT.jar

# pyspark
pyspark --jars ducklake-spark-0.1.0-SNAPSHOT.jar
```

## Compatibility

| Component | Version |
|-----------|---------|
| Java | 11+ |
| Apache Spark | 3.5.x |
| Scala | 2.12 / 2.13 |
| DuckLake catalog | v0.4 |
| Parquet | 1.15.x |

## How It Compares

DuckLake Spark occupies a similar niche to [PyIceberg](https://py.iceberg.apache.org/) — a lightweight, standalone connector that reads and writes a lakehouse format without requiring a full query engine or metastore.

| | DuckLake Spark | PyIceberg + Spark | Iceberg Spark Runtime |
|---|---|---|---|
| **Catalog** | SQLite file or PostgreSQL | REST / Hive / Glue | REST / Hive / Glue / JDBC |
| **Setup** | Single JAR, single `catalog` option | Python + Java bridge, catalog config | JARs + metastore setup |
| **Metastore** | None (catalog *is* the metadata) | Required (REST, Hive, etc.) | Required |
| **Schema evolution** | field_id-based, automatic | field_id-based, automatic | field_id-based, automatic |
| **DuckDB interop** | Native (same catalog format) | Via Iceberg REST | Via Iceberg REST |

DuckLake's advantage: point it at a `.ducklake` file and go. No metastore, no REST catalog, no infrastructure beyond the catalog file itself.

## Known Limitations

- **Catalog version gap** — DuckDB currently writes catalog version v0.3; this connector targets v0.4. Tables created with an older DuckDB may need a catalog migration.
- **Complex types** — `ARRAY`, `STRUCT`, and `MAP` columns are supported for type mapping but have limited read/write coverage in some edge cases.
- **Cloud storage** — S3/GCS/Azure paths require appropriate Hadoop filesystem configuration in your Spark session.

## Related Projects

- [DuckLake](https://ducklake.select/) — the DuckLake table format
- [DuckDB](https://duckdb.org/) — the native DuckLake implementation
- [ducklake-dataframe](https://github.com/pdet/ducklake-polars) — Pure Python Polars/Pandas integration

## License

[MIT](LICENSE)
