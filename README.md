# ducklake-spark

> **This project is a proof of concept. It was 100% written by [Claude Code](https://docs.anthropic.com/en/docs/build-with-claude/claude-code/overview) (Anthropic's AI coding agent). It is not intended for production use.**

Native Java [Spark](https://spark.apache.org/) extension for reading [DuckLake](https://ducklake.select/) catalogs. Reads DuckLake metadata directly from SQLite or PostgreSQL and scans the underlying Parquet data files through Spark's native Parquet reader. **No DuckDB runtime dependency.**

## Features

- **Spark DataSource V2** — native integration with `spark.read.format("ducklake")`
- **Column pruning** — only reads requested columns from Parquet files
- **Filter pushdown** — file-level pruning using catalog column statistics
- **Schema evolution** — handles column renames, added columns, dropped columns via name mappings
- **Delete file support** — applies position-delete files (DuckLake's Iceberg-compatible format)
- **Multiple backends** — SQLite (default) and PostgreSQL catalog databases
- **Type mapping** — full DuckDB → Spark type conversion (integers, decimals, structs, lists, maps)

## Usage

### Spark SQL (DataFrame API)

```java
Dataset<Row> df = spark.read()
    .format("ducklake")
    .option("catalog", "/path/to/catalog.ducklake")
    .option("table", "my_table")
    .load();

df.show();
```

### PySpark

```python
df = spark.read \
    .format("ducklake") \
    .option("catalog", "/path/to/catalog.ducklake") \
    .option("table", "my_table") \
    .load()

df.show()
```

### Options

| Option | Required | Description |
|--------|----------|-------------|
| `catalog` | Yes | Path to `.ducklake` SQLite file or PostgreSQL connection string |
| `table` | Yes | Table name to read |
| `schema` | No | Schema name (default: `main`) |
| `data_path` | No | Override data file base path (default: from catalog metadata) |

### PostgreSQL Backend

```python
df = spark.read \
    .format("ducklake") \
    .option("catalog", "jdbc:postgresql://localhost/mydb") \
    .option("table", "events") \
    .load()
```

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

### Components

| Class | Role |
|-------|------|
| `DuckLakeDataSource` | Spark `TableProvider` entry point |
| `DuckLakeMetadataBackend` | JDBC-based catalog reader (SQLite/PostgreSQL) |
| `DuckLakeScanBuilder` | Plans scans with column pruning and filter pushdown |
| `DuckLakeScan` | Maps DuckLake data files to Spark `InputPartition`s |
| `DuckLakePartitionReader` | Reads Parquet files with delete file and rename support |
| `DuckLakeTypeMapping` | DuckDB → Spark type conversion |

## Building

```bash
# Requires JDK 11+ and Maven
mvn clean package

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
| Spark | 3.5.x |
| Java | 11+ |
| Scala | 2.12 / 2.13 |
| DuckLake catalog | v0.3 |
| Parquet | 1.15.x |

## Limitations (POC)

- **Read-only** — no write support yet (use DuckDB or ducklake-dataframe for writes)
- **No inlined data** — tables with data inlined in the catalog are not yet supported
- **No streaming** — batch reads only
- **No Spark Catalog integration** — uses `format("ducklake")` rather than `USING ducklake` in SQL
- **Local files only** — S3/GCS/Azure paths not yet supported (needs Hadoop filesystem configuration)

## Related Projects

- [DuckLake](https://ducklake.select/) — the DuckLake table format specification
- [ducklake-dataframe](https://github.com/pdet/ducklake-polars) — Pure Python Polars/Pandas integration
- [DuckDB](https://duckdb.org/) — the native DuckLake implementation

## License

MIT
