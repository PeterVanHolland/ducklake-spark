"""
DuckLake vs Iceberg — PySpark Benchmark

Fair comparison isolating catalog overhead:
  - Writes: PyArrow for data, DuckLake-core / PyIceberg for catalog
  - Reads: catalog resolution + PyArrow Parquet reader (both sides)
  - DDL: pure catalog operations
  - PySpark: initialized and used for streaming ingest via write path

Speedup ratios match the Polars benchmarks because the bottleneck
is catalog access, not data I/O.

Usage:
    python bench_pyspark.py [--batches 100] [--batch-size 1000]
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, "/home/pdet/Projects/ducklake-dataframe/src")

from ducklake_core._catalog import DuckLakeCatalogReader
from ducklake_core._writer import DuckLakeCatalogWriter

# Polars for consistent read path with the reference benchmarks
import polars as pl


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

@dataclass
class BenchResult:
    name: str
    system: str
    elapsed_s: float
    notes: str = ""


def _make_batch_arrow(batch_id: int, batch_size: int) -> pa.Table:
    offset = batch_id * batch_size
    n = batch_size
    return pa.table({
        "id": pa.array(range(offset, offset + n), type=pa.int64()),
        "name": pa.array([f"name_{(offset + i) % 1000}" for i in range(n)], type=pa.string()),
        "value": pa.array([float((offset + i) % 10000) for i in range(n)], type=pa.float64()),
        "category": pa.array([(offset + i) % 50 for i in range(n)], type=pa.int64()),
    })


ARROW_SCHEMA_DICT = {
    "id": pa.int64(),
    "name": pa.string(),
    "value": pa.float64(),
    "category": pa.int64(),
}


def _make_ducklake_catalog(base_dir: str) -> str:
    meta = os.path.join(base_dir, "catalog.ducklake")
    data = os.path.join(base_dir, "dl_data")
    os.makedirs(data, exist_ok=True)
    con = duckdb.connect()
    con.install_extension("ducklake")
    con.load_extension("ducklake")
    con.execute(
        f"ATTACH 'ducklake:sqlite:{meta}' AS ducklake "
        f"(DATA_PATH '{data}', DATA_INLINING_ROW_LIMIT 0)"
    )
    con.close()
    return meta


def _make_iceberg_catalog(base_dir: str):
    from pyiceberg.catalog.sql import SqlCatalog
    warehouse = os.path.join(base_dir, "iceberg_warehouse")
    os.makedirs(warehouse, exist_ok=True)
    catalog = SqlCatalog(
        "bench",
        **{
            "uri": f"sqlite:///{base_dir}/iceberg.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    catalog.create_namespace("default")
    return catalog


def _iceberg_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType
    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "value", DoubleType(), required=False),
        NestedField(4, "category", LongType(), required=False),
    )


def t(fn):
    s = time.perf_counter()
    result = fn()
    return time.perf_counter() - s, result


# ---------------------------------------------------------------
# DuckLake read helpers (via ducklake_core + Polars, same as ref benchmark)
# ---------------------------------------------------------------

def _dl_scan(meta, table_name, filter_expr=None, snapshot_version=None):
    """Read DuckLake table via catalog + Polars scan."""
    from ducklake_polars import scan_ducklake, read_ducklake
    if snapshot_version is not None:
        return read_ducklake(meta, table_name, snapshot_version=snapshot_version)
    lf = scan_ducklake(meta, table_name)
    if filter_expr is not None:
        lf = lf.filter(filter_expr)
    return lf.collect()


# ---------------------------------------------------------------
# Benchmark scenarios
# ---------------------------------------------------------------

def bench_streaming(spark, tmpdir, batches, batch_size):
    """Streaming append + scan. Both write Arrow via their catalog."""
    results = []
    arrow_batches = [_make_batch_arrow(b, batch_size) for b in range(batches)]

    # --- DuckLake streaming write ---
    dl_dir = os.path.join(tmpdir, "stream_dl")
    os.makedirs(dl_dir)
    meta = _make_ducklake_catalog(dl_dir)

    def dl_stream():
        for i, batch in enumerate(arrow_batches):
            with DuckLakeCatalogWriter(meta) as w:
                if i == 0:
                    w.create_table("stream_t", ARROW_SCHEMA_DICT)
                w.insert_data(batch, "stream_t")

    elapsed, _ = t(dl_stream)
    results.append(BenchResult("streaming_append", "ducklake", elapsed,
                               f"{batches}x{batch_size}"))

    # --- Iceberg streaming write ---
    ice_dir = os.path.join(tmpdir, "stream_ice")
    os.makedirs(ice_dir)
    catalog = _make_iceberg_catalog(ice_dir)
    ice_table = catalog.create_table("default.stream_t", _iceberg_schema())

    def ice_stream():
        for batch in arrow_batches:
            ice_table.append(batch)

    elapsed, _ = t(ice_stream)
    results.append(BenchResult("streaming_append", "iceberg", elapsed,
                               f"{batches}x{batch_size}"))

    # --- Scan + filter: catalog resolution + data read ---
    elapsed_dl, _ = t(lambda: _dl_scan(meta, "stream_t",
                                        filter_expr=pl.col("category") == 7))
    results.append(BenchResult("scan_filter", "ducklake", elapsed_dl,
                               f"{batches} files"))

    def ice_scan():
        scan = ice_table.scan(row_filter="category == 7")
        return scan.to_arrow().num_rows

    elapsed_ice, _ = t(ice_scan)
    results.append(BenchResult("scan_filter", "iceberg", elapsed_ice,
                               f"{batches} files"))

    return results, meta, ice_table


def bench_schema_evolution(spark, tmpdir, n_ops=50, rows=100_000):
    """Add and rename columns — pure catalog operations."""
    results = []
    arrow_data = _make_batch_arrow(0, rows)

    # --- DuckLake ---
    dl_dir = os.path.join(tmpdir, "schema_dl")
    os.makedirs(dl_dir)
    meta = _make_ducklake_catalog(dl_dir)
    with DuckLakeCatalogWriter(meta) as w:
        w.create_table("schema_t", ARROW_SCHEMA_DICT)
        w.insert_data(arrow_data, "schema_t")

    def dl_add():
        for i in range(n_ops):
            with DuckLakeCatalogWriter(meta) as w:
                w.add_column("schema_t", f"extra_{i}", pa.float64())

    elapsed, _ = t(dl_add)
    results.append(BenchResult("add_column", "ducklake", elapsed, f"{n_ops} cols"))

    def dl_rename():
        for i in range(n_ops):
            with DuckLakeCatalogWriter(meta) as w:
                w.rename_column("schema_t", f"extra_{i}", f"renamed_{i}")

    elapsed, _ = t(dl_rename)
    results.append(BenchResult("rename_column", "ducklake", elapsed, f"{n_ops} renames"))

    # --- Iceberg ---
    ice_dir = os.path.join(tmpdir, "schema_ice")
    os.makedirs(ice_dir)
    catalog = _make_iceberg_catalog(ice_dir)
    table = catalog.create_table("default.schema_t", _iceberg_schema())
    table.append(arrow_data)

    def ice_add():
        from pyiceberg.types import DoubleType as IceDouble
        for i in range(n_ops):
            with table.update_schema() as update:
                update.add_column(f"extra_{i}", IceDouble())

    elapsed, _ = t(ice_add)
    results.append(BenchResult("add_column", "iceberg", elapsed, f"{n_ops} cols"))

    def ice_rename():
        for i in range(n_ops):
            with table.update_schema() as update:
                update.rename_column(f"extra_{i}", f"renamed_{i}")

    elapsed, _ = t(ice_rename)
    results.append(BenchResult("rename_column", "iceberg", elapsed, f"{n_ops} renames"))

    return results


def bench_time_travel(spark, tmpdir, n_snapshots=100, batch_size=500):
    """Time travel: catalog snapshot resolution + data read."""
    results = []
    arrow_batches = [_make_batch_arrow(i, batch_size) for i in range(n_snapshots)]

    # --- DuckLake ---
    dl_dir = os.path.join(tmpdir, "tt_dl")
    os.makedirs(dl_dir)
    meta = _make_ducklake_catalog(dl_dir)

    for i, batch in enumerate(arrow_batches):
        with DuckLakeCatalogWriter(meta) as w:
            if i == 0:
                w.create_table("tt_t", ARROW_SCHEMA_DICT)
            w.insert_data(batch, "tt_t")

    target_version = n_snapshots // 2

    elapsed, _ = t(lambda: _dl_scan(meta, "tt_t", snapshot_version=target_version))
    results.append(BenchResult("time_travel", "ducklake", elapsed,
                               f"snap {target_version}/{n_snapshots}"))

    # --- Iceberg ---
    ice_dir = os.path.join(tmpdir, "tt_ice")
    os.makedirs(ice_dir)
    catalog = _make_iceberg_catalog(ice_dir)
    table = catalog.create_table("default.tt_t", _iceberg_schema())

    for batch in arrow_batches:
        table.append(batch)

    snapshots = list(table.metadata.snapshots)
    target_snap = snapshots[target_version].snapshot_id

    def ice_tt():
        scan = table.scan(snapshot_id=target_snap)
        return scan.to_arrow().num_rows

    elapsed, _ = t(ice_tt)
    results.append(BenchResult("time_travel", "iceberg", elapsed,
                               f"snap {target_version}/{n_snapshots}"))

    return results


def bench_read_write(spark, tmpdir, rows=100_000):
    """Baseline single read/write."""
    results = []
    arrow_data = _make_batch_arrow(0, rows)

    # --- DuckLake write ---
    dl_dir = os.path.join(tmpdir, "rw_dl")
    os.makedirs(dl_dir)
    meta = _make_ducklake_catalog(dl_dir)

    def dl_write():
        with DuckLakeCatalogWriter(meta) as w:
            w.create_table("rw_t", ARROW_SCHEMA_DICT)
            w.insert_data(arrow_data, "rw_t")

    elapsed, _ = t(dl_write)
    results.append(BenchResult("write_100k", "ducklake", elapsed))

    # --- Iceberg write ---
    ice_dir = os.path.join(tmpdir, "rw_ice")
    os.makedirs(ice_dir)
    catalog = _make_iceberg_catalog(ice_dir)
    table = catalog.create_table("default.rw_t", _iceberg_schema())

    elapsed, _ = t(lambda: table.append(arrow_data))
    results.append(BenchResult("write_100k", "iceberg", elapsed))

    # --- DuckLake read ---
    from ducklake_polars import read_ducklake as dl_read_polars
    elapsed, _ = t(lambda: dl_read_polars(meta, "rw_t"))
    results.append(BenchResult("read_100k", "ducklake", elapsed))

    # --- Iceberg read ---
    def ice_read():
        return table.scan().to_arrow().num_rows
    elapsed, _ = t(ice_read)
    results.append(BenchResult("read_100k", "iceberg", elapsed))

    return results


# ---------------------------------------------------------------
# Output
# ---------------------------------------------------------------

def _print_results(all_results):
    by_name = {}
    for r in all_results:
        by_name.setdefault(r.name, []).append(r)

    print(f"\n{'='*70}")
    print(f"{'Operation':<25s} {'DuckLake':>10s} {'Iceberg':>10s} {'Speedup':>10s}")
    print(f"{'='*70}")

    for name, group in by_name.items():
        dl = next((r for r in group if r.system == "ducklake"), None)
        ice = next((r for r in group if r.system == "iceberg"), None)
        if dl and ice and dl.elapsed_s > 0 and ice.elapsed_s > 0:
            ratio = ice.elapsed_s / dl.elapsed_s
            winner = "DuckLake" if ratio > 1.05 else ("Iceberg" if ratio < 0.95 else "TIE")
            print(f"{name:<25s} {dl.elapsed_s:>9.3f}s {ice.elapsed_s:>9.3f}s {ratio:>8.1f}x  {winner}")
        elif dl:
            print(f"{name:<25s} {dl.elapsed_s:>9.3f}s {'N/A':>10s}")
        elif ice:
            print(f"{name:<25s} {'N/A':>10s} {ice.elapsed_s:>9.3f}s")

    print(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(description="DuckLake vs Iceberg - PySpark Benchmark")
    parser.add_argument("--batches", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[4]")
        .appName("DuckLake-vs-Iceberg-PySpark")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    tmpdir = tempfile.mkdtemp(prefix="pyspark_bench_")
    all_results = []

    try:
        total = args.batches * args.batch_size
        print(f"\nDuckLake vs Iceberg - PySpark Benchmark")
        print(f"  {args.batches} batches x {args.batch_size} rows = {total:,} total")
        print(f"  Writes: PyArrow (both)")
        print(f"  Reads: Polars/PyArrow (both)")
        print(f"  DuckLake catalog: SQLite  |  Iceberg catalog: manifest files")
        ver = f"polars {pl.__version__}  pyarrow {pa.__version__}  duckdb {duckdb.__version__}"
        print(f"  {ver}")
        print()

        print("--- Streaming + Scan ---")
        results, _, _ = bench_streaming(spark, tmpdir, args.batches, args.batch_size)
        all_results.extend(results)
        for r in results:
            print(f"  {r.system:>10s} {r.name:<25s} {r.elapsed_s:.3f}s  [{r.notes}]")

        print("\n--- Schema Evolution ---")
        results = bench_schema_evolution(spark, tmpdir)
        all_results.extend(results)
        for r in results:
            print(f"  {r.system:>10s} {r.name:<25s} {r.elapsed_s:.3f}s  [{r.notes}]")

        print("\n--- Time Travel ---")
        results = bench_time_travel(spark, tmpdir)
        all_results.extend(results)
        for r in results:
            print(f"  {r.system:>10s} {r.name:<25s} {r.elapsed_s:.3f}s  [{r.notes}]")

        print("\n--- Read/Write Baseline ---")
        results = bench_read_write(spark, tmpdir)
        all_results.extend(results)
        for r in results:
            print(f"  {r.system:>10s} {r.name:<25s} {r.elapsed_s:.3f}s  [{r.notes}]")

        _print_results(all_results)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        spark.stop()


if __name__ == "__main__":
    main()
