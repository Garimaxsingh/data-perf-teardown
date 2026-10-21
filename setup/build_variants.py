"""
Build the four physical variants defined in PROJECT_BRIEF.md §3 from the raw
downloaded TLC monthly files, using DuckDB to stream/spill rather than
loading 150-200M rows into pandas memory at once.

Schema reconciliation handled automatically by DuckDB's `union_by_name`:
  - passenger_count (DOUBLE in files through 2023-01, BIGINT from 2023-02
    onward) is promoted to a single DOUBLE column.
  - cbd_congestion_fee (absent before 2025-01, present from 2025-01 onward,
    confirmed via setup/inspect_schemas.py — genuinely absent, not just
    null) is unified as a nullable DOUBLE, NULL where the file lacks it.

Usage: python setup/build_variants.py <variant>
  variant in {C, B, D, A}

Build order matters per the brief: C first (partitioned — the "good"
version), then B/D/A can all be derived independently from the same base
query.
"""
import glob
import shutil
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "tlc_raw"
OUT_DIR = ROOT / "data" / "variants"
TMP_DIR = ROOT / "data" / "duckdb_tmp"


def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='2.5GB'")
    con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")
    con.execute("SET threads=2")
    return con


def base_query(files: list[str]) -> str:
    return f"""
    SELECT *, CAST(tpep_pickup_datetime AS DATE) AS pickup_date
    FROM read_parquet({files!r}, union_by_name=true)
    """


def build_c(con: duckdb.DuckDBPyConnection, files: list[str]) -> None:
    """Variant C: Parquet, partitioned by pickup_date — the 'good' version."""
    out = OUT_DIR / "parquet-partitioned"
    shutil.rmtree(out, ignore_errors=True)
    con.execute(f"""
        COPY ({base_query(files)}) TO '{out.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (pickup_date), OVERWRITE_OR_IGNORE true,
         COMPRESSION ZSTD)
    """)
    n_dirs = len(list(out.glob("*")))
    n_files = sum(1 for _ in out.rglob("*.parquet"))
    print(f"C (partitioned): {n_dirs} date partitions, {n_files} files")


def build_b(con: duckdb.DuckDBPyConnection, files: list[str]) -> None:
    """Variant B: Parquet, unpartitioned, properly sized (~128MB targets)."""
    out = OUT_DIR / "parquet-flat"
    shutil.rmtree(out, ignore_errors=True)
    sql = f"SELECT * EXCLUDE (pickup_date) FROM ({base_query(files)})"
    con.execute(f"""
        COPY ({sql}) TO '{out.as_posix()}'
        (FORMAT PARQUET, FILE_SIZE_BYTES '128MB', COMPRESSION ZSTD)
    """)
    fs = list(out.glob("*.parquet"))
    sizes = [f.stat().st_size for f in fs]
    print(f"B (flat, compacted): {len(fs)} files, avg {sum(sizes)/len(sizes)/1e6:.1f} MB")


def build_d(con: duckdb.DuckDBPyConnection, files: list[str]) -> None:
    """Variant D: Parquet, ~40,000 small files (~200KB each), unpartitioned.

    NOTE on the exact knob: DuckDB's FILE_SIZE_BYTES only rotates files at
    row-group boundaries, and row-group flush granularity itself snaps to
    multiples of the ~2048-row internal vector size (empirically calibrated
    against this dataset — see setup/inspect_schemas.py-adjacent scratch
    tests). ROW_GROUP_SIZE + a tiny FILE_SIZE_BYTES forces one row group per
    file. Actual rows/file still varies somewhat with thread scheduling, so
    threads is pinned to 1 here for more predictable output; the resulting
    file count is reported honestly rather than forced to exactly 40,000.
    """
    out = OUT_DIR / "parquet-smallfiles"
    shutil.rmtree(out, ignore_errors=True)
    con.execute("SET threads=1")
    sql = f"SELECT * EXCLUDE (pickup_date) FROM ({base_query(files)})"
    con.execute(f"""
        COPY ({sql}) TO '{out.as_posix()}'
        (FORMAT PARQUET, ROW_GROUP_SIZE 3000, FILE_SIZE_BYTES '1KB', COMPRESSION ZSTD)
    """)
    con.execute("SET threads=2")
    fs = list(out.glob("*.parquet"))
    sizes = [f.stat().st_size for f in fs]
    print(f"D (small files): {len(fs)} files, avg {sum(sizes)/len(sizes)/1024:.1f} KB")


def build_a(con: duckdb.DuckDBPyConnection, files: list[str]) -> None:
    """Variant A: CSV, unpartitioned — format/column-pruning baseline."""
    out = OUT_DIR / "csv-flat"
    shutil.rmtree(out, ignore_errors=True)
    sql = f"SELECT * EXCLUDE (pickup_date) FROM ({base_query(files)})"
    con.execute(f"""
        COPY ({sql}) TO '{out.as_posix()}'
        (FORMAT CSV, HEADER true, FILE_SIZE_BYTES '256MB')
    """)
    fs = list(out.glob("*.csv"))
    sizes = [f.stat().st_size for f in fs]
    print(f"A (CSV): {len(fs)} files, total {sum(sizes)/1e9:.2f} GB")


BUILDERS = {"C": build_c, "B": build_b, "D": build_d, "A": build_a}


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in BUILDERS:
        print(f"Usage: python {sys.argv[0]} <{'|'.join(BUILDERS)}>")
        sys.exit(1)

    variant = sys.argv[1]
    files = sorted(glob.glob(str(RAW_DIR / "yellow_tripdata_*.parquet")))
    print(f"Building variant {variant} from {len(files)} source files...")

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = get_connection()
    t0 = time.time()
    BUILDERS[variant](con, files)
    print(f"Done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
