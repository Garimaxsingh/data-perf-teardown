"""
E6: Iceberg stats pruning — the same highly selective filter
(pulocationid = 1, Newark Airport pickups) on four layouts of the same
167,858,646 rows:

  parquet_flat      plain Parquet, as landed (time-ordered, NOT clustered on pulocationid)
  iceberg_unsorted  Iceberg, 64 files, rows round-robin — stats exist, can't exclude anything
  iceberg_sorted    Iceberg, 64 files, range-sorted on pulocationid — manifest stats prune files
  parquet_sorted    plain Parquet, identical sorted layout, no Iceberg (control)

The three new layouts are written by jobs/e6_build_iceberg.py (Glue), which
also reports Iceberg's planning-time file counts. This script registers
parquet_sorted in the Glue catalog (schema copied from parquet_flat, new
location), then measures Athena bytes scanned + engine time for each.

Measure: bytes scanned via Athena, runtime; files planned (from the Glue job).
"""
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "setup"))
from athena_runner import DATABASE, REGION, run_experiment_query  # noqa: E402

BUCKET = "data-perf-teardown-29e0bb3c"
VARIANTS = ["parquet_flat", "iceberg_unsorted", "iceberg_sorted", "parquet_sorted"]
QUERY = """
    SELECT count(*) AS trips, avg(fare_amount) AS avg_fare
    FROM {table}
    WHERE pulocationid = 1
"""


def ensure_parquet_sorted_table() -> None:
    glue = boto3.client("glue", region_name=REGION)
    try:
        glue.get_table(DatabaseName=DATABASE, Name="parquet_sorted")
        return
    except glue.exceptions.EntityNotFoundException:
        pass
    src = glue.get_table(DatabaseName=DATABASE, Name="parquet_flat")["Table"]
    sd = dict(src["StorageDescriptor"])
    sd["Location"] = f"s3://{BUCKET}/optimized/parquet-sorted/"
    glue.create_table(
        DatabaseName=DATABASE,
        TableInput={
            "Name": "parquet_sorted",
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet"},
            "StorageDescriptor": {k: sd[k] for k in (
                "Columns", "Location", "InputFormat", "OutputFormat", "SerdeInfo", "Compressed"
            ) if k in sd},
        },
    )
    print("Registered parquet_sorted in the Glue catalog")


def main() -> None:
    ensure_parquet_sorted_table()
    results = {}
    for table in VARIANTS:
        print(f"Running {table}...")
        results[table] = run_experiment_query("e6", table, QUERY.format(table=table))

    print("\n=== E6 Results ===")
    for table, r in results.items():
        if r.get("all_failed"):
            print(f"{table}: ALL RUNS FAILED")
            continue
        mb = r["median_bytes_scanned"] / 1e6
        print(f"{table}: median {mb:.2f} MB scanned, {r['median_engine_ms']}ms engine time")


if __name__ == "__main__":
    main()
