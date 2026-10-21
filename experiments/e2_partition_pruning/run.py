"""
E2: Partition pruning — flat Parquet (parquet_flat) vs Parquet partitioned
by pickup_date (parquet_partitioned). Same predicate semantics on both
sides: filter to a single day, aggregate fare_amount. The flat table has
no pickup_date column (excluded at build time), so its filter goes through
tpep_pickup_datetime instead — this is deliberate: it's exactly the
real-world "you didn't pre-compute a partition column" case E2 is about.

Measure: bytes scanned via Athena (the real dollar figure, $5/TB), runtime.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "setup"))
from athena_runner import run_experiment_query  # noqa: E402

TARGET_DAY = "2024-06-15"

QUERIES = {
    "flat_no_pruning": f"""
        SELECT count(*) AS trips, avg(fare_amount) AS avg_fare
        FROM parquet_flat
        WHERE CAST(tpep_pickup_datetime AS DATE) = DATE '{TARGET_DAY}'
    """,
    "partitioned_pruned": f"""
        SELECT count(*) AS trips, avg(fare_amount) AS avg_fare
        FROM parquet_partitioned
        WHERE pickup_date = '{TARGET_DAY}'
    """,
}


def main() -> None:
    results = {}
    for label, sql in QUERIES.items():
        print(f"Running {label}...")
        results[label] = run_experiment_query("e2", label, sql)

    print("\n=== E2 Results ===")
    for label, r in results.items():
        if r.get("all_failed"):
            print(f"{label}: ALL RUNS FAILED")
            continue
        gb = r["median_bytes_scanned"] / 1e9
        cost = r["median_bytes_scanned"] / 1e12 * 5  # $5/TB
        print(f"{label}: median {gb:.4f} GB scanned, {r['median_engine_ms']}ms engine time, ~${cost:.6f}")

    if all(not r.get("all_failed") for r in results.values()):
        flat_bytes = results["flat_no_pruning"]["median_bytes_scanned"]
        part_bytes = results["partitioned_pruned"]["median_bytes_scanned"]
        if part_bytes > 0:
            print(f"\nReduction: {flat_bytes / part_bytes:.1f}x less data scanned with partition pruning")


if __name__ == "__main__":
    main()
