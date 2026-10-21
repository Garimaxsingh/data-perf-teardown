"""
E3: Format + column pruning — CSV (csv_flat) vs Parquet (parquet_flat),
touching 3 of 19 columns. Same query shape on both sides; CSV is row-based
so it must read every column's bytes regardless of what's SELECTed, while
Parquet's columnar layout lets Athena skip the other 16 columns entirely.

NOTE: this run requires the Athena workgroup's per-query scan cap
temporarily raised to 20GB (infra/athena.tf) — the full CSV table is
~17.82GB and would otherwise hit the same 10GB guardrail that blocked the
earlier row-count validation query (docs/RUNBOOK.md incident 3). Revert
the cap back to 10GB after this experiment runs.

Measure: bytes scanned via Athena, runtime, storage footprint (already
known from the build: CSV 17.82GB vs Parquet-flat 2.56GB, ~6.9x).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "setup"))
from athena_runner import run_experiment_query  # noqa: E402

QUERIES = {
    "csv_3col": """
        SELECT vendorid, avg(fare_amount) AS avg_fare, avg(trip_distance) AS avg_dist
        FROM csv_flat
        GROUP BY vendorid
    """,
    "parquet_3col": """
        SELECT vendorid, avg(fare_amount) AS avg_fare, avg(trip_distance) AS avg_dist
        FROM parquet_flat
        GROUP BY vendorid
    """,
}


def main() -> None:
    results = {}
    for label, sql in QUERIES.items():
        print(f"Running {label}...")
        results[label] = run_experiment_query("e3", label, sql)

    print("\n=== E3 Results ===")
    for label, r in results.items():
        if r.get("all_failed"):
            print(f"{label}: ALL RUNS FAILED")
            continue
        gb = r["median_bytes_scanned"] / 1e9
        cost = r["median_bytes_scanned"] / 1e12 * 5
        print(f"{label}: median {gb:.4f} GB scanned, {r['median_engine_ms']}ms engine time, ~${cost:.6f}")

    if all(not r.get("all_failed") for r in results.values()):
        csv_bytes = results["csv_3col"]["median_bytes_scanned"]
        pq_bytes = results["parquet_3col"]["median_bytes_scanned"]
        print(f"\nReduction: {csv_bytes / pq_bytes:.1f}x less data scanned with Parquet column pruning")
        print("Storage footprint (from build): CSV 17.82GB vs Parquet-flat 2.56GB (6.9x)")


if __name__ == "__main__":
    main()
