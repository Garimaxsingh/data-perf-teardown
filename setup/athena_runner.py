"""
Shared Athena query runner for experiments E1-E3/E5/E6. Enforces the
project's rigor rules from PROJECT_BRIEF.md §5: three runs per query,
first (cold-cache) run discarded from the reported median but still
logged to results/raw_measurements.csv, runtime + bytes scanned captured
from Athena's own query execution statistics (the real dollar-cost figure
at $5/TB).
"""
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

REGION = "ap-south-1"
WORKGROUP = "data-perf-teardown-wg"
DATABASE = "data_perf_teardown"
RESULTS_CSV = Path(__file__).resolve().parent.parent / "results" / "raw_measurements.csv"

athena = boto3.client("athena", region_name=REGION)


def run_query_once(sql: str) -> dict:
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    while True:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    out = {"query_execution_id": qid, "state": state}
    if state == "SUCCEEDED":
        stats = resp["QueryExecution"]["Statistics"]
        out["bytes_scanned"] = stats.get("DataScannedInBytes", 0)
        out["engine_ms"] = stats.get("EngineExecutionTimeInMillis", 0)
        out["total_ms"] = stats.get("TotalExecutionTimeInMillis", 0)
    else:
        out["reason"] = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
    return out


def run_experiment_query(experiment: str, label: str, sql: str, n_runs: int = 3) -> dict:
    """Run a query n_runs times, log every run to raw_measurements.csv, return median stats."""
    runs = []
    for i in range(1, n_runs + 1):
        print(f"  [{experiment}/{label}] run {i}/{n_runs}...")
        r = run_query_once(sql)
        r["run"] = i
        runs.append(r)
        if r["state"] != "SUCCEEDED":
            print(f"    FAILED: {r.get('reason')}")

    ts = datetime.now(timezone.utc).isoformat()
    with open(RESULTS_CSV, "a", newline="") as f:
        w = csv.writer(f)
        for r in runs:
            if r["state"] == "SUCCEEDED":
                w.writerow([f"{experiment}_{label}", r["run"], "bytes_scanned", r["bytes_scanned"], "bytes", ts])
                w.writerow([f"{experiment}_{label}", r["run"], "engine_time", r["engine_ms"], "ms", ts])
            else:
                w.writerow([f"{experiment}_{label}", r["run"], "failed", r.get("reason", ""), "error", ts])

    succeeded = [r for r in runs if r["state"] == "SUCCEEDED"]
    if not succeeded:
        return {"label": label, "all_failed": True, "runs": runs}

    # Rigor rule: discard first (cold-cache) run from the reported median,
    # but it's still in the CSV above.
    kept = succeeded[1:] if len(succeeded) > 1 else succeeded
    kept_bytes = sorted(r["bytes_scanned"] for r in kept)
    kept_ms = sorted(r["engine_ms"] for r in kept)
    median_bytes = kept_bytes[len(kept_bytes) // 2]
    median_ms = kept_ms[len(kept_ms) // 2]

    return {
        "label": label,
        "median_bytes_scanned": median_bytes,
        "median_engine_ms": median_ms,
        "all_runs": runs,
    }
