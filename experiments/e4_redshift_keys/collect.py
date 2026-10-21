"""
E4 collector: pulls the per-run EXPLAIN (ANALYZE, BUFFERS) results that
e4_run.sh uploaded to s3://<bucket>/e4-results/, saves the raw EXPLAIN
JSON and table sizes into results/e4/ (committed alongside the numbers),
and appends every run to results/raw_measurements.csv.

Usage: python experiments/e4_redshift_keys/collect.py
"""
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3

REGION = "ap-south-1"
BUCKET = "data-perf-teardown-29e0bb3c"
PREFIX = "e4-results/"
ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "results" / "e4"
RESULTS_CSV = ROOT / "results" / "raw_measurements.csv"


def main() -> None:
    s3 = boto3.client("s3", region_name=REGION)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", []):
        name = obj["Key"][len(PREFIX):]
        if name and not name.endswith(".out"):
            s3.download_file(BUCKET, obj["Key"], str(OUT_DIR / name))

    sizes = dict(line.split(",", 1) for line in (OUT_DIR / "sizes.csv").read_text().split())
    print("Sizes:", json.dumps(sizes, indent=2))

    ts = datetime.now(timezone.utc).isoformat()
    rows = []
    print(f"\n{'table':<16}{'run':>4}{'exec_ms':>12}{'read_blk':>12}{'hit_blk':>10}  plan")
    for line in (OUT_DIR / "results.jsonl").read_text().splitlines():
        r = json.loads(line)
        label = f"e4_{r['table']}"
        print(f"{r['table']:<16}{r['run']:>4}{r['execution_ms']:>12.1f}{r['shared_read_blocks']:>12,}"
              f"{r['shared_hit_blocks']:>10,}  {' > '.join(r['nodes'])}")
        rows += [
            [label, r["run"], "execution_time", f"{r['execution_ms']:.3f}", "ms", ts],
            [label, r["run"], "shared_read_blocks", r["shared_read_blocks"], "blocks", ts],
            [label, r["run"], "shared_hit_blocks", r["shared_hit_blocks"], "blocks", ts],
        ]
    with open(RESULTS_CSV, "a", newline="") as f:
        csv.writer(f).writerows(rows)
    print(f"\nAppended {len(rows)} rows to {RESULTS_CSV}")


if __name__ == "__main__":
    main()
