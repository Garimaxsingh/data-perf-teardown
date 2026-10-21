"""
E5 collector: turns one run of the Glue job `data-perf-teardown-e5-join-strategy`
into per-run numbers in results/raw_measurements.csv.

Two sources, both produced by the job itself:
  1. Spark event log (s3://<bucket>/spark-event-logs/e5/) — the same JSON
     event stream the Spark UI renders its DAG/stage pages from. Each run
     ran under its own Spark job group (e5_<variant>_run<N>), so every
     completed stage is attributed to exactly one run via
     SparkListenerJobStart's "spark.jobGroup.id" property. Shuffle
     read/write bytes come from each completed stage's aggregated task
     metrics (internal.metrics.shuffle.*). Stages that AQE planned but
     skipped never emit SparkListenerStageCompleted, so they aren't counted.
  2. The job's stdout in CloudWatch (RESULT|... lines) — wall-clock time
     per run, measured inside the driver around .collect().

Usage: python experiments/e5_join_strategy/collect.py <job_run_id>
"""
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3

REGION = "ap-south-1"
BUCKET = "data-perf-teardown-29e0bb3c"
EVENT_LOG_PREFIX = "spark-event-logs/e5/"
JOB_NAME = "data-perf-teardown-e5-join-strategy"
RESULTS_CSV = Path(__file__).resolve().parents[2] / "results" / "raw_measurements.csv"

SHUFFLE_READ = ("internal.metrics.shuffle.read.remoteBytesRead",
                "internal.metrics.shuffle.read.localBytesRead")
SHUFFLE_WRITE = "internal.metrics.shuffle.write.bytesWritten"
INPUT_BYTES = "internal.metrics.input.bytesRead"


def result_lines(run_id: str) -> list[str]:
    logs = boto3.client("logs", region_name=REGION)
    lines = []
    for group in ("/aws-glue/jobs/output", "/aws-glue/jobs/logs-v2"):
        try:
            pages = logs.get_paginator("filter_log_events").paginate(
                logGroupName=group, logStreamNamePrefix=run_id, filterPattern='"RESULT|"'
            )
            for page in pages:
                lines += [e["message"].strip() for e in page["events"]]
        except logs.exceptions.ResourceNotFoundException:
            continue
    return sorted(set(lines))


def main() -> None:
    run_id = sys.argv[1]
    s3 = boto3.client("s3", region_name=REGION)

    lines = result_lines(run_id)
    elapsed = {}
    for line in lines:
        m = re.search(r"RESULT\|(\w+)\|run=(\d+)\|elapsed_s=([\d.]+)", line)
        if m:
            elapsed[(m.group(1), int(m.group(2)))] = float(m.group(3))
    # Glue names the finished event log after the job run id (a truncated
    # *.inprogress snapshot sits next to it until the job ends).
    key = f"{EVENT_LOG_PREFIX}{run_id}"

    stage_group = {}
    stages = defaultdict(lambda: {"stages": 0, "tasks": 0, "shuffle_read": 0,
                                  "shuffle_write": 0, "input": 0})
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8", "replace")
    for raw in body.splitlines():
        ev = json.loads(raw)
        kind = ev.get("Event")
        if kind == "SparkListenerJobStart":
            group = (ev.get("Properties") or {}).get("spark.jobGroup.id")
            if group and group.startswith("e5_") and group != "e5_done":
                for sid in ev["Stage IDs"]:
                    stage_group[sid] = group
        elif kind == "SparkListenerStageCompleted":
            info = ev["Stage Info"]
            group = stage_group.get(info["Stage ID"])
            if not group:
                continue
            acc = {a["Name"]: a.get("Value", 0) for a in info.get("Accumulables", []) if "Name" in a}
            s = stages[group]
            s["stages"] += 1
            s["tasks"] += info["Number of Tasks"]
            s["shuffle_read"] += sum(int(acc.get(k, 0)) for k in SHUFFLE_READ)
            s["shuffle_write"] += int(acc.get(SHUFFLE_WRITE, 0))
            s["input"] += int(acc.get(INPUT_BYTES, 0))
    print(f"Event log: s3://{BUCKET}/{key}")

    ts = datetime.now(timezone.utc).isoformat()
    rows = []
    print(f"{'run':<22}{'elapsed_s':>10}{'stages':>8}{'tasks':>8}{'shuffle_read':>16}{'shuffle_write':>16}{'input_bytes':>16}")
    for group in sorted(stages):
        m = re.match(r"e5_(\w+)_run(\d+)", group)
        variant, run = m.group(1), int(m.group(2))
        s = stages[group]
        e = elapsed.get((variant, run))
        print(f"{group:<22}{e if e is not None else '?':>10}{s['stages']:>8}{s['tasks']:>8}"
              f"{s['shuffle_read']:>16,}{s['shuffle_write']:>16,}{s['input']:>16,}")
        label = f"e5_{variant}"
        if e is not None:
            rows.append([label, run, "elapsed_time", f"{e:.2f}", "seconds", ts])
        rows += [
            [label, run, "stage_count", s["stages"], "stages", ts],
            [label, run, "task_count", s["tasks"], "tasks", ts],
            [label, run, "shuffle_read", s["shuffle_read"], "bytes", ts],
            [label, run, "shuffle_write", s["shuffle_write"], "bytes", ts],
            [label, run, "input_bytes", s["input"], "bytes", ts],
        ]

    with open(RESULTS_CSV, "a", newline="") as f:
        csv.writer(f).writerows(rows)
    print(f"\nAppended {len(rows)} rows to {RESULTS_CSV}")


if __name__ == "__main__":
    main()
