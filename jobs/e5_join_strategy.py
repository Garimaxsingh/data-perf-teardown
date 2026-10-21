"""
E5: Spark join strategy — 167,858,646 trip rows (raw/parquet-flat/) joined
to the 265-row taxi_zone_lookup dimension, aggregated by pickup Borough.

Variants, same query, same Glue worker config:
  - sort_merge: broadcast disabled at BOTH the static planner and AQE
    (spark.sql.autoBroadcastJoinThreshold=-1 and
    spark.sql.adaptive.autoBroadcastJoinThreshold=-1), so Spark must
    hash-partition and shuffle the 167.8M-row side to co-locate join keys.
  - sort_merge_hinted: same -1 thresholds plus an explicit MERGE join
    hint. Added after the first run showed AQE rewriting the unhinted
    sort_merge variant's SortMergeJoin into a ShuffledHashJoin at runtime
    (still a full shuffle of the big side, but not literally sort-merge);
    the hint pins the textbook strategy. Run via --variants sort_merge_hinted.
  - broadcast: explicit F.broadcast() hint on the dimension (a hint wins
    over the -1 thresholds), so the 265 rows are shipped to every executor
    and the fact side is joined in place, no shuffle of the big side.

Each run is tagged with its own Spark job group, so shuffle read/write
bytes and stage counts can be attributed per run afterwards from the Spark
event log this job writes to S3 (--enable-spark-ui / --spark-event-logs-path,
the same data the Spark UI's DAG view is rendered from) — parsed by
experiments/e5_join_strategy/parse_event_log.py. This script itself prints
wall-clock time, the stage count Spark's status tracker saw, and the final
executed physical plan (post-AQE) for each run.

Three runs per variant, back to back in one job invocation (same pattern
as jobs/e1_small_files.py); first run of each is the cold one.
"""
import sys
import time

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["bucket"]
N_RUNS = 3

# Applies to both variants; the broadcast variant overrides it per-join via hint.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
print(f"CONFIG|adaptive.enabled={spark.conf.get('spark.sql.adaptive.enabled')}"
      f"|shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")

trips = spark.read.parquet(f"s3://{bucket}/raw/parquet-flat/")
zones = (
    spark.read.option("header", True)
    .csv(f"s3://{bucket}/dim/taxi_zone_lookup/")
    .select(F.col("LocationID").cast("bigint").alias("location_id"), "Borough")
)


def build_query(variant: str):
    dim = {"broadcast": F.broadcast(zones), "sort_merge_hinted": zones.hint("merge")}.get(variant, zones)
    return (
        trips.join(dim, trips["pulocationid"] == dim["location_id"])
        .groupBy("Borough")
        .agg(F.count(F.lit(1)).alias("trips"), F.sum("fare_amount").alias("total_fare"))
    )


tracker = sc.statusTracker()

# Optional --variants a,b to re-run a subset (e.g. just sort_merge_hinted).
variants = ["sort_merge", "broadcast"]
if "--variants" in sys.argv:
    variants = getResolvedOptions(sys.argv, ["variants"])["variants"].split(",")

for name in variants:
    for run in range(1, N_RUNS + 1):
        group = f"e5_{name}_run{run}"
        sc.setJobGroup(group, f"E5 {name} run {run}")
        q = build_query(name)
        t0 = time.time()
        rows = q.collect()
        elapsed = time.time() - t0

        stage_ids = set()
        for job_id in tracker.getJobIdsForGroup(group):
            info = tracker.getJobInfo(job_id)
            if info:
                stage_ids.update(info.stageIds)
        total_trips = sum(r["trips"] for r in rows)
        print(f"RESULT|{name}|run={run}|elapsed_s={elapsed:.2f}|jobs={len(tracker.getJobIdsForGroup(group))}"
              f"|stages_planned={len(stage_ids)}|joined_rows={total_trips}|boroughs={len(rows)}")
        if run == 1:
            plan = q._jdf.queryExecution().executedPlan().toString()
            print(f"PLAN|{name}|BEGIN")
            print(plan)
            print(f"PLAN|{name}|END")

sc.setJobGroup("e5_done", "done")
job.commit()
