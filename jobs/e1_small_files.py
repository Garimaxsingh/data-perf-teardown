"""
E1: Small files — ~40,983 x ~77KB Parquet files (raw/parquet-smallfiles/)
vs 21 x ~122MB compacted Parquet files (raw/parquet-flat/). Measures the
one number Athena can't natively give us: Spark's actual read task count
(df.rdd.getNumPartitions() immediately after a plain read, before any
shuffle, equals the number of input splits Spark will schedule one task
per). Also captures wall-clock read+count time per run.

Three runs per variant, back to back within this one job invocation (same
compute size, same session — avoids Glue's ~1min cold-start cost 6x over
for what the brief's rigor rules just call "run comparisons back to
back"). First run of each variant is the cold-cache one; still printed,
excluded from the reported median (done downstream when parsing logs, not
in this script).
"""
import sys
import time

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["bucket"]
TARGETS = {
    "smallfiles": f"s3://{bucket}/raw/parquet-smallfiles/",
    "compacted": f"s3://{bucket}/raw/parquet-flat/",
}
N_RUNS = 3

for name, path in TARGETS.items():
    for run in range(1, N_RUNS + 1):
        t0 = time.time()
        df = spark.read.parquet(path)
        n_partitions = df.rdd.getNumPartitions()
        cnt = df.count()
        elapsed = time.time() - t0
        print(f"RESULT|{name}|run={run}|partitions={n_partitions}|rows={cnt}|elapsed_s={elapsed:.2f}")

job.commit()
