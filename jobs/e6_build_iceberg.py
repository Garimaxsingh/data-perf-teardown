"""
E6 build: writes the same 167,858,646 trip rows (from raw/parquet-flat/) into
three new layouts, all with the SAME file count so the only thing that
differs is how rows are clustered and where the min/max stats live:

  - iceberg_unsorted: Iceberg table, rows round-robin across N files
    (every file spans the full pulocationid range 1-265 — Iceberg's
    per-file min/max stats exist but can't exclude anything).
  - iceberg_sorted:   Iceberg table, rows range-partitioned + sorted by
    pulocationid across N files (each file covers a narrow pulocationid
    range — the manifest's per-file min/max can exclude most files at
    planning time, before a single data file is opened).
  - parquet_sorted:   plain Parquet with the identical sorted layout, no
    Iceberg. Control variant: separates "the sort did it" from "Iceberg's
    metadata did it", since Athena can also skip Parquet row groups using
    footer min/max stats — but only after opening every file's footer.

The fourth E6 variant is the existing parquet_flat table (as-landed,
roughly time-ordered — not clustered on pulocationid at all).

After writing, uses Iceberg's own scan planner (Table.newScan().filter())
to report how many data files Iceberg would hand to an engine for the E6
predicate (pulocationid = 1) vs the table's total file count. Athena
bytes scanned for all four variants is measured separately by
experiments/e6_iceberg_stats/run.py.

Per PROJECT_BRIEF.md lesson #6: rows are sorted within partitions before
the Iceberg write (the table itself is unpartitioned here, so the
clustered-writer assumption doesn't strictly apply, but the sort is the
point of the sorted variant anyway).
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "database"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["bucket"]
db = args["database"]
N_FILES = 64  # ~40MB/file at this dataset's ~2.56GB compressed size
SORT_COL = "pulocationid"
PRED_VALUE = 1  # EWR (Newark Airport) — rare as a yellow-taxi pickup zone

src = spark.read.parquet(f"s3://{bucket}/raw/parquet-flat/")
# Lowercase column names: Glue/Athena lowercase everything, and mixed-case
# Iceberg column names are a known source of Athena resolution trouble.
src = src.toDF(*[c.lower() for c in src.columns])

unsorted_df = src.repartition(N_FILES)
sorted_df = src.repartitionByRange(N_FILES, SORT_COL).sortWithinPartitions(SORT_COL)

file_size_props = {"write.target-file-size-bytes": str(512 * 1024 * 1024)}


def write_iceberg(df, name):
    w = df.writeTo(f"glue_catalog.{db}.{name}").using("iceberg")
    for k, v in file_size_props.items():
        w = w.tableProperty(k, v)
    w.createOrReplace()


write_iceberg(unsorted_df, "iceberg_unsorted")
print("WROTE|iceberg_unsorted")
write_iceberg(sorted_df, "iceberg_sorted")
print("WROTE|iceberg_sorted")
sorted_df.write.mode("overwrite").parquet(f"s3://{bucket}/optimized/parquet-sorted/")
print("WROTE|parquet_sorted")

jvm = spark._jvm
Expressions = jvm.org.apache.iceberg.expressions.Expressions


def plan(table, expr=None):
    scan = table.newScan()
    if expr is not None:
        scan = scan.filter(expr)
    it = scan.planFiles().iterator()
    n, size, records = 0, 0, 0
    while it.hasNext():
        f = it.next().file()
        n += 1
        size += f.fileSizeInBytes()
        records += f.recordCount()
    return n, size, records


for name in ("iceberg_unsorted", "iceberg_sorted"):
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"glue_catalog.{db}.{name}"
    )
    total_n, total_bytes, total_rows = plan(table)
    hit_n, hit_bytes, hit_rows = plan(table, Expressions.equal(SORT_COL, PRED_VALUE))
    print(f"RESULT|{name}|total_files={total_n}|total_bytes={total_bytes}|total_rows={total_rows}"
          f"|planned_files={hit_n}|planned_bytes={hit_bytes}|planned_rows={hit_rows}")

job.commit()
