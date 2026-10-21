# E1: Small files

**Compares**: `raw/parquet-smallfiles/` (40,983 files, avg 76.6KB) vs
`raw/parquet-flat/` (21 files, avg 122MB) — same 167,858,646 rows either way.

**Job**: `jobs/e1_small_files.py`, run via Glue job
`data-perf-teardown-e1-small-files` (G.1X, 2 workers). Measures
`df.rdd.getNumPartitions()` right after a plain `spark.read.parquet(path)`
(the actual Spark read-task count, before any shuffle) and wall-clock
read+count time. Three runs per variant, back to back in one job
invocation; first run of each is the cold-cache run.

## Results (2026-08-27)

| Variant | Files | Task count | Run 1 (cold) | Run 2 | Run 3 | Median (runs 2-3) |
|---|---|---|---|---|---|---|
| Small files | 40,983 | 1,323 | 291.20s | 167.45s | 164.50s | **167.45s** |
| Compacted | 21 | 21 | 1.13s | 0.88s | 0.91s | **0.91s** |

**Task count: 63x more** (1,323 vs 21). **Runtime: ~184x slower**
(167.45s vs 0.91s) for the identical row count.

**Storage: ~25% larger** for the same rows and codec — 3,213,516,163
bytes (small files) vs 2,562,217,347 bytes (compacted), measured with
`aws s3 ls --summarize`. Every file repeats its own footer/schema, and
~4,100-row row groups give ZSTD and dictionary encoding far less data to
compress per chunk.

Note Spark's own read coalescing: 40,983 files did not become 40,983
tasks — Spark's Parquet reader packs multiple small files into each
partition up to `spark.sql.files.maxPartitionBytes` (default 128MB), so
task count landed at 1,323, not the raw file count. Even with that
mitigation already applied, the runtime damage is severe — the per-task
overhead of opening, reading footer/metadata from, and closing tens of
thousands of tiny S3 objects dominates.

**Why it worked**: fewer, larger files means fewer S3 GET/HEAD round
trips and fewer Spark task-scheduling units for the same total bytes —
per-task/per-request fixed overhead stops being the dominant cost.

**When it wouldn't help**: if the workload is genuinely low-latency
point lookups on a small working set (not a full-table scan), file count
matters less than index/predicate selectivity. Over-compacting into a
handful of huge files can also hurt write concurrency for streaming
ingestion (many small writers naturally produce many small files;
forcing large files requires a compaction step, which is exactly what
this experiment's "after" state represents, not something free).
