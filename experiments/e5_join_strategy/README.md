# E5: Spark join strategy

**Compares**: joining all 167,858,646 trips (`raw/parquet-flat/`) to the
265-row `taxi_zone_lookup` dimension, three ways: forced shuffle join
(two variants) vs broadcast join.

**Engine**: AWS Glue 4.0 (Spark 3.3), G.1X × 2 workers for every run
(Glue set `spark.sql.shuffle.partitions=4`; AQE enabled — Glue defaults,
left as-is). **Job**: `jobs/e5_join_strategy.py`. **Metrics**:
`collect.py` in this folder parses the Spark event log (the same data the
Spark UI's DAG/stage pages are built from) and attributes every completed
stage to its run via Spark job groups.

## Exact query

```python
trips = spark.read.parquet("s3://<bucket>/raw/parquet-flat/")
zones = (spark.read.option("header", True).csv("s3://<bucket>/dim/taxi_zone_lookup/")
         .select(F.col("LocationID").cast("bigint").alias("location_id"), "Borough"))

# All variants: spark.sql.autoBroadcastJoinThreshold = -1
#               spark.sql.adaptive.autoBroadcastJoinThreshold = -1
dim = zones                        # sort_merge         (no hint)
dim = zones.hint("merge")          # sort_merge_hinted  (MERGE hint)
dim = F.broadcast(zones)           # broadcast          (BROADCAST hint)

(trips.join(dim, trips["pulocationid"] == dim["location_id"])
      .groupBy("Borough")
      .agg(F.count(F.lit(1)).alias("trips"), F.sum("fare_amount").alias("total_fare"))
      .collect())
```

Every run joins all 167,858,646 rows into the same 8 borough groups.

## Results

| Variant | Final join operator (post-AQE) | Shuffle write | Shuffle read | Stages run | Tasks | Run 1 | Run 2 | Run 3 | Median (2-3) |
|---|---|---|---|---|---|---|---|---|---|
| `sort_merge` (unhinted) | **ShuffledHashJoin** | 1,265,908,518 B (1.27 GB) | 1.27 GB | 4 | 27 | 65.25 s | 50.45 s | 49.56 s | **50.45 s** |
| `sort_merge_hinted` | **SortMergeJoin** | 1,265,908,527 B (1.27 GB) | 1.27 GB | 4 | 27 | 91.82 s | 82.00 s | 71.55 s | **82.00 s** |
| `broadcast` | **BroadcastHashJoin** | **9,744 B** | 9,744 B | 2 | 22 | 29.56 s | 27.86 s | 26.13 s | **27.86 s** |

- **Shuffle: 1.27 GB → 9.7 KB** (~130,000x less data moved across the network).
- **Runtime: ~2.9x faster** than the true sort-merge join (82.0 s → 27.9 s),
  **~1.8x faster** than the AQE-chosen shuffled hash join (50.5 s → 27.9 s).
- **Stages: 4 → 2.** The event log also shows 9 vs 3 stages *planned*; the
  extra planned stages were skipped by AQE and never ran.
- The remaining 9,744 B of shuffle in the broadcast run is the final
  `groupBy("Borough")` (8 groups × partial aggregates) — present in every
  variant, not part of the join.

Dates: `sort_merge` and `broadcast` ran back to back in one job run
(2026-10-21, `jr_b03d75e9…`); `sort_merge_hinted` ran in a second job
run ~1 hour later (`jr_4d4fbc98…`) with identical configuration — added
after the first run's plan was inspected (see below). Strictly, that one
comparison is not back to back; it is reported as such.

## Dive deep: AQE quietly changed the "forced sort-merge" join

The first job run disabled broadcast at both the static planner and AQE
and expected a sort-merge join. The **initial** plan was indeed
`SortMergeJoin`, but the **final** plan Spark printed after execution was:

```
*(3) ShuffledHashJoin [pulocationid#7L], [location_id#65L], Inner, BuildRight
:- ShuffleQueryStage 0
:  +- Exchange hashpartitioning(pulocationid#7L, 4)
+- ShuffleQueryStage 1
   +- Exchange hashpartitioning(location_id#65L, 4)
```

After the shuffle stages finished, AQE re-planned the join from the
real shuffle statistics and replaced sort-merge with a shuffled hash join
(building a hash table from the tiny dimension partitions) — which skips
the sort but still shuffles all 167.8M fact rows. The exact AQE setting
that triggered the switch in Glue's Spark build wasn't isolated here; the
before/after plans above are what was observed.
Re-running with an explicit `MERGE` hint pinned the textbook plan:

```
*(5) SortMergeJoin [pulocationid#7L], [location_id#65L], Inner
:- *(3) Sort [pulocationid#7L ASC NULLS FIRST]
:     +- Exchange hashpartitioning(pulocationid#7L, 4)
+- *(4) Sort [location_id#65L ASC NULLS FIRST]
      +- Exchange hashpartitioning(location_id#65L, 4)
```

Same 1.27 GB of shuffle either way, so the 50 s vs 82 s difference is
mostly the cost of sorting 167.8M rows across 4 shuffle partitions (with
the caveat that these two ran in separate job invocations); the 82 s vs 28 s
difference is the cost of the shuffle itself plus that sort. The broadcast
plan has no `Exchange` on the fact side at all:

```
*(2) BroadcastHashJoin [pulocationid#7L], [location_id#65L], Inner, BuildRight
:- *(2) Filter isnotnull(pulocationid#7L)
:  +- FileScan parquet [PULocationID#7L, fare_amount#10]
+- BroadcastQueryStage 0
   +- BroadcastExchange HashedRelationBroadcastMode(...)
```

## Why it worked

A shuffle join must hash-partition **both** sides by join key so matching
keys land on the same executor: every one of the 167.8M (pulocationid,
fare_amount) pairs is serialized, written to local disk, and fetched over
the network by another task (1.27 GB here), and a sort-merge join then
sorts each partition too. A broadcast join ships the 265-row dimension to
every executor once as an in-memory hash table, and each task probes it
while scanning its own slice of the fact table — the big side never moves.

## When it wouldn't help

- **Spark already does this by default**: with the default 10 MB
  `autoBroadcastJoinThreshold`, Spark would broadcast a 265-row table
  without being told. The real-world version of this mistake is a
  threshold set to `-1` cluster-wide after an earlier OOM incident, a
  dimension whose size statistics are unknown or over-estimated (e.g. a
  filtered or computed DataFrame), or a "dimension" that has quietly
  grown.
- **Large dimensions**: broadcasting copies the whole table into every
  executor's memory and through the driver. A multi-GB dimension risks
  driver/executor OOM and a long broadcast phase — the shuffle join
  is the right choice there.
- **Both sides large**: no broadcast option exists; bucketing both tables
  on the join key, or pre-partitioning, avoids repeated shuffles instead.
- **Outer joins on the wrong side**: Spark can't broadcast the preserved
  side of an outer join (e.g. the left side of a LEFT JOIN), so the hint
  is ignored.
