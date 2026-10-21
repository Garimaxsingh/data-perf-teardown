# E4: Sort keys and zone maps (Postgres fallback for Redshift DIST/SORT keys)

**Substitution, stated up front**: the original spec runs this on Redshift
Serverless (`DISTSTYLE EVEN` vs `DISTKEY`/`SORTKEY`, measured with
`SVL_QUERY_REPORT`). Redshift Serverless returns
`SubscriptionRequiredException` on this AWS account (`docs/RUNBOOK.md`
incident 1), so E4 runs on **PostgreSQL 16 on a single EC2 t3.micro**
and demonstrates the part of the story a single node *can* show: the
**sort key + zone map** effect. Redshift's distribution styles are about
moving rows between compute nodes and are not demonstrated here.

| Redshift concept | Postgres stand-in used here |
|---|---|
| Table with no `SORTKEY` (rows in load order) | `trips_unsorted` — loaded in arrival (pickup-day) order |
| `SORTKEY(pulocationid)` | `trips_sorted` — same rows, `CREATE TABLE AS ... ORDER BY pulocationid` |
| Zone maps (per-block min/max, automatic) | BRIN index on `pulocationid` (min/max per range of 128 heap blocks), created on **both** tables |
| Blocks scanned (`SVL_QUERY_REPORT`) | `Shared Read/Hit Blocks` from `EXPLAIN (ANALYZE, BUFFERS)` (8 KB blocks) |

**Data**: all 2024 pickups — **41,169,691 rows, all 20 columns** (2024
partitions of `optimized/parquet-partitioned/`), ~7.7 GB heap per table.
A one-year subset rather than the full 167.8M rows, chosen to fit a
t3.micro's 30 GB disk and 1 GB RAM.

**Box**: EC2 t3.micro (2 vCPU, 913 MB usable RAM), 30 GB gp3, Amazon
Linux 2023, Postgres 16 with `shared_buffers=128MB`, `work_mem=32MB`,
`max_parallel_workers_per_gather=2`. Created by `infra/e4_postgres.tf`
(`-var e4_enabled=true`), driven via SSM Run Command (no SSH, no inbound
ports), destroyed the same session — it existed for ~26 minutes
(2026-10-21 07:15:49 → 07:42:10 UTC). Everything ran from
`e4_run.sh` in this folder; raw EXPLAIN JSON for every run is in
`results/e4/`.

## Exact query (identical for both tables)

Top drop-off zones for JFK Airport pickups (`pulocationid = 132`):

```sql
SELECT z.zone, count(*) AS trips, avg(t.fare_amount) AS avg_fare
FROM <table> t JOIN zones z ON z.locationid = t.dolocationid
WHERE t.pulocationid = 132
GROUP BY z.zone ORDER BY trips DESC LIMIT 10;
```

Both tables return the identical top 10 (`results/e4/answer_*.txt`) —
JFK Airport 102,842 trips, Times Sq/Theatre District 94,129, Outside of
NYC 84,737, … — from **1,989,990 JFK pickups** (4.8% of 2024's trips).

## Results (2026-10-21)

Before run 1 of each table: Postgres restarted (empties `shared_buffers`)
and the OS page cache dropped (`echo 3 > /proc/sys/vm/drop_caches`) — a
true cold start.

| Table | Plan chosen | Heap blocks read (run 2) | Run 1 (cold) | Run 2 | Run 3 | Median (2-3) |
|---|---|---|---|---|---|---|
| `trips_unsorted` | Parallel **Seq Scan** | **936,582** (all 936,678 blocks) | 57.59 s | 57.52 s | 57.65 s | **57.65 s** |
| `trips_sorted` | **BRIN Bitmap Index Scan → Bitmap Heap Scan** | **46,369** | 14.15 s | 1.25 s | 1.25 s | **1.26 s** |

- **Blocks read: ~20.2x fewer** (936,582 → 46,369) — the deterministic
  number, identical across runs.
- **Cold run: ~4.1x faster** (57.6 s → 14.1 s) — both disk-bound.
- **Warm runs: ~46x faster** (57.6 s → 1.26 s) — see the caveat below.

Rows the unsorted scan read and threw away: 39,179,700 (3 parallel
workers × 13,059,900 `Rows Removed by Filter`). The sorted scan removed
3,093 rows on BRIN recheck (3 × 1,031) — the edges of the matching block
ranges.

**BRIN index size**: 270,336 bytes (264 KB) on each 7.7 GB table — a zone
map is tiny. **Cost of the sort**: building `trips_sorted` took 458 s, plus
320 s for both BRIN indexes + `VACUUM ANALYZE`; the sorted heap is the
same size as the unsorted one (7,685,537,792 vs 7,673,266,176 bytes).

## Dive deep

**Why the warm-run speedup is inflated.** On warm runs the sorted query
still reports 46,369 blocks as `Shared Read` (not `Hit`), yet `I/O Read
Time` (summed across the 3 parallel processes) dropped from 40.2 s (cold)
to 0.39 s. `shared_buffers` is only
128 MB (16,384 blocks), so Postgres "reads" these blocks again — but from
the **OS page cache**, not disk: the matching 46K blocks (~362 MB) fit in
the box's RAM, while the unsorted table's 7.7 GB can never be cached on a
1 GB machine. So the 46x warm-run figure partly measures "fits in RAM vs
doesn't". The **20x block reduction** and the **4.1x cold-run** figure are
the honest headline numbers; on a box with enough RAM to cache both
tables, the warm-run gap would shrink toward the block ratio.

**Same BRIN index, useless on unsorted data.** Both tables have an
identical 264 KB BRIN index. On `trips_unsorted` every 128-block range
contains pickups from nearly every zone, so every range's min/max spans
~1–265 and nothing can be excluded — the planner correctly ignored the
index and chose a full sequential scan. This is exactly Redshift's zone
map behaviour without a sort key: the zone maps exist, but they don't
help.

**Row store vs Redshift's column store.** Postgres reads whole rows, so
the 46,369 blocks include all 20 columns of every matching row. Redshift
would additionally read only the 3 referenced columns' blocks — sort key
and columnar storage compound there; here only the sort-key effect is
measured. Sorting also didn't shrink the Postgres heap (no compression in
a row store), whereas sorted Redshift columns typically compress better.

## Why it worked

Physically ordering rows by the filter column puts all JFK pickups into a
contiguous run of ~46K blocks. The BRIN index records each block range's
min/max `pulocationid`, so the planner can identify that run from a
264 KB index and read only those blocks — skipping ~95% of the table.
Same mechanism as Redshift's `SORTKEY` + zone maps.

## When it wouldn't help

- **Filters on other columns**: the table can be sorted one way.
  `trips_sorted` is ordered by `pulocationid`; a pickup-time range filter
  gets no zone-map pruning from it (while `trips_unsorted`, which is in
  arrival order, would — given a BRIN index on pickup time, not built here). Choose the sort key from the dominant filter.
- **Ongoing inserts**: new rows are appended in arrival order, so the sort
  decays over time. Redshift needs `VACUUM SORT`; here the 458 s sort is a
  one-off and would need repeating (or partitioning by the sort column).
- **Non-selective predicates**: if the filter matches most of the table,
  reading 80% of blocks in order is no better than a sequential scan.
- **Small tables**: under a few thousand blocks, a full scan is already
  cheap and the sort/maintenance cost isn't repaid.
- **What this does NOT show — distribution styles**: Redshift's
  `DISTSTYLE KEY` (co-locate fact rows by join key) and `DISTSTYLE ALL`
  (copy a small dimension like the 265-row zone table to every node,
  the spec's "correction") avoid redistributing rows across nodes during
  a join. That needs multiple compute nodes; a single Postgres box has
  no data movement to eliminate. E5's broadcast join is the closest
  measured analog in this repo: broadcasting the 265-row dimension is the
  Spark equivalent of `DISTSTYLE ALL`, and it cut shuffle from 1.27 GB to
  9.7 KB.
