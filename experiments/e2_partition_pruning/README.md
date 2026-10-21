# E2: Partition pruning

**Compares**: `raw/parquet-flat/` (21 files, unpartitioned) vs
`optimized/parquet-partitioned/` (Hive-style `pickup_date=YYYY-MM-DD/`
folders) — same 167,858,646 rows either way, both ZSTD Parquet.

**Engine**: Athena (engine v3), workgroup `data-perf-teardown-wg`.
**Script**: `run.py` in this folder (three runs per query via
`setup/athena_runner.py`, every run logged to `results/raw_measurements.csv`).

## Exact queries

```sql
-- flat_no_pruning: no partition column, so the filter goes through the raw timestamp
SELECT count(*) AS trips, avg(fare_amount) AS avg_fare
FROM parquet_flat
WHERE CAST(tpep_pickup_datetime AS DATE) = DATE '2024-06-15';

-- partitioned_pruned: filter on the partition column
SELECT count(*) AS trips, avg(fare_amount) AS avg_fare
FROM parquet_partitioned
WHERE pickup_date = '2024-06-15';
```

Both return the identical answer: **115,235 trips, avg fare $18.385**.

## Results (2026-08-27)

| Variant | Bytes scanned (every run) | Run 1 (cold) | Run 2 | Run 3 | Median (runs 2-3) |
|---|---|---|---|---|---|
| Flat | 12,885,632 (12.9 MB) | 899 ms | 858 ms | 1,006 ms | **1,006 ms** |
| Partitioned | 140,343 (140 KB) | 772 ms | 640 ms | 682 ms | **682 ms** |

**Bytes scanned: ~91.8x less** with partition pruning. **Engine time:
~1.5x faster**. Rows read by the scan operator (from `EXPLAIN ANALYZE`,
below): 3,563,520 flat vs 115,235 partitioned — **~31x fewer rows touched**.

## Dive deep: why the flat baseline wasn't a full scan

A naive expectation is that the flat table would scan the whole
`tpep_pickup_datetime` + `fare_amount` columns (~1 GB+). It scanned 12.9 MB.
`EXPLAIN ANALYZE` (run 2026-10-21, same queries) shows why:

```
ScanFilterProject[table = ...:parquet_flat,
  filterPredicate = (("tpep_pickup_datetime" >= TIMESTAMP '2024-06-15 00:00:00.000')
                 AND ("tpep_pickup_datetime" <  TIMESTAMP '2024-06-16 00:00:00.000'))]
  Input: 3563520 rows (31.71MB), Filtered: 96.77%, Physical input: 12.29MB

TableScan[table = ...:parquet_partitioned]
  Input: 115235 rows (1012.81kB), Physical input: 137.05kB
```

1. Trino unwrapped the `CAST(... AS DATE) = DATE '...'` into a plain
   timestamp range, so the predicate could still be pushed into the
   Parquet reader — a cast on the column did **not** defeat pushdown here.
2. The flat files were written by DuckDB from monthly TLC files in month
   order, so the data is *naturally* clustered on pickup time. Each row
   group's footer min/max on `tpep_pickup_datetime` is narrow, and Athena
   skipped every row group whose range excluded 2024-06-15. It still had
   to open all 21 file footers and read every row group whose min/max
   range overlapped the day (3.56M rows, ~31x the matching rows), then
   throw away 96.77% of them.
3. Partitioning eliminates that residue entirely: Athena lists one S3
   folder and reads exactly the matching rows, with zero filter waste.

So this measurement is honestly "**partition pruning vs row-group-stats
pruning on time-ordered data**", not "partition pruning vs full scan". On a
flat table that is *not* clustered on the filter column (e.g. data landed
by a process that interleaves dates, or a filter on a column unrelated to
load order), row-group stats can't exclude anything and the gap would be
far larger. E6 tests exactly that clustered-vs-unclustered effect directly.

## Dollar cost — and the Athena 10 MB minimum

At $5/TB: flat ~$0.000064/query, partitioned ~$0.0000007/query on paper.
But **Athena bills a 10 MB minimum per query** (rounded up to the nearest
MB), so the billed amounts are 13 MB vs 10 MB — a real-money ratio of only
**1.3x** at this scale, not 91.8x. The 91.8x is the ratio that matters once
the scanned volume per query is well above 10 MB: e.g. a dashboard
querying a month of data, or the same layout at 100x the row count.

## Why it worked

The partition column turns the filter into a metadata operation: Athena
resolves `pickup_date = '2024-06-15'` against the Glue catalog's partition
list and only plans files under that one prefix. No footer reads, no
row-group stats evaluation, no rows filtered after reading.

## When it wouldn't help

- **Queries that don't filter on the partition column** — a filter on
  `pulocationid` gets no benefit, and 1,481 partitions × small files can
  make it *slower* than the 21-file flat layout (more S3 LISTs/GETs, more
  splits). This is E1's small-files problem, created by over-partitioning.
- **Over-fine partitioning**: daily partitions here average ~113K rows /
  ~1.7 MB each — already below a healthy file size. Partitioning by hour
  would be strictly worse.
- **Already-clustered flat data with selective footers**, as shown above —
  the incremental win over good row-group stats is real but smaller than
  "partitioned vs unpartitioned" folklore suggests.
- **Dirty partition keys**: this dataset's real TLC records include
  pickup timestamps outside the 2022-2025 range (e.g. 2001-01-01,
  2002-10-21), so the table has 1,481 partitions instead of the ~1,461
  days in four years. Each bad timestamp becomes its own near-empty
  partition — partitioning on a raw, unvalidated column bakes data
  quality problems into the physical layout.
