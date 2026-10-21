# E6: Iceberg stats pruning

**Question**: on a highly selective filter, how much does Iceberg's
per-file column statistics actually save over plain Parquet — and how much
of that is Iceberg, versus how the data is physically ordered?

**Filter**: `pulocationid = 1` (Newark Airport pickups) — 27,229 of
167,858,646 trips (0.016%).

**Four layouts of the same 167,858,646 rows**:

| Table | Layout | Files | Codec | Storage |
|---|---|---|---|---|
| `parquet_flat` | plain Parquet, as landed (time-ordered, not clustered on `pulocationid`) | 21 | ZSTD (DuckDB) | 2.56 GB |
| `iceberg_unsorted` | Iceberg, rows round-robin across files | 64 | GZIP (Iceberg 1.0 default) | 3.55 GB |
| `iceberg_sorted` | Iceberg, range-partitioned + sorted on `pulocationid` | 64 | GZIP | 2.75 GB |
| `parquet_sorted` | plain Parquet, identical sorted layout, no Iceberg (control) | 64 | Snappy (Spark default) | 3.91 GB |

The 64-file layouts are built by `jobs/e6_build_iceberg.py` (Glue 4.0,
G.1X × 4, 588 s). Same file count across the three new layouts so file
count isn't a variable; codecs differ because each writer's default was
kept (noted below where it matters).

**Engine**: Athena (engine v3) for bytes scanned/runtime
(`run.py`, three runs each); Iceberg's own scan planner
(`Table.newScan().filter(...).planFiles()`, inside the Glue job) for
planning-time file counts.

## Exact query (identical for all four tables)

```sql
SELECT count(*) AS trips, avg(fare_amount) AS avg_fare
FROM <table>
WHERE pulocationid = 1;
```

All four return **27,229 trips, avg fare $84.88**.

## Results (2026-10-21)

| Table | Files planned | Rows read by scan¹ | Bytes scanned (every run) | Run 1 | Run 2 | Run 3 | Median (2-3) |
|---|---|---|---|---|---|---|---|
| `parquet_flat` | 21 (all) | 158,055,213 | 282.80 MB | 6,948 ms | 2,463 ms | 5,047 ms | 5,047 ms |
| `iceberg_unsorted` | **64 / 64** | 167,858,646 | 375.17 MB | 1,301 ms | 1,074 ms | 999 ms | 1,074 ms |
| `iceberg_sorted` | **1 / 64** | 2,669,736 | **4.11 MB** | 1,248 ms | 728 ms | 1,442 ms | 1,442 ms |
| `parquet_sorted` | 64 (footers opened) | 40,000 | **0.13 MB** | 893 ms | 676 ms | 526 ms | 676 ms |

¹ From `EXPLAIN ANALYZE` on the same query (run 2026-10-21, one extra run
per table, not counted in the medians above).

- **Iceberg sorted vs Iceberg unsorted**: 63 of 64 files pruned at
  planning time, **~91x fewer bytes** (375 MB → 4.1 MB).
- **Iceberg sorted vs the as-landed flat table**: **~69x fewer bytes**
  (283 MB → 4.1 MB).
- **Plain Parquet sorted beat Iceberg sorted on bytes** (0.13 MB vs
  4.11 MB) — explained below.

## Dive deep: what actually did the work

**1. Iceberg's stats without clustering do nothing.** `iceberg_unsorted`
has exactly the same per-file min/max statistics in its manifests as
`iceberg_sorted` — but every file's `pulocationid` range is 1–265, so
the planner kept 64/64 files and Athena read every row. It scanned
*more* than the flat table (375 vs 283 MB), because round-robin
shuffling destroys run-length compression and GZIP is a different codec
from the flat table's ZSTD. Storage tells the same story: the same GZIP
data takes 3.55 GB unsorted vs 2.75 GB sorted.

**2. Sorting made the stats selective.** Range-sorted on `pulocationid`,
the first file covers only `pulocationid` 1–33, so Iceberg's planner
(reading only manifest files, never touching a data file) handed Athena
exactly one file. Athena then read that file's single row group in full:
2,669,736 rows, and bytes scanned of 4,105,631 — which is the file's
`fare_amount` chunk (4,097,683 B) + `pulocationid` chunk (7,848 B),
confirmed by reading the file's Parquet footer with pyarrow.

**3. Plain Parquet with the same sort went further, via page indexes.**
`parquet_sorted`'s files (written by Spark's parquet-mr 1.12.2) carry
Parquet *column indexes* — per-page min/max inside each row group. Athena
read only 40,000 rows (a handful of pages) from the matching file,
125 KB total. The Iceberg file also has column indexes in its footer
(checked with pyarrow: `has_column_index=True`), but Athena's Iceberg
read path read the whole row group instead. The measured behaviour is
"Athena's Hive-table reader used page indexes here and its Iceberg
reader didn't" — this is inferred from the row counts above, not from
Athena documentation.

**4. What Iceberg uniquely bought.** Pruning happened at *planning* time:
1 data file opened instead of 64 footers. That doesn't show up in Athena's
bytes-scanned figure at all (footers aren't billed as data scanned), and
at 64 files it's a small effect. It matters when tables have tens of
thousands of files: a plain Parquet table must LIST and open every
footer to decide what to skip, while Iceberg decides from a few manifest
files.

## Dollar cost — and the Athena 10 MB minimum

| Table | Billed per query (10 MB min, rounded up to MB) | @ $5/TB |
|---|---|---|
| `parquet_flat` | 283 MB | $0.00141 |
| `iceberg_unsorted` | 376 MB | $0.00188 |
| `iceberg_sorted` | 10 MB (minimum) | $0.00005 |
| `parquet_sorted` | 10 MB (minimum) | $0.00005 |

On Athena's bill the two sorted layouts are identical — the 0.13 MB vs
4.11 MB difference is real I/O, but both are under the 10 MB floor. The
billing-relevant fix is the sort, at ~28x cheaper than the as-landed
flat table for this query shape.

## Why it worked

Min/max statistics (Iceberg manifests, Parquet footers, Parquet page
indexes) can only exclude data whose value range is *narrow*. Sorting
on the filter column makes each file's — and each page's — range narrow,
so a point filter matches one file (and a few pages) instead of all of
them. Iceberg moves the file-level decision into metadata, so the engine
doesn't have to open each data file to decide.

## When it wouldn't help

- **Unclustered data**: shown directly above — Iceberg with no sort order
  pruned 0 of 64 files. "Move to Iceberg" alone is not a performance fix.
- **Filters on a different column**: data can only be sorted one way
  (or Z-ordered across a few columns, with weaker pruning per column).
  A `vendorid` filter on `iceberg_sorted` should expect little or no file
  pruning (not measured here — each file spans every vendor).
- **Non-selective filters**: a filter matching 30% of rows touches most
  files no matter how it's sorted.
- **Below Athena's 10 MB minimum**: once a query scans under 10 MB,
  further byte reductions don't reduce the bill (see the table above).
- **Writes**: keeping data sorted costs a range shuffle at write time
  (part of this build job's 588 s — not timed separately) or periodic `rewrite_data_files` sort
  compaction in production — real compute spent to make reads cheaper.
