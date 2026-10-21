# E3: Format + column pruning

**Compares**: `raw/csv-flat/` (70 uncompressed CSV files, ~256 MB each)
vs `raw/parquet-flat/` (21 ZSTD Parquet files, ~122 MB each) — same
167,858,646 rows, same 20 columns, query touches 3 of them.

**Engine**: Athena (engine v3), workgroup `data-perf-teardown-wg`.
**Script**: `run.py` in this folder (three runs per query, all logged to
`results/raw_measurements.csv`).

## Exact queries

```sql
-- csv_3col
SELECT vendorid, avg(fare_amount) AS avg_fare, avg(trip_distance) AS avg_dist
FROM csv_flat
GROUP BY vendorid;

-- parquet_3col
SELECT vendorid, avg(fare_amount) AS avg_fare, avg(trip_distance) AS avg_dist
FROM parquet_flat
GROUP BY vendorid;
```

## Results (2026-08-27)

| Variant | Storage (S3) | Bytes scanned (every run) | Run 1 (cold) | Run 2 | Run 3 | Median (runs 2-3) | Cost/query @ $5/TB |
|---|---|---|---|---|---|---|---|
| CSV | 17.82 GB | 17,821,064,005 (17.82 GB) | 2,211 ms | 2,783 ms | 2,379 ms | **2,783 ms** | **$0.0891** |
| Parquet | 2.56 GB | 443,619,935 (0.44 GB) | 3,492 ms | 3,960 ms | 6,201 ms | **6,201 ms** | **$0.0022** |

- **Bytes scanned: ~40.2x less** with Parquet → **~40x cheaper per query**.
- **Storage: ~6.9x smaller** (17.82 GB vs 2.56 GB).
- **Engine time: Parquet was *slower*** — reported as measured, see below.

Cost extrapolation: a job that runs this query hourly costs
$0.0891 × 24 = **$2.14/day on CSV vs $0.053/day on Parquet**, from the
format choice alone.

## Why it worked

- **Column pruning**: CSV is row-oriented — to find the 3 needed fields
  Athena must read and parse every byte of every line, so bytes scanned
  equals the full table size (17.82 GB) no matter which columns are
  selected. Parquet stores each column contiguously per row group, so
  Athena reads only the `vendorid`, `fare_amount` and `trip_distance`
  column chunks (0.44 GB, ~17% of the Parquet table).
- **Encoding + compression**: the CSV variant is uncompressed text (every
  number spelled out as characters), while Parquet stores typed binary
  values with dictionary/RLE encoding plus ZSTD. Together these account
  for the 6.9x storage gap (not broken down further per column here).
- **Metadata**: Parquet footers carry row counts and min/max stats; CSV
  carries nothing. This showed up before E3 even started: a bare
  `count(*)` on `csv_flat` hit the workgroup's 10 GB scan cap, while the
  same `count(*)` on every Parquet variant scanned **0 bytes** (answered
  from footers) — see `docs/RUNBOOK.md` incident 3.

## Dive deep: why Parquet was slower on engine time

The cost win is unambiguous; the latency result went the other way, and
it is reported rather than hidden:

- **Parallelism**: 70 splittable, uncompressed CSV files give Athena many
  more independent splits to fan out across workers than 21 Parquet
  files do. Plain-text CSV parsing of a few numeric fields is cheap
  per byte, so with enough parallel readers 17.82 GB of scanning finishes
  in ~2.4–2.8 s.
- **Decode cost**: Parquet's bytes are fewer but denser — every value
  passes through ZSTD decompression plus dictionary/RLE decoding.
- **Variance**: Parquet run 3 (6.2 s) was ~1.6x run 2 (4.0 s) for an
  identical scan, so with only two kept runs the "median" is noisy. Athena
  is a shared, multi-tenant service and its engine time includes queueing
  for worker capacity outside the user's control.

The point for a cost-focused reader: Athena bills on **bytes scanned, not
seconds**, so on Athena the format decision is a 40x cost decision and
roughly a wash on latency. On an engine that bills by compute time
(Glue, EMR, a provisioned warehouse), the latency comparison would
deserve its own controlled benchmark — this experiment doesn't claim one.

## When it wouldn't help

- **`SELECT *` workloads**: column pruning only pays when queries touch a
  subset of columns. Reading all 20 columns from Parquet still wins on
  compression (~6.9x fewer bytes), but loses the extra column-pruning factor.
- **Small, frequently appended data**: Parquet files are immutable and
  only efficient at reasonable sizes; appending a few rows at a time
  creates exactly E1's small-files problem. CSV/JSON is a fine *landing*
  format for streaming ingest, as long as something compacts it into
  Parquet before it's queried at scale.
- **Interchange with tools that need human-readable files**: CSV remains
  the lowest-common-denominator export format — the fix is to not *query*
  it repeatedly, not to never produce it.
