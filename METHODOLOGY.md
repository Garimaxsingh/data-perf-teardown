# Methodology

How every number in this repo was produced, and the limits of what it
can claim.

## Data

- **NYC TLC yellow taxi trips, 2022-01 through 2025-12** — 48 monthly
  Parquet files, 167,858,646 rows, 2.6 GB as downloaded
  (`setup/download_tlc_data.py`).
- **Taxi Zone Lookup** — 265 rows, the join dimension for E4 and E5.
- Schema reconciliation (`setup/build_variants.py`, DuckDB `union_by_name`):
  `passenger_count` is DOUBLE through 2023-01 and BIGINT from 2023-02 —
  unified to DOUBLE. `cbd_congestion_fee` is physically absent before
  2025-01 (checked per file with `pyarrow.parquet.read_schema()` in
  `setup/inspect_schemas.py`, not assumed from the TLC changelog) — unified
  as a nullable column.
- Dirty records are **kept**, not cleaned: negative fares, zero-distance
  high-fare trips, out-of-range pickup timestamps (2001–2026 in a 2022–2025
  dataset). The benchmark measures physical layouts of real data; cleaning
  would change the data under test. Where dirt affects a result it is
  called out (e.g. E2's 1,481 partitions instead of ~1,461).

## Physical variants

All derived once from the same source and never regenerated:

| Variant | S3 prefix | Files | Size |
|---|---|---|---|
| A. CSV, flat | `raw/csv-flat/` | 70 | 17.82 GB |
| B. Parquet (ZSTD), flat, ~128 MB files | `raw/parquet-flat/` | 21 | 2.56 GB |
| C. Parquet, partitioned by `pickup_date` | `optimized/parquet-partitioned/` | 1,562 | 2.62 GB |
| D. Parquet, small files | `raw/parquet-smallfiles/` | 40,983 | 3.21 GB |

Experiment-specific layouts (E4's Postgres tables, E6's Iceberg and
sorted-Parquet tables) are described in each experiment's README.
Row counts were cross-checked across all variants (167,858,646 each).

## Engines and fixed compute

| Exp | Engine | Compute held constant across variants |
|---|---|---|
| E1 | AWS Glue 4.0 (Spark 3.3) | G.1X × 2 workers |
| E2, E3, E6 | Amazon Athena (engine v3) | Serverless; one workgroup, 10 GB/query scan cap (raised to 20 GB for E3's CSV scan only) |
| E4 | PostgreSQL 16 on one EC2 t3.micro (2 vCPU, 1 GB RAM) | Same instance, same config, both tables |
| E5 | AWS Glue 4.0 (Spark 3.3) | G.1X × 2 workers, AQE on (Glue default) |

E4 uses Postgres instead of Redshift because Redshift Serverless returns
`SubscriptionRequiredException` on this account (`docs/RUNBOOK.md`
incident 1).

## Rigor rules

1. **Three runs per variant.** Every individual run is logged to
   `results/raw_measurements.csv` — the CSV is the source of truth, the
   READMEs are summaries of it.
2. **Run 1 is the cold run and is excluded from the reported median.**
   With two remaining runs, the reported "median" is the **higher** of
   runs 2 and 3 (the shared runner takes the upper middle element). This
   is the conservative choice for the optimized variants; both values are
   always shown in the experiment tables.
3. **Back to back.** Variants of one experiment run in the same session —
   same Glue job invocation for E1/E5, consecutive Athena queries for
   E2/E3/E6, the same EC2 session for E4. One exception, disclosed in
   E5's README: the `sort_merge_hinted` variant ran ~1 hour after the
   other two, after the first run's plan showed AQE had changed the join.
4. **Exact query committed next to every number** — each experiment
   folder's `run.py` / job script / README holds the literal SQL or
   PySpark, and every variant of an experiment is checked to return the
   same answer.
5. **Measured, not estimated.** Bytes scanned come from Athena's
   `DataScannedInBytes`; Spark shuffle bytes and stage counts from the
   Spark event log (`experiments/e5_join_strategy/collect.py`); Postgres
   blocks from `EXPLAIN (ANALYZE, BUFFERS)`; Iceberg file counts from
   Iceberg's own scan planner. Where a mechanism is inferred rather than
   measured, the README says so.
6. **Surprises are reported, not tuned away** — e.g. Parquet slower than
   CSV on engine time (E3), AQE silently changing the join (E5), plain
   sorted Parquet beating Iceberg on bytes (E6).

## Cost figures

- Athena: $5 per TB scanned. **Athena bills a 10 MB minimum per query**
  (rounded up to the MB), which caps how much a tiny scan can save in
  real money — E2 and E6 show both the raw ratio and the billed ratio.
- Glue: $0.44 per DPU-hour, from each job run's reported DPU-seconds.
- EC2 (E4): t3.micro on-demand plus "unlimited" CPU surplus at
  $0.05/vCPU-hour, plus 30 GB gp3 for the hours it existed.
- Every line item, including failed and repeated attempts, is in
  `docs/COST.md`.

## What this benchmark does not claim

- **Single dataset, single scale.** Ratios are for ~168M rows of one
  table; absolute timings will differ on other data or cluster sizes.
- **Athena latency is noisy.** It's a shared multi-tenant service; engine
  time varies run to run for identical scans (E3 Parquet: 4.0 s vs 6.2 s).
  Bytes scanned is deterministic and is the primary Athena metric here.
- **E4 is not Redshift.** It shows the sort-key / zone-map principle on a
  single Postgres node; Redshift's distribution styles (EVEN / KEY / ALL)
  are about data movement between compute nodes and cannot be shown on
  one machine.
