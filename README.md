# Redshift & Spark Performance/Cost Teardown

A controlled benchmark of six common data-engineering performance mistakes on 150-200M
rows of real NYC TLC taxi data, each with before/after runtime, bytes scanned, and
dollar cost.

**Status**: scaffolding only (2026-08-23). No AWS account created yet, no data
downloaded, no experiments run. Shares one AWS account with `../retail-lakehouse-aws` —
see `docs/AWS_SETUP.md` for the account setup (do it once, covers both repos). In the
combined execution plan this project is Weeks 6-7, after the lakehouse project.

**Engine split**: Athena is used for E1, E2, E3, E5, E6 (cheap, real, non-simulated).
Redshift Serverless is used only for E4, in a single bounded 4-6 hour session — create
workgroup, load, run, capture `SVL_QUERY_REPORT` output, drop the workgroup, same day.

## Repo layout

```
data-perf-teardown/
├── docs/
│   └── AWS_SETUP.md      ← shared with retail-lakehouse-aws, do this first
├── setup/                ← download_tlc.py, build_variants.py, load_redshift.sql
├── experiments/
│   ├── e1_small_files/
│   ├── e2_partition_pruning/
│   ├── e3_format/
│   ├── e4_redshift_keys/
│   ├── e5_join_strategy/
│   └── e6_iceberg_stats/
├── results/
│   └── raw_measurements.csv   ← every individual run, not just medians
└── infra/                ← minimal Terraform
```

## The six experiments

| # | Experiment | Compares |
|---|---|---|
| E1 | Small files | ~40,000 × 200KB files vs compacted 128MB targets |
| E2 | Partition pruning | Flat Parquet vs partitioned by `pickup_date` |
| E3 | Format + column pruning | CSV vs Parquet, 3-of-19 columns touched |
| E4 | Redshift DIST/SORT keys | `DISTSTYLE EVEN` vs `DISTKEY`/`SORTKEY` (and the honest correction to `DISTSTYLE ALL` on the 265-row dimension) |
| E5 | Spark join strategy | Forced sort-merge vs broadcast join, 200M rows × 265-row dimension |
| E6 | Iceberg stats pruning | Plain Parquet vs Iceberg column-level stats on a highly selective filter |

Methodology (three runs, median reported, cold cache discarded, back-to-back
comparisons) lives in `METHODOLOGY.md` once written. Results table goes in `README.md`
above this section once real numbers exist — per the project's own rule, no unexplained
benchmark numbers.

## Data

- **NYC TLC Trip Record Data**, yellow taxi, 2022-01 through 2025-12 — [official page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- **Taxi Zone Lookup** (265 rows) — the broadcast-join dimension.

Both are publicly downloadable; nothing here is irreplaceable if the AWS account is torn down.
