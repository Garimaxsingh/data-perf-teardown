# data-perf-teardown

| # | Mistake → fix | Engine | Before | After | Improvement | Cost per run, before → after |
|---|---|---|---|---|---|---|
| [E1](experiments/e1_small_files/) | 40,983 small files → 21 compacted files | Glue Spark | 1,323 tasks, 167.5 s | 21 tasks, 0.9 s | **~184x faster** | $0.041 → $0.0002 (Glue, 2 DPU) |
| [E2](experiments/e2_partition_pruning/) | Flat table → partitioned by `pickup_date` | Athena | 12.9 MB scanned | 140 KB scanned | **~92x fewer bytes** | billed 13 MB → 10 MB (Athena's 10 MB minimum) |
| [E3](experiments/e3_format/) | CSV → Parquet (3 of 20 columns read) | Athena | 17.82 GB scanned | 0.44 GB scanned | **~40x fewer bytes**, 6.9x less storage | **$0.089 → $0.0022** per query |
| [E4](experiments/e4_redshift_keys/) | No sort key → sorted + zone map (BRIN) | Postgres 16 (Redshift fallback) | 936,582 blocks, 57.6 s cold | 46,369 blocks, 14.1 s cold | **~20x fewer blocks**, 4.1x faster cold | — (single EC2 box) |
| [E5](experiments/e5_join_strategy/) | Sort-merge join → broadcast join (265-row dim) | Glue Spark | 1.27 GB shuffle, 82.0 s | 9.7 KB shuffle, 27.9 s | **~2.9x faster**, shuffle eliminated | $0.020 → $0.0068 (Glue, 2 DPU) |
| [E6](experiments/e6_iceberg_stats/) | Unsorted Iceberg → Iceberg sorted on filter column | Athena | 64/64 files, 375 MB | 1/64 files, 4.1 MB | **~91x fewer bytes** | $0.0019 → $0.00005 per query |

Every number above is measured on **167,858,646 real NYC TLC yellow-taxi
trips (2022–2025)** — except E4, which uses the 41,169,691 trips from
2024 — on real AWS resources: three runs per variant, cold first run
excluded from the median, every individual run in
[`results/raw_measurements.csv`](results/raw_measurements.csv). Exact
queries, raw plans, and the "why it worked / when it wouldn't help"
analysis are in each experiment's README. Method: [`METHODOLOGY.md`](METHODOLOGY.md).
Cost-per-run figures: Athena at $5/TB scanned (10 MB minimum per query);
Glue as the run's share of a 2-DPU job at $0.44/DPU-hour (Glue's 1-minute
minimum applies per job, not per query).

**What one format choice is worth** (E3, extrapolated): a dashboard
running that 3-column query every hour costs **$2.14/day on CSV vs
$0.053/day on Parquet**, before any other optimization.

**Total AWS spend for the whole benchmark: ~$0.97** — itemized, including
failed and repeated runs, in [`docs/COST.md`](docs/COST.md).

## What the numbers don't say on their own

The findings that took digging, each documented with the plan or metric
that shows it:

- **E2 — the "unoptimized" baseline was already pruning.** `EXPLAIN
  ANALYZE` shows Athena reading only 3.56M of 167.8M rows from the flat
  table: the data is naturally time-ordered, so Parquet row-group min/max
  stats skipped most of it. Partitioning still removed the remaining 31x
  of wasted rows. And because Athena bills a 10 MB minimum per query, the
  92x byte reduction is only 1.3x in real money at this scale.
- **E3 — Parquet was 40x cheaper but *slower*** on Athena engine time
  (6.2 s vs 2.8 s for CSV). Reported as measured; Athena bills bytes, not
  seconds.
- **E5 — "I disabled broadcast" did not produce a sort-merge join.**
  Spark's Adaptive Query Execution re-planned the forced join into a
  shuffled hash join at runtime; a true sort-merge needed an explicit
  `MERGE` hint. Read the *final* plan, not the initial one.
- **E6 — Iceberg alone did nothing; the sort did the work.** Iceberg
  without a sort order pruned 0 of 64 files and scanned *more* than plain
  Parquet. Plain Parquet with the same sort scanned even less than
  Iceberg (0.13 MB vs 4.1 MB), because Athena used Parquet page indexes on
  the plain table but not on the Iceberg one.
- **E4 — part of the warm-run speedup was RAM, not the sort key.** The
  sorted table's matching blocks fit in the OS page cache of a 1 GB box;
  the unsorted table never could. The headline uses the cold-run and
  block-count figures instead.

## Substitutions from the original plan

Documented as incidents in [`docs/RUNBOOK.md`](docs/RUNBOOK.md), not
silently swapped:

| Planned | Used | Why |
|---|---|---|
| Redshift Serverless (E4: `DISTKEY`/`SORTKEY`, `SVL_QUERY_REPORT`) | Postgres 16 on one EC2 t3.micro: sort order + BRIN index, `EXPLAIN (ANALYZE, BUFFERS)` | `SubscriptionRequiredException` on this AWS account (incident 1). Shows the sort-key/zone-map principle; Redshift's distribution styles need multiple nodes and aren't shown — E5's broadcast join is the closest analog to `DISTSTYLE ALL` |
| EMR Serverless for Spark | AWS Glue 4.0 jobs | Same account restriction, hit first by the sibling `retail-lakehouse-aws` project |
| 200M rows | 167,858,646 rows | What 48 months of TLC yellow-taxi data actually contains |
| E4 at full scale | 2024 only (41.2M rows, all columns) | Fits a t3.micro's 1 GB RAM and 30 GB disk for a same-day session |

## Data

- **[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)**,
  yellow taxi, 2022-01 through 2025-12, plus the 265-row **Taxi Zone
  Lookup** (the join dimension for E4/E5).
- Real data-quality issues handled, not hidden: `passenger_count` is
  DOUBLE in files through 2023-01 and BIGINT after; `cbd_congestion_fee`
  physically appears only from 2025-01 (NYC congestion pricing — verified
  per file, not assumed); dirty records include negative fares and pickup
  timestamps from 2001 in a 2022–2025 dataset (visible as stray
  partitions in E2).
- Four physical variants built once with DuckDB
  (`setup/build_variants.py`): CSV flat, Parquet flat, Parquet partitioned
  by `pickup_date`, Parquet in 40,983 small files.

## Repo layout

```
data-perf-teardown/
├── README.md                 ← you are here
├── METHODOLOGY.md            ← run rules, engines, cost basis, limits
├── setup/                    ← download, schema checks, variant builds, Athena runner
├── jobs/                     ← Glue (Spark) jobs for E1, E5, E6
├── experiments/e1..e6/       ← per-experiment README (+ run.py / collect.py / e4_run.sh)
├── results/
│   ├── raw_measurements.csv  ← every individual run
│   └── e4/                   ← raw Postgres EXPLAIN JSON + table sizes
├── infra/                    ← Terraform: S3, Glue catalog/crawler/jobs, Athena workgroup, E4 EC2 (off by default)
└── docs/                     ← COST.md, RUNBOOK.md (incidents), AWS_SETUP.md, PROJECT_BRIEF.md
```

## Reproduce

Region `ap-south-1`, Terraform + AWS CLI + Python 3.12:

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt
python setup/download_tlc_data.py
for v in C B D A; do python setup/build_variants.py $v; done
cd infra && terraform apply                 # S3, Glue, Athena (no compute)
# sync data/variants/* to the bucket prefixes in infra/s3.tf, run the crawler, then:
python experiments/e2_partition_pruning/run.py
python experiments/e3_format/run.py
aws glue start-job-run --job-name data-perf-teardown-e1-small-files    # likewise e5, e6
python experiments/e6_iceberg_stats/run.py
terraform apply -var e4_enabled=true        # E4 box; see experiments/e4_redshift_keys/
terraform apply                             # ...and tear it down the same session
```

The only continuously-billing resource is E4's EC2 instance, which is
off by default.
