# Cost tracking

Real spend on this project's AWS resources, tracked as it happens —
including failed/wasted attempts, not just successes. Combined with the
sibling `retail-lakehouse-aws` project, both share one AWS Free Plan
account ($200 credit ceiling, 6-month/credit-exhaustion cap).

Sibling project's spend so far: ~$5.3-5.6 (see `retail-lakehouse-aws/docs/COST.md`).

| Date | Item | Cost | Notes |
|---|---|---|---|
| 2026-08-27 | Redshift Serverless risk-check API calls | $0 | `list-namespaces`/`list-workgroups` both failed with `SubscriptionRequiredException` before any resource was created — see `docs/RUNBOOK.md` incident 1 |
| 2026-08-27 | Terraform apply: S3 buckets (data + athena-results), Glue database, Glue crawler + IAM role, Athena workgroup | ~$0 | All free-tier-eligible; no compute; S3 storage cost begins accruing once data lands (negligible at ~2-3GB) |
| 2026-08-27 | TLC data download (48 monthly files + zone lookup, 2.6GB) | $0 | Public CloudFront URLs, no AWS egress cost on the download side |
| 2026-08-27 | S3 sync of all 4 variants (~25.6GB total across csv-flat/parquet-flat/parquet-partitioned/parquet-smallfiles) | ~$0.001 | PUT request cost only (~41,636 objects total, mostly from variant D's 40,983 small files — at $0.005/1000 PUTs in ap-south-1 that's ~$0.0002, genuinely negligible) |
| 2026-08-27 | Glue crawler runs (2x — first run mis-targeted the partitioned-table prefix, corrected and rerun) | ~$0.02 | First run's mistake cost real (tiny) money — see docs/RUNBOOK.md incident 2 |
| 2026-08-27 | Athena validation queries (parquet_flat, parquet_partitioned, parquet_smallfiles count(*)) | $0 | All three scanned 0 bytes per Athena's stats (metadata-only count optimization) |
| 2026-08-27 | Athena validation query on csv_flat count(*) — **FAILED**, hit the 10GB/query workgroup cap | $0 | Query never executed past the cap check, so no scan cost incurred — but this is the real, unprompted E3 preview: CSV has no metadata-only count shortcut and forces a real full-table scan just to count rows. Validated row count locally via DuckDB instead (167,858,646, matches all other variants). |

| 2026-08-27 | E2 Athena queries (flat vs partitioned, 3 runs each) | ~$0.0002 | flat scan 12.9MB/query, partitioned scan 140KB/query — real ~91.8x reduction |
| 2026-08-27 | E3 Athena queries — cap raised to 20GB, CSV 3-col scan (3 runs) + Parquet 3-col scan (3 runs), cap reverted to 10GB | ~$0.27 | CSV: 17.82GB/query (full scan regardless of columns); Parquet: 0.44GB/query — real ~40.2x reduction. Cap raise/revert both applied via Terraform, documented not silent. |
| 2026-08-27 | E1 Glue job (`data-perf-teardown-e1-small-files`, G.1X x2 workers, 672s) | ~$0.16 | 3 runs each on smallfiles (40,983 files) vs compacted (21 files) variants — real 1,323 vs 21 Spark tasks, ~167s vs ~0.9s runtime (~184x) for the identical 167.8M rows |

**Running total, this project: ~$0.45** as of 2026-08-27.

**Anticipated cost drivers, in order of risk** (per `PROJECT_BRIEF.md` §5
budget-discipline rule — estimate bytes before running, especially E3's
CSV variant which can produce large full-table scans):

1. **E3 (format/column pruning)**: CSV variant is expected ~4-5x the
   Parquet size. A careless `SELECT *` here is the single largest realistic
   scan-cost risk in this project. Always run `EXPLAIN`/check estimated
   bytes before executing.
2. **S3 storage**: source dataset is ~2-3GB compressed; four physical
   variants (A-D) plus derived Iceberg table push total storage to
   perhaps 10-15GB. At S3 standard pricing (~$0.023/GB-month in
   ap-south-1) this is well under $1/month even left running.
3. **Athena queries**: $5/TB scanned, workgroup-capped at 10GB/query
   (~$0.05 max per query). Three runs x six experiments x modest per-query
   scans should total low single-digit dollars.
4. **Glue crawler runs**: manual-trigger only (no schedule), a few cents
   per run.
5. **EC2 `t3.micro` for E4's Postgres fallback**: always-free tier
   (750 hrs/month) should cover a same-day session at $0, but will be
   created and torn down the same day regardless, per the brief's
   same-day-teardown rule (originally written for Redshift Serverless,
   applied here to its substitute).

Will update this table after each session with real numbers, not
estimates, once resources are actually used.
