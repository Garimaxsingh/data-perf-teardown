# Runbook — incidents, substitutions, and honest deviations from the original plan

This log exists so every deviation from `PROJECT_BRIEF.md`'s original spec is
documented plainly, not silently swapped in. Same discipline as the sibling
`retail-lakehouse-aws` project's own `docs/RUNBOOK.md`.

---

## Incident 1: Redshift Serverless unavailable on this AWS account (2026-08-27)

**What happened**: before building any infrastructure around Experiment E4
(Redshift DIST/SORT keys), ran the risk check called for in
`PROJECT_BRIEF.md` lesson #1 — attempted to list Redshift Serverless
resources on this account:

```
aws redshift-serverless list-namespaces --region ap-south-1
aws redshift-serverless list-workgroups --region ap-south-1
```

Both calls failed identically:

```
An error occurred (SubscriptionRequiredException) when calling the
ListNamespaces operation: The AWS Access Key Id needs a subscription for
the service
```

**Why this isn't treated as a transient/retryable error**: this is the
*third* independent AWS "Serverless"-branded service to fail with the exact
same `SubscriptionRequiredException` on this account — EMR Serverless and Kinesis Data Streams both failed
identically while building the sibling `retail-lakehouse-aws` project,
on an account that was already several days old at the time (ruling out
new-account activation lag). Three independent services failing the same
way is a real account-level restriction, not bad luck.

**Decision**: per the brief's own documented fallback, E4 moves from
Redshift Serverless to **Postgres on an EC2 `t3.micro`** (always-free
tier). This demonstrates the same underlying principle Redshift's
DISTKEY/SORTKEY story is about — physical clustering/sort order on disk
changes how much data a query has to scan — just without Redshift's
specific `SVL_QUERY_REPORT`/`STL_ALERT_EVENT_LOG` system tables. Instead,
E4's measurement will use Postgres's own `EXPLAIN (ANALYZE, BUFFERS)`
output (buffers hit/read, actual rows removed by filter) comparing an
unclustered table to one with `CLUSTER ... USING <index>` applied on the
join/filter column — the same "reorganize the physical layout" story,
different engine.

**What this changes in practice**:
- E4's "before" state: a wide fact table loaded in arrival order (mimics
  `DISTSTYLE EVEN` — no useful physical ordering for the query pattern).
- E4's "after" state: same table with `CLUSTER` applied on the
  filter/join column (mimics `DISTKEY`/`SORTKEY` — physically co-locating
  and ordering rows the query will scan together).
- README will state plainly that this is a Postgres substitution for
  Redshift, name the exact reason (`SubscriptionRequiredException`, not a
  design choice), and note what's lost (Redshift's specific terminology
  and system tables) vs. what's preserved (the underlying physical-layout
  principle, measured with real `EXPLAIN ANALYZE` numbers on the same
  200M-row dataset).
- No cost incurred by the failed API calls themselves. EC2 `t3.micro` will
  be created same-day as the E4 experiment and torn down immediately after
  (per the brief's own "Redshift day" same-day-teardown rule, applied here
  to the substitute resource instead).

---

## Incident 2: Glue crawler mis-targeted the partitioned-table prefix (2026-08-27)

**What happened**: the Terraform crawler config pointed its second `s3_target`
at `s3://<bucket>/optimized/` (the parent prefix) rather than
`s3://<bucket>/optimized/parquet-partitioned/` (the actual variant C data).
Since `optimized/` has only one child folder (`parquet-partitioned/`), which
itself contains `pickup_date=...` partition folders, the crawler's
heuristics interpreted `parquet-partitioned` as an extra synthetic partition
level and produced a single wrongly-shaped table named `optimized` with
**2** partition keys instead of the intended `parquet_partitioned` table
with 1 partition key (`pickup_date`).

**Why this didn't happen for the `raw/` target**: `raw/` has three sibling
folders (`csv-flat/`, `parquet-flat/`, `parquet-smallfiles/`) at the same
level, which gave the crawler's schema-similarity heuristic enough contrast
to correctly split them into three separate non-partitioned tables. A
single-child prefix has no such contrast to work from.

**Fix**: updated `infra/glue.tf`'s `s3_target` to point directly at
`optimized/parquet-partitioned/` (matching the same "point at the table
root, not its parent" pattern already used correctly for the `raw/`
sub-tables), `terraform apply`, deleted the stray `optimized` table via
`aws glue delete-table`, reran the crawler. Result: correct
`parquet_partitioned` table with exactly 1 partition key and all 1,481 date
partitions registered, verified via `aws glue get-partitions`.

**Cost impact**: two crawler runs instead of one — genuinely negligible
(~$0.02 total, see `docs/COST.md`), but a real example of Terraform-authored
infra having a real, catchable bug rather than working perfectly on paper.

## Incident 3: Athena's own cost guardrail correctly blocked a CSV full-scan (2026-08-27)

**What happened**: while validating row counts across all four Athena
tables, `SELECT count(*) FROM csv_flat` failed with `Bytes scanned limit
was exceeded` — the workgroup's 10GB/query `bytes_scanned_cutoff_per_query`
guardrail (set in `infra/athena.tf`) rejected the query before it ran. The
same `count(*)` on `parquet_flat`, `parquet_partitioned`, and
`parquet_smallfiles` all succeeded and scanned **0 bytes** (Athena's
Parquet reader can answer `count(*)` from file metadata alone; CSV has no
such shortcut and must read every row).

**Why this is left as-is rather than "fixed"**: this is the guardrail
working exactly as designed, and — unprompted — it's a live, real preview
of Experiment E3's entire point before E3 has even formally started: a
row-based format's cost is insensitive to what you actually need (even a
bare row count forces reading real data), while a columnar format can
answer the same question for free. Row count for `csv_flat` was instead
validated locally via DuckDB directly against the source CSV files
(167,858,646 — matches all three other variants exactly), at zero AWS cost.

## Incident 4: AQE silently replaced the "forced" sort-merge join (2026-10-21)

**What happened**: E5's first Glue run disabled broadcast joins at both
the static planner and AQE (`spark.sql.autoBroadcastJoinThreshold=-1`,
`spark.sql.adaptive.autoBroadcastJoinThreshold=-1`) to force a
sort-merge join. The job's printed physical plan showed the *initial*
plan as `SortMergeJoin`, but the *final* (post-AQE) plan as
`ShuffledHashJoin` — AQE re-planned the join at runtime from real shuffle
statistics. Still a full 1.27 GB shuffle of the fact side, but not the
strategy the experiment claimed to measure.

**Why it's treated as real**: the plan text is printed by the job itself
after execution (`executedPlan` with `isFinalPlan=true`), so this is what
actually ran, not a planning artifact.

**Decision**: kept the unhinted run's numbers (relabelled in the README as
what they are — a shuffled hash join) and added a third variant,
`sort_merge_hinted`, using an explicit `MERGE` join hint, which pinned a
real `SortMergeJoin` (verified in its final plan). Run as a second Glue
job invocation with identical config.

**What this changes**: E5 now reports three strategies instead of two.
The extra variant ran ~1 hour after the other two, not strictly back to
back — stated in `experiments/e5_join_strategy/README.md`. Cost: one extra
~$0.08 Glue run.

**Lesson**: on Spark 3.2+ with AQE on, "I disabled broadcast" does not
mean "I got sort-merge". Always read the *final* plan, not the initial one.

## Incident 5: E6's Iceberg table scanned more than plain Parquet (2026-10-21)

**What happened**: the expected E6 headline was "Iceberg stats beat plain
Parquet". With the same sort and the same 64 files, `parquet_sorted`
scanned 0.13 MB while `iceberg_sorted` scanned 4.11 MB.

**Investigation**: `EXPLAIN ANALYZE` showed Athena reading 40,000 rows
from `parquet_sorted` vs the full 2,669,736-row row group from
`iceberg_sorted`. Downloading the matching file from each layout and
reading its footer with pyarrow showed both have one row group and both
carry Parquet column/offset indexes; the Iceberg scan's 4,105,631 bytes
equals exactly that file's `fare_amount` + `pulocationid` column chunks.
So Athena's Hive-table reader used page-level indexes and its Iceberg
reader read whole column chunks.

**Decision**: reported as measured, not tuned away. The E6 write-up now
credits the sort for the pruning, credits Iceberg specifically for
planning-time file elimination (1 of 64 files, from manifests), and
notes that both sorted layouts bill identically under Athena's 10 MB
per-query minimum.

## Incident log format

Future entries should follow the same shape: what happened, the exact
error, why it's treated as real vs. transient, the decision made, and what
concretely changes as a result.
