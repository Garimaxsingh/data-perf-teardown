# Project Brief: data-perf-teardown

Planning document written before the build: the original spec, the
execution plan shared with the sibling `retail-lakehouse-aws` project, and
the lessons from that project that apply directly here. Kept as written
(plan, not results) — see `README.md` for what was actually measured and
`docs/RUNBOOK.md` for every deviation from this plan.

---

## 1. What this project is

**Pitch**: a controlled benchmark of six common data-engineering
performance mistakes on 150-200M rows of real NYC TLC taxi data, each with
before/after runtime, bytes scanned, and dollar cost. Repo name:
`data-perf-teardown`.

**Why it matters more than its size suggests**: Amazon's DE interview loop
scores "Frugality" and "Dive Deep" as Leadership Principles. A README whose
first table reads "$4.80/day → $0.31/day" does more for a recruiter screen
than three more pipeline projects. Almost no junior portfolio has real,
measured cost numbers — this project's entire point is to have them.

**Relationship to the sibling project**: this account also hosts
`retail-lakehouse-aws` (Projects 1, a 100M-row lakehouse with batch +
streaming + DQ, built first — §4 lists what carried over from
it). They are two separate portfolio pieces sharing one AWS account and one
$200 Free Plan credit budget, not either/or.

---

## 2. Environment (at planning time, 2026-08-26)

- **AWS account**: on the **Free Plan** ($100 signup + $100 earnable,
  6-month/credit-exhaustion cap, cannot be billed past that). Shared with
  `retail-lakehouse-aws`.
- **Region**: `ap-south-1` (Mumbai) — same as the sibling project.
- **Tooling**: AWS CLI v2, Terraform, Python 3.12 (`python -m venv .venv`,
  install from `requirements.txt`).
- **Spend before this project started**: $0 here; ~$5.5-5.8 on the sibling
  project, nowhere near the ~$200 Free Plan ceiling.

---

## 3. What must be done — the six experiments

| # | Experiment | Compares | Measure |
|---|---|---|---|
| E1 | Small files | ~40,000 × 200KB files vs compacted 128MB targets | Task count, runtime, S3 GET count |
| E2 | Partition pruning | Flat Parquet vs partitioned by `pickup_date` | **Bytes scanned via Athena** (your dollar figure), runtime |
| E3 | Format + column pruning | CSV vs Parquet, 3-of-19 columns touched | Bytes scanned, runtime, storage footprint |
| E4 | Redshift DIST/SORT keys | `DISTSTYLE EVEN` vs `DISTKEY`/`SORTKEY`, corrected to `DISTSTYLE ALL` | `SVL_QUERY_REPORT`, `STL_ALERT_EVENT_LOG`, blocks scanned |
| E5 | Spark join strategy | Forced sort-merge vs broadcast join, 200M rows × 265-row dimension | Shuffle read/write bytes, stage count, Spark UI DAG |
| E6 | Iceberg stats pruning | Plain Parquet vs Iceberg column-level stats, highly selective filter | Files scanned, bytes scanned |

### Data

- **NYC TLC Trip Record Data**, yellow taxi, **2022-01 through 2025-12**
  (anonymous download, no AWS account needed for the source):
  `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
  (verify this URL is still current against the official TLC page before
  scripting — it has changed hosts before). ~48 monthly files, landing
  around 150-200M rows but only **~2-3GB compressed** — much smaller than
  it sounds, which keeps S3/Athena costs trivial even with sloppy queries.
- **Taxi Zone Lookup** (`taxi_zone_lookup.csv`, same TLC page): 265 rows —
  `LocationID`, `Borough`, `Zone`, `service_zone`. This is the whole reason
  to use this dataset: 265 rows against 200M is the textbook broadcast-join
  demonstration (E5), and it's the dimension table for E4's DISTKEY story.
- **Three real, not-fabricated data quality gifts** already in this
  dataset — mention all three in the README, they prove you actually read
  the data:
  1. `passenger_count` stored as `DOUBLE` in some monthly files, `INT64` in
     others — genuine schema reconciliation needed.
  2. A `cbd_congestion_fee` column added to Yellow/Green/HVFHV for 2025
     onward (NYC congestion pricing, effective 2025-01-05) — a *real*
     physical schema-evolution event, unlike the sibling project's
     synthetic `promotion_code` field, which turned out to only be a
     population-rate change, not a real column addition (see §4's lesson
     #4 — check this one for real before describing it as such in your
     README).
  3. Genuine dirty records at scale: negative fares, zero-distance $200
     trips, timestamps outside the file's own month, `passenger_count` of
     0 or 9.

### Four physical variants (your experiment inputs)

| Variant | Prefix | Purpose |
|---|---|---|
| A. CSV, unpartitioned | `raw/csv-flat/` | Format/column-pruning baseline |
| B. Parquet, unpartitioned | `raw/parquet-flat/` | Partition-pruning baseline |
| C. Parquet, partitioned by `pickup_date` | `optimized/parquet-partitioned/` | The good version |
| D. Parquet, ~40,000 small files (~200KB each) | `raw/parquet-smallfiles/` | Small-file baseline |

Build C first (partition by pickup_date), derive D by repartitioning C to
~40,000 files, derive A by writing C out as CSV (expect ~4-5× the size —
itself a result worth recording).

---

## 4. Hard lessons from Project 1 — read before building infra

These cost real debugging time and real (small) money on the sibling
project. Apply them from the start here instead of rediscovering them.

1. **EMR Serverless and Kinesis Data Streams BOTH hit
   `SubscriptionRequiredException`** on this AWS account — two independent
   "Serverless"-branded/newer service families failing with the identical
   error, which points at a real account limitation, not bad luck or
   new-account activation lag (the account was already 2 days old when
   this happened). **Implication for this project: check Redshift
   Serverless availability FIRST, before building anything around E4.**
   Try `aws redshift-serverless create-workgroup` (or just attempt the
   Terraform resource) as a cheap, fast, early test. If it also fails with
   `SubscriptionRequiredException`, don't fight it — the spec's own
   fallback is ClickHouse or Postgres on an EC2 `t3.micro` (always-free
   tier) to demonstrate the same sort-order/clustering-key principle.
   Document the substitution honestly, the same way Project 1 documented
   EMR→Glue Jobs and Kinesis→file-source in its `docs/RUNBOOK.md`.
2. **Use AWS Glue ETL Jobs, not EMR Serverless**, for whatever Spark
   compute this project needs (E1, E5, E6 could all run as Glue jobs, or
   you may be able to do all of E1/E2/E3/E5/E6 in Athena directly and only
   need Spark for E5's join-strategy DAG inspection). Glue Jobs worked
   reliably on this account once configured correctly.
3. **The small-files problem is real and expensive if it surprises you.**
   Project 1 accidentally generated 96,289 small files and a Glue job hit
   a 60-minute TIMEOUT trying to read them (cost ~$1.76 wasted before the
   fix). Here, E1 is *deliberately* about exactly this — good, that's the
   point — but be deliberate about which OTHER experiments (E2, E3, E5,
   E6) use variant D vs variant C, so you don't accidentally tank an
   unrelated experiment's runtime with the small-files variant.
4. **Verify real schema evolution before claiming it.** Project 1 wrote
   documentation claiming a synthetic field was "genuinely absent" from
   pre-cutover data, and this turned out to be false when actually
   inspected (the field always existed, just null) — caught late, had to
   correct three docs. Before writing anything about `cbd_congestion_fee`
   "not existing" in pre-2025 files, actually check a pre-2025 file's
   schema directly (`pyarrow.parquet.read_schema()`), don't assume from
   the TLC changelog description.
5. **Windows Git Bash + AWS CLI**: any AWS CLI argument starting with `/`
   (like `--log-group-name /aws-glue/...`) gets mangled by MSYS path
   conversion. Prefix the command with `MSYS_NO_PATHCONV=1` when this
   happens (e.g. `MSYS_NO_PATHCONV=1 aws logs describe-log-groups
   --log-group-name-prefix "/aws-glue"`).
6. **Iceberg partitioned writes need sorted/clustered input.** Writing an
   unsorted DataFrame to a `partitionedBy(...)` Iceberg table at any
   meaningful row count throws `IllegalStateException: Incoming records
   violate the writer assumption that records are clustered by spec`.
   Always `.sort(partition_column)` (or set
   `write.distribution-mode=hash`) immediately before
   `.writeTo(...).createOrReplace()` on a partitioned table. This will
   matter for E6 (Iceberg table) and any partitioned Athena/Glue table
   here.
7. **Nanosecond-precision timestamps break Spark's Parquet reader**
   (`Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`). If writing
   any Parquet from pandas with `pd.to_timedelta`-derived timestamp
   columns and later reading them with Spark, explicitly cast to
   microsecond precision (`.astype("datetime64[us]")`) at write time.
8. **`MERGE INTO` is disallowed inside a Spark Structured Streaming
   micro-batch's execution thread** in this Glue 4.0 / Spark 3.3 setup —
   irrelevant here unless E4 or E5 end up needing a streaming component
   (they shouldn't), but worth knowing if you reach for MERGE anywhere.
9. **Real cost discipline that worked well**: keep a running `docs/COST.md`
   updated *as you go*, including failed/wasted attempts, not just
   successes — it becomes genuine Frugality-story material for the resume,
   and Project 1's own honest accounting of ~$2.40 in debugging-attempt
   waste before a working run was more interesting material than a clean
   number would have been.
10. **Confirm the Free Plan service list before building**, don't defer it
    like Project 1 did (which caused two mid-build pivots). Check
    `aws.amazon.com/free/` from inside the console, or just attempt the
    riskiest resource (Redshift Serverless, for E4) very early, even
    before building E1-E3.

---

## 5. How to sequence this (the "two-weekend plan," adjusted)

| Session | Work |
|---|---|
| 1 | **First**: attempt Redshift Serverless creation as an early risk check (see lesson #1). Then: Terraform for a new S3 bucket + Glue database (mirror Project 1's `infra/s3.tf`/`infra/glue.tf` patterns — they're proven to work on this account). Download TLC data (2022-01 to 2025-12), resolve the `passenger_count` type conflict, land raw Parquet in S3. |
| 2 | Build variants A-D. Set up Athena tables (crawler or explicit DDL) and an Athena workgroup with a bytes-scanned cap (copy Project 1's `infra/athena.tf` pattern — 10GB cap worked well there). |
| 3 | E1, E2, E3 — all Athena-queryable, cheap and fast. |
| 4 | E5, E6 — needs Spark (Glue Job) for E5's shuffle/DAG inspection; E6 needs an actual Iceberg table with column stats. |
| 5 | **Redshift day** (E4) — single focused session: workgroup up, load, run both queries, capture `SVL_QUERY_REPORT`, drop the workgroup. Same-day teardown, not left running. |
| 6 | Write-ups: mechanism paragraphs (why each fix worked, and when it *wouldn't* help — this is the differentiator per the original spec), README results table, cost extrapolation, `docs/COST.md`, `METHODOLOGY.md`. |

**Rigor rules from the original spec, still binding**: three runs per
experiment, report the median, discard the first (cold-cache) run; same
compute size across all runs of one experiment; run comparisons back to
back, not spread across days; commit the exact query alongside every
number; `results/raw_measurements.csv` holds every individual run, not
just medians.

**Budget discipline**: estimate bytes before running E3 (CSV full scans
can be large) and set an AWS Budget alarm before starting, same pattern as
Project 1's three-tier alarm ($25/$75/$150).

---

## 6. Definition of done

- All six experiments have real, measured before/after numbers with the
  exact query committed.
- README opens with the results table, nothing above it.
- Every experiment section has: setup, exact query, measurement
  (screenshot or Athena/Spark stat), **why it worked**, and **when it
  wouldn't help** (the honest-tradeoffs section — this is what separates
  this repo from a tutorial clone, per the original spec).
- `docs/COST.md` tracks real spend, including any wasted/failed attempts,
  same discipline as Project 1.
- Any substitution from the original plan (Redshift fallback, if needed)
  is documented in a RUNBOOK-style incident log, not silently swapped in.

---

## 7. What NOT to build (explicit non-goals, from the original spec)

No Streamlit dashboard. No ML model. No Kafka-in-Docker tutorial
architecture. No "real-time analytics" claim that's actually five-minute
batch. No unexplained benchmark numbers.
