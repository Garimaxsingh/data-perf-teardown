#!/bin/bash
# E4 (Postgres fallback for Redshift SORTKEY + zone maps), run on the EC2
# instance from infra/e4_postgres.tf via SSM Run Command, as root.
#
#   trips_unsorted  2024 trips loaded in arrival order (by pickup day) —
#                   analog of a Redshift table with no SORTKEY
#   trips_sorted    the same rows, physically sorted on pulocationid —
#                   analog of SORTKEY(pulocationid)
#
# Both get a BRIN index on pulocationid. BRIN stores min/max per range of
# heap blocks, which is the closest single-node equivalent of Redshift's
# per-block zone maps: it can only skip blocks whose value range is narrow,
# i.e. only after the data is sorted. Measurement is EXPLAIN (ANALYZE,
# BUFFERS) — blocks read/hit stand in for SVL_QUERY_REPORT's blocks scanned.
#
# Progress and results are written to s3://$BUCKET/e4-results/.
set -euo pipefail

BUCKET="$1"
W=/data/e4
RES="s3://$BUCKET/e4-results"
mkdir -p "$W" && chmod 755 /data "$W"
cd "$W"

log() {
  echo "$(date -u +%FT%TZ) $*" | tee -a "$W/progress.log"
  aws s3 cp "$W/progress.log" "$RES/progress.log" --quiet || true
}
psql_e4() { sudo -u postgres psql -X -v ON_ERROR_STOP=1 -At -d e4 "$@"; }
# Runs detached (nohup) under SSM, so surface failures through S3.
trap 'log "FAILED at line $LINENO"; aws s3 cp /root/e4.out "$RES/e4.out" --quiet || true' ERR

until [ -f /var/tmp/e4-bootstrap-done ]; do sleep 10; done
log "bootstrap done; $(nproc) vCPU, $(free -m | awk '/Mem:/{print $2}') MB RAM"

# ---------------------------------------------------------------- load ----
log "downloading 2024 partitions of optimized/parquet-partitioned/"
aws s3 cp "s3://$BUCKET/optimized/parquet-partitioned/" parquet/ --recursive \
  --exclude "*" --include "pickup_date=2024-*" --quiet
aws s3 cp "s3://$BUCKET/dim/taxi_zone_lookup/taxi_zone_lookup.csv" zones.csv --quiet
log "downloaded $(find parquet -name '*.parquet' | wc -l) files, $(du -sh parquet | cut -f1)"

log "converting to CSV with DuckDB (arrival order = partition/day order)"
python3 - <<'PY'
import glob
import duckdb
con = duckdb.connect()
con.execute("SET memory_limit='400MB'; SET threads=2; SET preserve_insertion_order=true")
cols = ("vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, "
        "trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, "
        "payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, "
        "improvement_surcharge, total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee")
files = sorted(glob.glob("parquet/pickup_date=2024-*/*.parquet"))
con.execute(f"COPY (SELECT {cols} FROM read_parquet({files!r}, union_by_name=true)) "
            f"TO 'trips.csv' (HEADER, FORMAT CSV)")
PY
rm -rf parquet
chmod 644 trips.csv zones.csv
log "CSV ready: $(du -sh trips.csv | cut -f1)"

sudo -u postgres psql -X -q -c "DROP DATABASE IF EXISTS e4" -c "CREATE DATABASE e4"
psql_e4 <<'SQL'
CREATE TABLE zones (locationid int PRIMARY KEY, borough text, zone text, service_zone text);
COPY zones FROM '/data/e4/zones.csv' CSV HEADER;
CREATE TABLE trips_unsorted (
  vendorid bigint, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp,
  passenger_count double precision, trip_distance double precision, ratecodeid double precision,
  store_and_fwd_flag text, pulocationid bigint, dolocationid bigint, payment_type bigint,
  fare_amount double precision, extra double precision, mta_tax double precision,
  tip_amount double precision, tolls_amount double precision, improvement_surcharge double precision,
  total_amount double precision, congestion_surcharge double precision, airport_fee double precision,
  cbd_congestion_fee double precision
);
SQL
log "COPY into trips_unsorted"
T0=$(date +%s)
psql_e4 -c "COPY trips_unsorted FROM '/data/e4/trips.csv' CSV HEADER"
log "loaded in $(( $(date +%s) - T0 ))s"
rm -f trips.csv

# ----------------------------------------------------------- sorted copy --
log "building trips_sorted (CREATE TABLE AS ... ORDER BY pulocationid)"
T0=$(date +%s)
psql_e4 -c "SET work_mem='64MB'" -c "CREATE TABLE trips_sorted AS SELECT * FROM trips_unsorted ORDER BY pulocationid"
SORT_S=$(( $(date +%s) - T0 ))
log "sorted copy built in ${SORT_S}s"

T0=$(date +%s)
psql_e4 -c "CREATE INDEX trips_unsorted_pu_brin ON trips_unsorted USING brin (pulocationid)" \
        -c "CREATE INDEX trips_sorted_pu_brin ON trips_sorted USING brin (pulocationid)" \
        -c "VACUUM ANALYZE trips_unsorted" -c "VACUUM ANALYZE trips_sorted" -c "ANALYZE zones"
log "BRIN indexes + VACUUM ANALYZE in $(( $(date +%s) - T0 ))s"

psql_e4 -F ',' > sizes.csv <<SQL
SELECT 'rows_unsorted', count(*) FROM trips_unsorted;
SELECT 'rows_sorted', count(*) FROM trips_sorted;
SELECT 'heap_bytes_unsorted', pg_relation_size('trips_unsorted');
SELECT 'heap_bytes_sorted', pg_relation_size('trips_sorted');
SELECT 'heap_blocks_unsorted', pg_relation_size('trips_unsorted') / 8192;
SELECT 'heap_blocks_sorted', pg_relation_size('trips_sorted') / 8192;
SELECT 'brin_bytes_unsorted', pg_relation_size('trips_unsorted_pu_brin');
SELECT 'brin_bytes_sorted', pg_relation_size('trips_sorted_pu_brin');
SELECT 'sort_build_seconds', $SORT_S;
SQL
aws s3 cp sizes.csv "$RES/sizes.csv" --quiet
log "sizes: $(tr '\n' ' ' < sizes.csv)"

# -------------------------------------------------------------- measure --
# Top drop-off zones for JFK (pulocationid = 132) pickups.
QUERY="SELECT z.zone, count(*) AS trips, avg(t.fare_amount) AS avg_fare
FROM TABLE_NAME t JOIN zones z ON z.locationid = t.dolocationid
WHERE t.pulocationid = 132
GROUP BY z.zone ORDER BY trips DESC LIMIT 10"

: > results.jsonl
for tbl in trips_unsorted trips_sorted; do
  for run in 1 2 3; do
    if [ "$run" = 1 ]; then
      # True cold start for run 1: restart Postgres (empties shared_buffers)
      # and drop the OS page cache.
      systemctl restart postgresql; sync; echo 3 > /proc/sys/vm/drop_caches; sleep 5
    fi
    q="${QUERY//TABLE_NAME/$tbl}"
    psql_e4 -c "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) $q" > "explain_${tbl}_run${run}.json"
    python3 - "$tbl" "$run" "explain_${tbl}_run${run}.json" >> results.jsonl <<'PY'
import json, sys
tbl, run, path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
doc = json.load(open(path))[0]
plan = doc["Plan"]
nodes = []
def walk(n):
    nodes.append(n["Node Type"] + (f" on {n['Relation Name']}" if "Relation Name" in n else ""))
    for c in n.get("Plans", []):
        walk(c)
walk(plan)
print(json.dumps({
    "table": tbl, "run": run,
    "execution_ms": doc["Execution Time"],
    "shared_hit_blocks": plan["Shared Hit Blocks"],
    "shared_read_blocks": plan["Shared Read Blocks"],
    "io_read_ms": plan.get("I/O Read Time"),
    "nodes": nodes,
}))
PY
    log "measured $tbl run $run: $(tail -1 results.jsonl | cut -c1-200)"
  done
done
psql_e4 -c "${QUERY//TABLE_NAME/trips_sorted}" > answer_sorted.txt
psql_e4 -c "${QUERY//TABLE_NAME/trips_unsorted}" > answer_unsorted.txt

aws s3 cp "$W/" "$RES/" --recursive --exclude "*" --include "*.json" --include "*.jsonl" --include "*.txt" --include "*.csv" --quiet
log "E4 DONE"
