"""
Inspect schemas of downloaded monthly Parquet files to:
  1. Confirm/quantify the passenger_count DOUBLE vs INT64 type conflict
     across monthly files (PROJECT_BRIEF.md data-quality item #1).
  2. Actually verify (not assume) whether cbd_congestion_fee exists as a
     real column in pre-2025 files vs a genuine schema addition in 2025+
     (PROJECT_BRIEF.md lesson #4 — don't trust the changelog description,
     check pyarrow.parquet.read_schema() directly).

Run after setup/download_tlc_data.py has landed at least a few files from
both 2022-2024 and 2025 into data/tlc_raw/.
"""
import glob
from pathlib import Path

import pyarrow.parquet as pq

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "tlc_raw"


def main() -> None:
    files = sorted(glob.glob(str(DATA_DIR / "yellow_tripdata_*.parquet")))
    if not files:
        print("No parquet files found yet in", DATA_DIR)
        return

    print(f"Inspecting {len(files)} files...\n")

    passenger_count_types: dict[str, list[str]] = {}
    cbd_fee_presence: dict[str, bool] = {}

    for f in files:
        month = Path(f).stem.replace("yellow_tripdata_", "")
        schema = pq.read_schema(f)
        names = schema.names

        if "passenger_count" in names:
            t = str(schema.field("passenger_count").type)
            passenger_count_types.setdefault(t, []).append(month)

        cbd_fee_presence[month] = "cbd_congestion_fee" in names

    print("=== passenger_count dtype by month ===")
    for t, months in passenger_count_types.items():
        print(f"  {t}: {len(months)} files, e.g. {months[:3]}...{months[-3:]}")

    print("\n=== cbd_congestion_fee column presence ===")
    present = [m for m, v in cbd_fee_presence.items() if v]
    absent = [m for m, v in cbd_fee_presence.items() if not v]
    print(f"  Present in {len(present)} files: {sorted(present)[:5]}...")
    print(f"  Absent in {len(absent)} files: {sorted(absent)[:5]}...")

    # If present anywhere pre-2025, check whether it's all-null there
    # (population-rate change) vs missing entirely (real schema addition) —
    # this only tells us column *presence*; null-rate check requires reading
    # data, not just schema, and should be done separately once this proves
    # the column is genuinely absent (not just always-null) pre-2025.
    pre_2025_present = [m for m in present if m < "2025-01"]
    if pre_2025_present:
        print(
            f"\n  WARNING: cbd_congestion_fee IS present in {len(pre_2025_present)} "
            f"pre-2025 file(s): {pre_2025_present} — this would mean it is NOT a "
            f"genuine schema addition, same trap as the sibling project's "
            f"promotion_code finding. Do not claim 'schema addition' without "
            f"checking further."
        )
    else:
        print(
            "\n  cbd_congestion_fee is genuinely ABSENT from all pre-2025 files "
            "checked (column doesn't exist in schema at all, not just null) — "
            "safe to describe as a real physical schema-evolution event."
        )


if __name__ == "__main__":
    main()
