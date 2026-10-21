"""
Download NYC TLC yellow taxi trip data (2022-01 through 2025-12) and the
taxi zone lookup table into ./data/tlc_raw/. Source URLs confirmed live via
HEAD request on 2026-08-27 (see docs/RUNBOOK.md).

Idempotent: skips a file if it already exists locally with a size matching
the server's Content-Length.
"""
import sys
from pathlib import Path

import requests

BASE = "https://d37ci6vzurychx.cloudfront.net"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "tlc_raw"


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    head = requests.head(url, timeout=30)
    head.raise_for_status()
    expected_size = int(head.headers.get("Content-Length", -1))

    if dest.exists() and dest.stat().st_size == expected_size:
        print(f"  skip (already downloaded): {dest.name}")
        return

    print(f"  downloading: {dest.name} ({expected_size / 1e6:.1f} MB)")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
        tmp.rename(dest)

    actual_size = dest.stat().st_size
    if actual_size != expected_size:
        raise RuntimeError(
            f"size mismatch for {dest.name}: expected {expected_size}, got {actual_size}"
        )


def main() -> None:
    print("Downloading taxi zone lookup...")
    download(f"{BASE}/misc/taxi_zone_lookup.csv", OUT_DIR / "taxi_zone_lookup.csv")

    print("Downloading yellow taxi trip data, 2022-01 through 2025-12...")
    months = [(y, m) for y in range(2022, 2026) for m in range(1, 13)]
    for i, (year, month) in enumerate(months, start=1):
        fname = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
        print(f"[{i}/{len(months)}] {fname}")
        try:
            download(f"{BASE}/trip-data/{fname}", OUT_DIR / fname)
        except requests.HTTPError as e:
            print(f"  WARNING: failed to download {fname}: {e}", file=sys.stderr)

    print("Done.")


if __name__ == "__main__":
    main()
