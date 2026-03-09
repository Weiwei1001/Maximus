#!/usr/bin/env python3
"""
Generate ClickBench benchmark data.

Downloads the ClickBench hits.parquet (if not present), then creates
sampled CSV datasets at specified percentages. Converts EventTime/EventDate
to timestamp format required by Maximus.

Usage:
    python generate_clickbench_data.py --output-dir /path/to/output --percentages 10 20
    python generate_clickbench_data.py --output-dir /path/to/output --parquet /path/to/hits.parquet --percentages 10 20
"""
import argparse
import subprocess
import time
from pathlib import Path

import duckdb

PARQUET_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"


def download_parquet(dest: Path):
    """Download ClickBench hits.parquet if not present."""
    if dest.exists():
        size_gb = dest.stat().st_size / (1024 ** 3)
        print(f"  Parquet already exists: {dest} ({size_gb:.1f} GB)")
        return
    print(f"  Downloading hits.parquet from {PARQUET_URL}...")
    subprocess.run(["wget", "-q", "--show-progress", "-O", str(dest), PARQUET_URL], check=True)
    size_gb = dest.stat().st_size / (1024 ** 3)
    print(f"  Downloaded: {size_gb:.1f} GB")


def export_clickbench(parquet_path: Path, output_dir: Path, pct: int):
    """Export a percentage sample of ClickBench data to CSV with timestamp conversion."""
    sf_dir = output_dir / f"sf{pct}"
    sf_dir.mkdir(parents=True, exist_ok=True)
    csv_path = sf_dir / "t.csv"

    if csv_path.exists():
        print(f"  {pct}%: CSV already exists, skipping")
        return

    print(f"  {pct}%: Creating sampled dataset...")
    t0 = time.time()

    con = duckdb.connect(":memory:")

    # Load parquet and sample
    con.execute(f"CREATE TABLE hits AS SELECT * FROM read_parquet('{parquet_path}') USING SAMPLE {pct} PERCENT (bernoulli)")

    # Get column info for timestamp conversion
    cols_info = con.execute("PRAGMA table_info(hits)").fetchall()

    select_parts = []
    for col in cols_info:
        name = col[1]
        dtype = col[2].upper()
        if name == "EventTime":
            select_parts.append(f"strftime(to_timestamp({name}), '%Y-%m-%dT%H:%M:%S.000000000') AS {name}")
        elif name == "EventDate":
            select_parts.append(
                f"strftime(CAST(make_date(1970, 1, 1) + INTERVAL ({name}) DAY AS TIMESTAMP), "
                f"'%Y-%m-%dT%H:%M:%S.000000000') AS {name}"
            )
        elif "VARCHAR" in dtype or "TEXT" in dtype:
            # Remove newlines in string fields (Maximus CSV parser limitation)
            select_parts.append(f"REPLACE(REPLACE({name}, chr(10), ' '), chr(13), ' ') AS {name}")
        else:
            select_parts.append(name)

    select_sql = ", ".join(select_parts)
    con.execute(f"COPY (SELECT {select_sql} FROM hits) TO '{csv_path}' (HEADER, DELIMITER ',')")

    row_count = con.execute("SELECT count(*) FROM hits").fetchone()[0]
    con.close()

    size_gb = csv_path.stat().st_size / (1024 ** 3)
    elapsed = time.time() - t0
    print(f"  {pct}%: Done ({row_count:,} rows, {size_gb:.1f} GB, {elapsed:.1f}s)")


def main():
    parser = argparse.ArgumentParser(description="Generate ClickBench data as CSV")
    parser.add_argument("--output-dir", type=str, required=True, help="Output directory for CSV files")
    parser.add_argument("--parquet", type=str, default=None,
                        help="Path to hits.parquet (will download if not specified)")
    parser.add_argument("--percentages", type=int, nargs="+", default=[5, 10, 20],
                        help="Percentage samples to create (default: 5 10 20)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Handle parquet file
    if args.parquet:
        parquet_path = Path(args.parquet)
    else:
        parquet_path = output_dir / "hits.parquet"
        download_parquet(parquet_path)

    print(f"Generating ClickBench data: {args.percentages}%")
    print(f"Output: {output_dir}")

    for pct in args.percentages:
        export_clickbench(parquet_path, output_dir, pct)

    print("All ClickBench data generated.")


if __name__ == "__main__":
    main()
