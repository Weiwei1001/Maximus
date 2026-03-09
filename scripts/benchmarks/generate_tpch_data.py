#!/usr/bin/env python3
"""
Generate TPC-H benchmark data using DuckDB.

Creates DuckDB databases with TPC-H data at specified scale factors,
then exports them to CSV files for use with Maximus.

Usage:
    python generate_tpch_data.py --output-dir /path/to/output --scale-factors 1 2 10 20
"""
import argparse
import time
from pathlib import Path

import duckdb


TPCH_TABLES = ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"]


def generate_tpch(output_dir: Path, sf: int):
    """Generate TPC-H data at the given scale factor and export to CSV."""
    sf_dir = output_dir / f"sf{sf}"
    sf_dir.mkdir(parents=True, exist_ok=True)

    # Check if already generated
    if all((sf_dir / f"{t}.csv").exists() for t in TPCH_TABLES):
        print(f"  SF{sf}: CSV files already exist, skipping")
        return

    print(f"  SF{sf}: Generating TPC-H data...")
    t0 = time.time()

    con = duckdb.connect(":memory:")
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={sf})")

    for table in TPCH_TABLES:
        csv_path = str(sf_dir / f"{table}.csv")
        con.execute(f"COPY {table} TO '{csv_path}' (HEADER, DELIMITER ',')")

    con.close()

    total_size = sum(f.stat().st_size for f in sf_dir.glob("*.csv")) / (1024 ** 3)
    elapsed = time.time() - t0
    print(f"  SF{sf}: Done ({total_size:.1f} GB, {elapsed:.1f}s)")


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H data as CSV")
    parser.add_argument("--output-dir", type=str, required=True, help="Output directory for CSV files")
    parser.add_argument("--scale-factors", type=int, nargs="+", default=[1, 2, 5, 10, 20],
                        help="TPC-H scale factors (default: 1 2 5 10 20)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating TPC-H data: SF={args.scale_factors}")
    print(f"Output: {output_dir}")

    for sf in args.scale_factors:
        generate_tpch(output_dir, sf)

    print("All TPC-H data generated.")


if __name__ == "__main__":
    main()
