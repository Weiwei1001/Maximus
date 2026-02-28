#!/usr/bin/env python3
"""
ClickBench data generation.
Downloads the ClickBench dataset and creates DuckDB databases and CSV files
at various scale factors (percentage of full dataset).

Usage:
    python generate_clickbench.py                    # Default scales: 10,20,50,100
    python generate_clickbench.py --scales 1 2 10    # Custom scales
    python generate_clickbench.py --format csv       # CSV for Maximus
    python generate_clickbench.py --format duckdb    # DuckDB for Sirius
    python generate_clickbench.py --parquet-path /path/to/clickbench.parquet
"""

import argparse
import os
import sys
import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CLICKBENCH_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"


def download_parquet(parquet_path: Path):
    """Download ClickBench parquet file if not present."""
    if parquet_path.exists():
        size_gb = parquet_path.stat().st_size / (1024 ** 3)
        print(f"  Parquet exists: {parquet_path} ({size_gb:.1f} GB)")
        return
    print(f"  Downloading ClickBench dataset to {parquet_path}...")
    print(f"  URL: {CLICKBENCH_URL}")
    print("  (This is ~14 GB, may take a while)")
    subprocess.check_call(["wget", "-q", "--show-progress", "-O", str(parquet_path), CLICKBENCH_URL])
    size_gb = parquet_path.stat().st_size / (1024 ** 3)
    print(f"  Downloaded: {size_gb:.1f} GB")


def generate_duckdb(parquet_path: Path, out_dir: Path, scales: list):
    """Generate DuckDB databases at various scale factors."""
    import duckdb
    con = duckdb.connect(":memory:")
    total = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    con.close()
    print(f"  Total rows in parquet: {total:,}")

    for scale in scales:
        out_db = out_dir / f"clickbench_{scale}.duckdb"
        if out_db.exists():
            out_db.unlink()
        con = duckdb.connect(str(out_db))
        if scale == 100:
            con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{parquet_path}')")
        else:
            pct = scale / 100.0
            limit_n = max(1, int(total * pct))
            con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{parquet_path}') LIMIT {limit_n}")
        n = con.execute("SELECT count(*) FROM t").fetchone()[0]
        con.close()
        size_mb = out_db.stat().st_size / (1024 ** 2)
        print(f"  Created {out_db.name}: {n:,} rows ({size_mb:.0f} MB, scale={scale}%)")


def generate_csv(parquet_path: Path, out_dir: Path, scales: list):
    """Generate CSV files at various scale factors for Maximus."""
    import duckdb
    con = duckdb.connect(":memory:")
    total = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    con.close()

    for scale in scales:
        csv_dir = out_dir / f"csv-{scale}"
        csv_dir.mkdir(parents=True, exist_ok=True)
        csv_path = csv_dir / "t.csv"
        con = duckdb.connect(":memory:")
        if scale == 100:
            con.execute(f"COPY (SELECT * FROM read_parquet('{parquet_path}')) TO '{csv_path}' (HEADER, DELIMITER ',')")
        else:
            pct = scale / 100.0
            limit_n = max(1, int(total * pct))
            con.execute(f"COPY (SELECT * FROM read_parquet('{parquet_path}') LIMIT {limit_n}) TO '{csv_path}' (HEADER, DELIMITER ',')")
        con.close()
        size_mb = csv_path.stat().st_size / (1024 ** 2)
        print(f"  Created {csv_dir.name}/t.csv ({size_mb:.0f} MB, scale={scale}%)")


def main():
    parser = argparse.ArgumentParser(description="ClickBench data generation")
    parser.add_argument("--scales", type=int, nargs="+", default=[1, 2, 10, 20],
                        help="Scale factors as percentage (default: 1 2 10 20)")
    parser.add_argument("--output-dir", "-o", type=str, default=".",
                        help="Output directory")
    parser.add_argument("--format", choices=["csv", "duckdb", "both"], default="both",
                        help="Output format (default: both)")
    parser.add_argument("--parquet-path", type=str, default=None,
                        help="Path to clickbench parquet (auto-downloads if missing)")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = Path(args.parquet_path) if args.parquet_path else out_dir / "clickbench.parquet"

    try:
        import duckdb
    except ImportError:
        print("duckdb not found. Run: pip install duckdb", file=sys.stderr)
        sys.exit(1)

    download_parquet(parquet_path)

    if args.format in ("duckdb", "both"):
        print("\nGenerating DuckDB databases...")
        generate_duckdb(parquet_path, out_dir, args.scales)

    if args.format in ("csv", "both"):
        print("\nGenerating CSV files...")
        generate_csv(parquet_path, out_dir, args.scales)

    print("\nDone.")


if __name__ == "__main__":
    main()
