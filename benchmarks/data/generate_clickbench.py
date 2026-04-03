#!/usr/bin/env python3
"""
ClickBench data generation.
Downloads the ClickBench dataset and creates DuckDB databases and CSV files
at various scale factors (equivalent TPC-H SF based on data size ratio).

Scale factor mapping (SF = csv_size_gb / tpch_sf1_size_gb ≈ 3.2):
  SF=2  ≈ 10% sample  ≈  7 GB CSV
  SF=6  ≈ 30% sample  ≈ 20 GB CSV
  SF=13 ≈ 60% sample  ≈ 42 GB CSV
  SF=22 ≈ 100% sample ≈ 70 GB CSV

Usage:
    python generate_clickbench.py                      # Default SFs: 2,6,13,22
    python generate_clickbench.py --scales 2 6 13 22   # Explicit SFs
    python generate_clickbench.py --format csv         # CSV for Maximus
    python generate_clickbench.py --format duckdb      # DuckDB for Sirius
    python generate_clickbench.py --parquet-path /path/to/clickbench.parquet
"""

import argparse
import os
import sys
import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CLICKBENCH_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"

# ClickBench SF → sample percentage mapping.
# SF is defined as: csv_data_size / tpch_sf1_csv_size (≈3.2 GB).
_SF_TO_SAMPLE_PCT = {
    2: 10,
    6: 30,
    13: 60,
    22: 100,
}


def sf_to_sample_pct(sf: int) -> float:
    """Convert a ClickBench SF to a sample fraction (0..1).

    Uses the predefined mapping for known SFs, otherwise computes:
      sample_pct = sf * 3.2 / 70.1 * 100   (based on full dataset = 70.1 GB).
    """
    if sf in _SF_TO_SAMPLE_PCT:
        return _SF_TO_SAMPLE_PCT[sf] / 100.0
    return min(1.0, sf * 3.2 / 70.1)


def download_parquet(parquet_path: Path):
    """Download ClickBench parquet file if not present."""
    if parquet_path.exists():
        size_gb = parquet_path.stat().st_size / (1024 ** 3)
        print(f"  Parquet exists: {parquet_path} ({size_gb:.1f} GB)")
        return
    print(f"  Downloading ClickBench dataset to {parquet_path}...")
    print(f"  URL: {CLICKBENCH_URL}")
    print("  (This is ~14 GB, may take a while)")
    subprocess.check_call(["wget", "-q", "--show-progress", "-O",
                           str(parquet_path), CLICKBENCH_URL])
    size_gb = parquet_path.stat().st_size / (1024 ** 3)
    print(f"  Downloaded: {size_gb:.1f} GB")


def generate_duckdb(parquet_path: Path, out_dir: Path, scale_factors: list):
    """Generate DuckDB databases at various scale factors."""
    import duckdb
    con = duckdb.connect(":memory:")
    total = con.execute(
        f"SELECT count(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    con.close()
    print(f"  Total rows in parquet: {total:,}")

    for sf in scale_factors:
        out_db = out_dir / f"clickbench_{sf}.duckdb"
        if out_db.exists():
            out_db.unlink()
        con = duckdb.connect(str(out_db))
        pct = sf_to_sample_pct(sf)
        if pct >= 1.0:
            con.execute(f"CREATE TABLE t AS SELECT * FROM "
                        f"read_parquet('{parquet_path}')")
        else:
            limit_n = max(1, int(total * pct))
            con.execute(f"CREATE TABLE t AS SELECT * FROM "
                        f"read_parquet('{parquet_path}') LIMIT {limit_n}")
        n = con.execute("SELECT count(*) FROM t").fetchone()[0]
        con.close()
        size_mb = out_db.stat().st_size / (1024 ** 2)
        print(f"  Created {out_db.name}: {n:,} rows "
              f"({size_mb:.0f} MB, SF={sf}, sample={pct*100:.0f}%)")


def generate_csv(parquet_path: Path, out_dir: Path, scale_factors: list):
    """Generate CSV files at various scale factors for Maximus."""
    import duckdb
    con = duckdb.connect(":memory:")
    total = con.execute(
        f"SELECT count(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    con.close()

    for sf in scale_factors:
        csv_dir = out_dir / f"csv-{sf}"
        csv_dir.mkdir(parents=True, exist_ok=True)
        csv_path = csv_dir / "t.csv"
        pct = sf_to_sample_pct(sf)
        con = duckdb.connect(":memory:")
        if pct >= 1.0:
            con.execute(f"COPY (SELECT * FROM read_parquet('{parquet_path}')) "
                        f"TO '{csv_path}' (HEADER, DELIMITER ',')")
        else:
            limit_n = max(1, int(total * pct))
            con.execute(f"COPY (SELECT * FROM read_parquet('{parquet_path}') "
                        f"LIMIT {limit_n}) TO '{csv_path}' "
                        f"(HEADER, DELIMITER ',')")
        con.close()
        size_mb = csv_path.stat().st_size / (1024 ** 2)
        print(f"  Created csv-{sf}/t.csv "
              f"({size_mb:.0f} MB, SF={sf}, sample={pct*100:.0f}%)")


def main():
    parser = argparse.ArgumentParser(description="ClickBench data generation")
    parser.add_argument("--scales", type=int, nargs="+",
                        default=[2, 6, 13, 22],
                        help="Scale factors (equiv TPC-H SF, default: 2 6 13 22)")
    parser.add_argument("--output-dir", "-o", type=str, default=".",
                        help="Output directory")
    parser.add_argument("--format", choices=["csv", "duckdb", "both"],
                        default="both", help="Output format (default: both)")
    parser.add_argument("--parquet-path", type=str, default=None,
                        help="Path to clickbench parquet (auto-downloads if missing)")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = (Path(args.parquet_path) if args.parquet_path
                    else out_dir / "clickbench.parquet")

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
