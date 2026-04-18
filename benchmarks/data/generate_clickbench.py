#!/usr/bin/env python3
"""
ClickBench data generation.
Downloads the ClickBench dataset and creates DuckDB databases and CSV files
at various scale factors, where SF = final CSV data size in GB.

Scale factor semantics: SF=N means the produced t.csv is ≈ N GB.
Full 100% ClickBench CSV ≈ 70 GB, so sample_pct = N / 70.

Default SFs:
  SF=1   →  ≈1 GB CSV  (≈1.4% sample)
  SF=5   →  ≈5 GB CSV  (≈7.1% sample)
  SF=10  →  ≈10 GB CSV (≈14.3% sample)
  SF=20  →  ≈20 GB CSV (≈28.6% sample)

Usage:
    python generate_clickbench.py                      # Default SFs: 1,5,10,20
    python generate_clickbench.py --scales 1 5 10 20   # Explicit SFs (GB)
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

# Full 100% ClickBench CSV size in GB. Used to translate SF (= target CSV GB)
# into a sample fraction: sample_pct = sf_gb / full_csv_gb.
_CLICKBENCH_FULL_CSV_GB = 70.0


def sf_to_sample_pct(sf: int) -> float:
    """Convert a ClickBench SF (target CSV size in GB) to a sample fraction (0..1)."""
    return min(1.0, float(sf) / _CLICKBENCH_FULL_CSV_GB)


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
    """Generate CSV files at various scale factors for Maximus.

    EventTime/EventDate are converted from raw Unix-second integers to ISO
    timestamp strings so the Arrow CSV reader (CPU storage path) can parse
    them as `timestamp[ns]`. Newlines inside string columns are stripped to
    avoid breaking row delimiters.
    """
    import duckdb
    con = duckdb.connect(":memory:")
    total = con.execute(
        f"SELECT count(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    cols_info = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}') LIMIT 1"
    ).fetchall()
    con.close()

    select_parts = []
    for col in cols_info:
        name = col[0]
        dtype = str(col[1]).upper()
        if name == "EventTime":
            select_parts.append(
                f"strftime(to_timestamp({name}), '%Y-%m-%dT%H:%M:%S.000000000') AS {name}"
            )
        elif name == "EventDate":
            select_parts.append(
                f"strftime(CAST(make_date(1970, 1, 1) + INTERVAL ({name}) DAY AS TIMESTAMP), "
                f"'%Y-%m-%dT%H:%M:%S.000000000') AS {name}"
            )
        elif "VARCHAR" in dtype or "TEXT" in dtype:
            select_parts.append(
                f"REPLACE(REPLACE({name}, chr(10), ' '), chr(13), ' ') AS {name}"
            )
        else:
            select_parts.append(name)
    select_sql = ", ".join(select_parts)

    for sf in scale_factors:
        csv_dir = out_dir / f"csv-{sf}"
        csv_dir.mkdir(parents=True, exist_ok=True)
        csv_path = csv_dir / "t.csv"
        pct = sf_to_sample_pct(sf)
        con = duckdb.connect(":memory:")
        if pct >= 1.0:
            con.execute(
                f"COPY (SELECT {select_sql} FROM read_parquet('{parquet_path}')) "
                f"TO '{csv_path}' (HEADER, DELIMITER ',')"
            )
        else:
            limit_n = max(1, int(total * pct))
            con.execute(
                f"COPY (SELECT {select_sql} FROM read_parquet('{parquet_path}') "
                f"LIMIT {limit_n}) TO '{csv_path}' "
                f"(HEADER, DELIMITER ',')"
            )
        con.close()
        size_mb = csv_path.stat().st_size / (1024 ** 2)
        print(f"  Created csv-{sf}/t.csv "
              f"({size_mb:.0f} MB, SF={sf}, sample={pct*100:.0f}%)")


def main():
    parser = argparse.ArgumentParser(description="ClickBench data generation")
    parser.add_argument("--scales", type=int, nargs="+",
                        default=[1, 5, 10, 20],
                        help="Scale factors (target CSV size in GB, default: 1 5 10 20)")
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
