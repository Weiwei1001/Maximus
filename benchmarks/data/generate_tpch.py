#!/usr/bin/env python3
"""
TPC-H data generation: generates DuckDB databases for benchmarking.

Steps:
  1. Check/install duckdb via pip
  2. Generate DuckDB databases for specified scale factors using dbgen
  3. Verify schema and run a sample TPC-H Q1

Usage:
  python generate_tpch.py                           # Default: SF 1,5,10,20
  python generate_tpch.py -o ./tpch_data            # Custom output directory
  python generate_tpch.py -sf 1 2                   # Specific scale factors
  python generate_tpch.py --skip-install --no-run-query
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

TPCH_TABLES = [
    "nation", "region", "part", "supplier",
    "partsupp", "customer", "orders", "lineitem",
]


def ensure_duckdb():
    """Install duckdb via pip if not available."""
    try:
        import duckdb
        return True
    except ImportError:
        pass
    print("[1/4] duckdb not found, installing via pip...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "duckdb"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        print("      duckdb installed.")
    except subprocess.CalledProcessError as e:
        print(f"      pip install failed: {e}", file=sys.stderr)
        sys.exit(1)
    try:
        import duckdb
        return True
    except ImportError:
        print("      Cannot import duckdb after install. Run: pip install duckdb", file=sys.stderr)
        sys.exit(1)


def generate_one_db(scale_factor: float, db_path: str, verbose: bool = True):
    """Generate a DuckDB file with TPC-H data at the given scale factor."""
    import duckdb
    if verbose:
        print(f"[2/4] [SF={scale_factor}] Generating: {db_path}")
    conn = duckdb.connect(db_path)
    conn.execute("INSTALL tpch")
    conn.execute("LOAD tpch")
    conn.execute(f"CALL dbgen(sf={scale_factor})")
    if verbose:
        for table in TPCH_TABLES:
            (cnt,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            print(f"      {table}: {cnt:,} rows")
    conn.close()
    if verbose:
        print(f"      [SF={scale_factor}] Done: {db_path}\n")


def verify_schema(db_path: str):
    """Print tables and columns to confirm standard TPC-H schema."""
    import duckdb
    conn = duckdb.connect(db_path, read_only=True)
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    print(f"[3/4] Schema verification: {db_path}")
    print(f"      Tables: {[t[0] for t in tables]}")
    for (tname,) in tables:
        cols = conn.execute(f"DESCRIBE {tname}").fetchall()
        print(f"      {tname}: {[c[0] for c in cols]}")
    conn.close()


def run_tpch_q1_sample(db_path: str):
    """Run a simplified TPC-H Q1 to verify data usability."""
    import duckdb
    print("[4/4] Running TPC-H sample query (Q1)...")
    conn = duckdb.connect(db_path, read_only=True)
    rows = conn.execute("""
        SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, count(*) AS count_order
        FROM lineitem
        WHERE l_shipdate <= date '1998-12-01' - interval '90' day
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """).fetchall()
    conn.close()
    print("      Result (first rows):", rows[:5])
    print("      TPC-H queries can run on this schema (Q1-Q22).\n")


def main():
    parser = argparse.ArgumentParser(
        description="TPC-H data generation for GPU benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", "-o", type=str, default=".",
                        help="DuckDB output directory (default: current dir)")
    parser.add_argument("--scale-factors", "-sf", type=float, nargs="+",
                        default=[1.0, 5.0, 10.0, 20.0], metavar="SF",
                        help="Scale factors (default: 1 5 10 20)")
    parser.add_argument("--skip-install", action="store_true",
                        help="Skip pip install of duckdb")
    parser.add_argument("--no-run-query", action="store_true",
                        help="Skip running sample query after generation")
    parser.add_argument("--quiet", "-q", action="store_true", help="Reduce output")
    args = parser.parse_args()

    if not args.skip_install:
        ensure_duckdb()
    else:
        try:
            import duckdb
        except ImportError:
            print("--skip-install but duckdb not found. Run: pip install duckdb", file=sys.stderr)
            sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)
    for sf in args.scale_factors:
        if sf <= 0:
            continue
        name_sf = int(sf) if sf == int(sf) else sf
        db_path = os.path.join(args.output_dir, f"tpch_sf{name_sf}.duckdb")
        generate_one_db(sf, db_path, verbose=not args.quiet)

    first_sf = next((s for s in args.scale_factors if s > 0), None)
    if first_sf is None:
        print("No databases generated (no valid scale factors).")
        return
    name_sf = int(first_sf) if first_sf == int(first_sf) else first_sf
    sample_db = os.path.join(args.output_dir, f"tpch_sf{name_sf}.duckdb")

    if not args.quiet:
        verify_schema(sample_db)
    if not args.no_run_query and os.path.isfile(sample_db):
        run_tpch_q1_sample(sample_db)

    print("Done. Generated standard TPC-H databases ready for Q1-Q22.")


if __name__ == "__main__":
    main()
