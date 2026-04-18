#!/usr/bin/env python3
"""
H2O groupby benchmark data generation.
Generates DuckDB databases and CSV files for H2O groupby aggregation benchmarks.

Usage:
    python generate_h2o.py                          # Default: 1gb 2gb 4gb 8gb
    python generate_h2o.py 1gb 2gb                  # Specific sizes
    python generate_h2o.py --format csv             # CSV output (for Maximus)
    python generate_h2o.py --format duckdb          # DuckDB output (for Sirius)
    python generate_h2o.py --format both            # Both formats
    python generate_h2o.py --output-dir ./h2o_data
"""

import argparse
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

# Target sizes and estimated row counts
TARGETS = {
    "1gb":  35_000_000,
    "2gb":  70_000_000,
    "3gb":  105_000_000,
    "4gb":  140_000_000,
    "5gb":  175_000_000,
    "8gb":  280_000_000,
    "10gb": 350_000_000,
    "20gb": 700_000_000,
}


def generate_h2o_duckdb(out_path: Path, n_rows: int):
    """Generate H2O groupby DuckDB database."""
    import duckdb
    if out_path.exists():
        out_path.unlink()
    con = duckdb.connect(str(out_path))
    batch_size = 50_000_000
    n_batches = max(1, (n_rows + batch_size - 1) // batch_size)
    print(f"  Generating {n_rows:,} rows in {n_batches} batch(es)...")
    sql_body = """
        SELECT
            'id' || LPAD(CAST(1 + (random() * 99)::int AS VARCHAR), 3, '0') AS id1,
            'id' || LPAD(CAST(1 + (random() * 99)::int AS VARCHAR), 3, '0') AS id2,
            'id' || LPAD(CAST((random() * 9999999)::int AS VARCHAR), 10, '0') AS id3,
            CAST(1 + (random() * 99)::int AS INTEGER) AS id4,
            CAST(1 + (random() * 99)::int AS INTEGER) AS id5,
            CAST(1 + (random() * 99999)::int AS INTEGER) AS id6,
            CAST(1 + (random() * 4)::int AS INTEGER) AS v1,
            CAST(1 + (random() * 14)::int AS INTEGER) AS v2,
            CAST(random() * 100 AS DOUBLE) AS v3
        FROM generate_series(1, {rows})
    """
    for i in range(n_batches):
        rows_this = min(batch_size, n_rows - i * batch_size)
        if rows_this <= 0:
            break
        if i == 0:
            con.execute(f"CREATE TABLE groupby AS {sql_body.format(rows=rows_this)}")
        else:
            con.execute(f"INSERT INTO groupby {sql_body.format(rows=rows_this)}")
        print(f"    Batch {i+1}/{n_batches}: {rows_this:,} rows")
    actual = con.execute("SELECT count(*) FROM groupby").fetchone()[0]
    con.close()
    size_gb = out_path.stat().st_size / (1024 ** 3)
    return actual, size_gb


def generate_h2o_csv(out_dir: Path, target: str, n_rows: int):
    """Generate H2O groupby CSV file for Maximus."""
    import duckdb
    csv_dir = out_dir / f"csv-{target}"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / "groupby.csv"
    print(f"  Generating CSV: {csv_path}")
    con = duckdb.connect(":memory:")
    con.execute(f"""
        COPY (
            SELECT
                'id' || LPAD(CAST(1 + (random() * 99)::int AS VARCHAR), 3, '0') AS id1,
                'id' || LPAD(CAST(1 + (random() * 99)::int AS VARCHAR), 3, '0') AS id2,
                'id' || LPAD(CAST((random() * 9999999)::int AS VARCHAR), 10, '0') AS id3,
                CAST(1 + (random() * 99)::int AS INTEGER) AS id4,
                CAST(1 + (random() * 99)::int AS INTEGER) AS id5,
                CAST(1 + (random() * 99999)::int AS INTEGER) AS id6,
                CAST(1 + (random() * 4)::int AS INTEGER) AS v1,
                CAST(1 + (random() * 14)::int AS INTEGER) AS v2,
                CAST(random() * 100 AS DOUBLE) AS v3
            FROM generate_series(1, {n_rows})
        ) TO '{csv_path}' (HEADER, DELIMITER ',')
    """)
    con.close()
    size_gb = csv_path.stat().st_size / (1024 ** 3)
    return size_gb


def main():
    parser = argparse.ArgumentParser(description="H2O groupby data generation")
    parser.add_argument("targets", nargs="*", default=["1gb", "2gb", "4gb", "8gb"],
                        help=f"Target sizes (choices: {', '.join(TARGETS.keys())})")
    parser.add_argument("--output-dir", "-o", type=str, default=".",
                        help="Output directory")
    parser.add_argument("--format", choices=["csv", "duckdb", "both"], default="both",
                        help="Output format (default: both)")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        import duckdb
    except ImportError:
        print("duckdb not found. Run: pip install duckdb", file=sys.stderr)
        sys.exit(1)

    for target in args.targets:
        target = target.lower()
        if target not in TARGETS:
            print(f"Unknown target: {target}. Available: {', '.join(TARGETS.keys())}")
            continue
        n_rows = TARGETS[target]
        print(f"\n{'='*50}")
        print(f"  Generating H2O {target} ({n_rows:,} rows)")
        print(f"{'='*50}")
        t0 = time.time()

        if args.format in ("duckdb", "both"):
            db_path = out_dir / f"h2o_{target}.duckdb"
            actual, size_gb = generate_h2o_duckdb(db_path, n_rows)
            print(f"  DuckDB: {db_path.name} ({actual:,} rows, {size_gb:.2f} GB)")

        if args.format in ("csv", "both"):
            size_gb = generate_h2o_csv(out_dir, target, n_rows)
            print(f"  CSV: csv-{target}/groupby.csv ({size_gb:.2f} GB)")

        print(f"  Time: {time.time() - t0:.1f}s")

    print("\nDone.")


if __name__ == "__main__":
    main()
