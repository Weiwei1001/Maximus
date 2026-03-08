#!/usr/bin/env python3
"""
Generate H2O groupby benchmark data using DuckDB.

Creates synthetic data matching the H2O benchmark schema:
  id1-id3 (VARCHAR), id4-id6 (INTEGER), v1-v2 (INTEGER), v3 (DOUBLE)

Usage:
    python generate_h2o_data.py --output-dir /path/to/output --scales 1 2 3 4
"""
import argparse
import time
from pathlib import Path

import duckdb

# Target GB -> row count mapping (~15.5 bytes/row in CSV)
SCALE_CONFIGS = {
    1: {"target_gb": 1, "n_rows": 65_000_000, "k": 300},
    2: {"target_gb": 2, "n_rows": 130_000_000, "k": 400},
    3: {"target_gb": 3, "n_rows": 190_000_000, "k": 400},
    4: {"target_gb": 4, "n_rows": 250_000_000, "k": 500},
}

CHUNK_SIZE = 50_000_000  # Generate in 50M row chunks for memory efficiency


def generate_h2o(output_dir: Path, scale: int):
    """Generate H2O groupby data at the given scale."""
    sf_dir = output_dir / f"sf{scale}"
    sf_dir.mkdir(parents=True, exist_ok=True)
    csv_path = sf_dir / "groupby.csv"

    if csv_path.exists():
        print(f"  Scale {scale}: CSV already exists, skipping")
        return

    if scale not in SCALE_CONFIGS:
        print(f"  Scale {scale}: Not in predefined configs, using linear estimation")
        n_rows = int(scale * 65_000_000)
        k = max(300, scale * 100)
    else:
        cfg = SCALE_CONFIGS[scale]
        n_rows = cfg["n_rows"]
        k = cfg["k"]

    print(f"  Scale {scale}: Generating {n_rows:,} rows (target ~{scale}GB)...")
    t0 = time.time()

    con = duckdb.connect(":memory:")
    con.execute("SET threads TO 8;")

    # Create table
    con.execute("""
        CREATE TABLE groupby (
            id1 VARCHAR, id2 VARCHAR, id3 VARCHAR,
            id4 INTEGER, id5 INTEGER, id6 INTEGER,
            v1 INTEGER, v2 INTEGER, v3 DOUBLE
        )
    """)

    # Insert in chunks
    remaining = n_rows
    chunk_id = 0
    while remaining > 0:
        chunk = min(CHUNK_SIZE, remaining)
        offset = chunk_id * CHUNK_SIZE
        con.execute(f"""
            INSERT INTO groupby
            SELECT
                'id' || CAST(abs(hash(i + {offset})) % {k} + 1 AS VARCHAR) AS id1,
                'id' || CAST(abs(hash(CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT))) % {k} + 1 AS VARCHAR) AS id2,
                'id' || CAST(abs(hash(CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT)*2)) % ({n_rows} // {k}) + 1 AS VARCHAR) AS id3,
                CAST(abs(hash(CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT)*3)) % {k} + 1 AS INTEGER) AS id4,
                CAST(abs(hash(CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT)*4)) % {k} + 1 AS INTEGER) AS id5,
                CAST(abs(hash(CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT)*5)) % ({n_rows} // {k}) + 1 AS INTEGER) AS id6,
                CAST(abs(hash(CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT)*6)) % 100 + 1 AS INTEGER) AS v1,
                CAST(abs(hash(CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT)*7)) % 100 + 1 AS INTEGER) AS v2,
                abs(hash(CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT)*8)) % 100000 / 100.0 AS v3
            FROM generate_series(1, {chunk}) AS t(i)
        """)
        remaining -= chunk
        chunk_id += 1

    # Export to CSV
    con.execute(f"COPY groupby TO '{csv_path}' (HEADER, DELIMITER ',')")
    con.close()

    size_gb = csv_path.stat().st_size / (1024 ** 3)
    elapsed = time.time() - t0
    print(f"  Scale {scale}: Done ({size_gb:.2f} GB, {elapsed:.1f}s)")


def main():
    parser = argparse.ArgumentParser(description="Generate H2O groupby benchmark data as CSV")
    parser.add_argument("--output-dir", type=str, required=True, help="Output directory for CSV files")
    parser.add_argument("--scales", type=int, nargs="+", default=[1, 2, 3, 4],
                        help="Scale factors in GB (default: 1 2 3 4)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating H2O data: scales={args.scales}")
    print(f"Output: {output_dir}")

    for scale in args.scales:
        generate_h2o(output_dir, scale)

    print("All H2O data generated.")


if __name__ == "__main__":
    main()
