#!/usr/bin/env bash
# =============================================================================
# Generate all benchmark data for TPC-H, H2O, and ClickBench.
#
# Usage:
#   ./generate_all.sh [DATA_DIR]
#
# DATA_DIR defaults to the Maximus tests/ directory.
# For Sirius, DuckDB databases are generated alongside the CSV data.
# =============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAXIMUS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="${1:-$MAXIMUS_DIR/tests}"

echo "=============================================="
echo "  Benchmark Data Generation"
echo "  Output: $DATA_DIR"
echo "=============================================="

# Ensure duckdb is available
python3 -c "import duckdb" 2>/dev/null || {
    echo "Installing duckdb..."
    pip install duckdb
}

# ── TPC-H ──────────────────────────────────────────────────────────────────
echo ""
echo "=== TPC-H Data Generation ==="

# DuckDB databases (for Sirius)
TPCH_DB_DIR="$DATA_DIR/tpch_duckdb"
mkdir -p "$TPCH_DB_DIR"
echo "Generating TPC-H DuckDB databases in $TPCH_DB_DIR..."
python3 "$SCRIPT_DIR/generate_tpch.py" -o "$TPCH_DB_DIR" -sf 1 2 10 20 --no-run-query

# CSV data (for Maximus) - generated from DuckDB using dbgen + COPY
TPCH_CSV_DIR="$DATA_DIR/tpch"
mkdir -p "$TPCH_CSV_DIR"
echo "Generating TPC-H CSV data in $TPCH_CSV_DIR..."
for SF in 1 2 10 20; do
    CSV_SF_DIR="$TPCH_CSV_DIR/csv-$SF"
    if [ -d "$CSV_SF_DIR" ] && [ "$(ls "$CSV_SF_DIR"/*.csv 2>/dev/null | wc -l)" -gt 0 ]; then
        echo "  SF=$SF: CSV already exists, skipping"
        continue
    fi
    mkdir -p "$CSV_SF_DIR"
    DB="$TPCH_DB_DIR/tpch_sf${SF}.duckdb"
    if [ -f "$DB" ]; then
        echo "  SF=$SF: Exporting from DuckDB to CSV..."
        python3 -c "
import duckdb
con = duckdb.connect('$DB', read_only=True)
tables = [r[0] for r in con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall()]
for t in tables:
    con.execute(f\"COPY {t} TO '$CSV_SF_DIR/{t}.csv' (HEADER, DELIMITER ',')\")
    print(f'    Exported {t}.csv')
con.close()
"
    else
        echo "  SF=$SF: DuckDB not found, skipping CSV export"
    fi
done

# ── H2O ────────────────────────────────────────────────────────────────────
echo ""
echo "=== H2O Data Generation ==="

H2O_DIR="$DATA_DIR/h2o"
H2O_DB_DIR="$DATA_DIR/h2o_duckdb"
mkdir -p "$H2O_DIR" "$H2O_DB_DIR"
echo "Generating H2O data..."
python3 "$SCRIPT_DIR/generate_h2o.py" --output-dir "$H2O_DIR" --format csv 1gb 2gb 3gb 4gb
python3 "$SCRIPT_DIR/generate_h2o.py" --output-dir "$H2O_DB_DIR" --format duckdb 1gb 2gb 3gb 4gb

# ── ClickBench ─────────────────────────────────────────────────────────────
echo ""
echo "=== ClickBench Data Generation ==="

CB_DIR="$DATA_DIR/clickbench"
CB_DB_DIR="$DATA_DIR/click_duckdb"
mkdir -p "$CB_DIR" "$CB_DB_DIR"

# Check for parquet file
PARQUET_PATH="$DATA_DIR/clickbench.parquet"
if [ ! -f "$PARQUET_PATH" ]; then
    echo "ClickBench parquet not found. Downloading (~14GB)..."
    echo "  This may take a while depending on network speed."
fi

echo "Generating ClickBench CSV data..."
python3 "$SCRIPT_DIR/generate_clickbench.py" --output-dir "$CB_DIR" --format csv --scales 1 2 10 20 --parquet-path "$PARQUET_PATH"
echo "Generating ClickBench DuckDB databases..."
python3 "$SCRIPT_DIR/generate_clickbench.py" --output-dir "$CB_DB_DIR" --format duckdb --scales 10 20 50 100 --parquet-path "$PARQUET_PATH"

# ── Summary ────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
echo "  Data Generation Complete"
echo "=============================================="
echo ""
echo "TPC-H:"
echo "  DuckDB: $TPCH_DB_DIR/tpch_sf{1,2,10,20}.duckdb"
echo "  CSV:    $TPCH_CSV_DIR/csv-{1,2,10,20}/"
echo ""
echo "H2O:"
echo "  DuckDB: $H2O_DB_DIR/h2o_{1gb,2gb,3gb,4gb}.duckdb"
echo "  CSV:    $H2O_DIR/csv-{1gb,2gb,3gb,4gb}/groupby.csv"
echo ""
echo "ClickBench:"
echo "  DuckDB: $CB_DB_DIR/clickbench_{10,20,50,100}.duckdb"
echo "  CSV:    $CB_DIR/csv-{1,2,10,20}/t.csv"
