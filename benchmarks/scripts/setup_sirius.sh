#!/bin/bash
# ==============================================================================
# Sirius (DuckDB GPU Extension) - Download, Build, and Benchmark Setup
# ==============================================================================
#
# This script sets up Sirius for benchmarking against Maximus.
# It handles: cloning, dependency installation, building, and benchmark data.
#
# Prerequisites:
#   - CUDA >= 12.x installed and nvcc in PATH
#   - CMake >= 3.30.4  (check: cmake --version)
#   - ninja-build
#   - conda or pip (for libcudf)
#   - Python 3 with duckdb package (pip install duckdb)
#
# Usage:
#   bash setup_sirius.sh [--install-dir /path/to/install]
#
# After running, you will have:
#   <install-dir>/sirius/build/release/duckdb   - Sirius DuckDB binary
#   <install-dir>/tpch_duckdb/                  - TPC-H databases
#   <install-dir>/h2o_duckdb/                   - H2O databases
#   <install-dir>/click_duckdb/                 - ClickBench databases
#   <install-dir>/tpch_sql/                     - TPC-H GPU query files
#   <install-dir>/h2o_sql/                      - H2O GPU query files
#   <install-dir>/click_sql/                    - ClickBench GPU query files
# ==============================================================================

set -e

# ── Parse arguments ──────────────────────────────────────────────────────────
INSTALL_DIR="${1:-/workspace}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================"
echo "  Sirius Setup Script"
echo "  Install directory: ${INSTALL_DIR}"
echo "============================================"

# ── Step 0: Check prerequisites ──────────────────────────────────────────────
echo ""
echo "[0/6] Checking prerequisites..."

check_cmd() {
    if ! command -v "$1" &> /dev/null; then
        echo "  ERROR: $1 not found. Please install it first."
        echo "  $2"
        exit 1
    fi
    echo "  OK: $1 ($(command -v $1))"
}

check_cmd cmake "Install: apt-get install cmake  OR  pip install cmake"
check_cmd ninja "Install: apt-get install ninja-build"
check_cmd nvcc  "Install CUDA toolkit from https://developer.nvidia.com/cuda-downloads"
check_cmd git   "Install: apt-get install git"
check_cmd python3 "Install: apt-get install python3"

# Check CMake version >= 3.30.4
CMAKE_VER=$(cmake --version | head -1 | grep -oP '[\d.]+')
CMAKE_MAJOR=$(echo "$CMAKE_VER" | cut -d. -f1)
CMAKE_MINOR=$(echo "$CMAKE_VER" | cut -d. -f2)
if [ "$CMAKE_MAJOR" -lt 3 ] || ([ "$CMAKE_MAJOR" -eq 3 ] && [ "$CMAKE_MINOR" -lt 30 ]); then
    echo "  ERROR: CMake >= 3.30.4 required, found $CMAKE_VER"
    echo "  Install latest: pip install cmake"
    exit 1
fi
echo "  OK: CMake $CMAKE_VER"

# ── Step 1: Clone Sirius ─────────────────────────────────────────────────────
echo ""
echo "[1/6] Cloning Sirius..."

cd "$INSTALL_DIR"
if [ -d "sirius" ]; then
    echo "  sirius/ already exists, pulling latest..."
    cd sirius
    git pull --recurse-submodules || true
    git submodule update --init --recursive
else
    git clone --recurse-submodules https://github.com/sirius-db/sirius.git
    cd sirius
fi

SIRIUS_DIR="$INSTALL_DIR/sirius"
echo "  Sirius directory: $SIRIUS_DIR"

# ── Step 2: Install dependencies ─────────────────────────────────────────────
echo ""
echo "[2/6] Installing dependencies..."

# System packages
echo "  Installing system packages..."
apt-get update -qq && apt-get install -y -qq \
    libssl-dev libnuma-dev libconfig++-dev 2>/dev/null || \
    echo "  (apt-get not available or failed - assuming deps are installed)"

# libcudf - try conda first, then pip
echo "  Installing libcudf..."
if command -v conda &> /dev/null; then
    echo "  Using conda..."
    conda install -y -c rapidsai -c conda-forge -c nvidia \
        "rapidsai::libcudf>=26.04" 2>/dev/null || \
        echo "  conda install failed, trying pip..."
fi

if ! python3 -c "import libcudf" 2>/dev/null; then
    echo "  Using pip..."
    pip install --quiet libcudf-cu12 cudf-cu12 2>/dev/null || \
        echo "  pip install failed. Please install libcudf manually."
fi

# abseil - try apt, then conda
if ! dpkg -l | grep -q libabsl-dev 2>/dev/null; then
    apt-get install -y -qq libabsl-dev 2>/dev/null || \
    conda install -y -c conda-forge "libabseil>=20260107.0" 2>/dev/null || \
        echo "  NOTE: abseil not found. Sirius build may need it."
fi

# ── Step 3: Build Sirius ─────────────────────────────────────────────────────
echo ""
echo "[3/6] Building Sirius..."

cd "$SIRIUS_DIR"

# Source setup script if it exists
if [ -f "setup_sirius.sh" ]; then
    source setup_sirius.sh 2>/dev/null || true
fi

# Set up LDFLAGS for libcudf if using conda
if [ -n "$CONDA_PREFIX" ]; then
    export LDFLAGS="-Wl,-rpath,$CONDA_PREFIX/lib -L$CONDA_PREFIX/lib $LDFLAGS"
    export LIBCUDF_ENV_PREFIX="$CONDA_PREFIX"
fi

# Build
echo "  Building with $(nproc) cores..."
CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make 2>&1 | tail -5

if [ -f "build/release/duckdb" ]; then
    echo "  OK: build/release/duckdb built successfully"
else
    echo "  ERROR: Build failed. Check output above."
    exit 1
fi

# ── Step 4: Generate benchmark databases ─────────────────────────────────────
echo ""
echo "[4/6] Generating benchmark databases..."

cd "$INSTALL_DIR"

# TPC-H databases (import from Maximus CSV to ensure identical data)
echo "  Generating TPC-H databases from Maximus CSV data..."
mkdir -p tpch_duckdb
TPCH_TABLES="lineitem orders customer part partsupp supplier nation region"
for sf in 1 2 10 20; do
    DB="tpch_duckdb/tpch_sf${sf}.duckdb"
    if [ -f "$DB" ]; then
        echo "    $DB already exists, skipping"
        continue
    fi
    CSV_DIR="$INSTALL_DIR/Maximus/tests/tpch/csv-${sf}"
    if [ -d "$CSV_DIR" ]; then
        echo "    Creating $DB from CSV (SF=${sf})..."
        python3 -c "
import duckdb, os
conn = duckdb.connect('$DB')
csv_dir = '$CSV_DIR'
for table in '$TPCH_TABLES'.split():
    csv_path = os.path.join(csv_dir, table + '.csv')
    if os.path.exists(csv_path):
        conn.execute(f\"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_path}')\")
        rows = conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
        print(f'      {table}: {rows} rows')
conn.close()
print('    Done: $DB')
"
    else
        echo "    CSV data not found at $CSV_DIR, skipping"
    fi
done

# H2O databases
echo "  Generating H2O databases..."
mkdir -p h2o_duckdb
for sf in 1gb 2gb 3gb 4gb; do
    DB="h2o_duckdb/h2o_${sf}.duckdb"
    if [ -f "$DB" ]; then
        echo "    $DB already exists, skipping"
        continue
    fi
    CSV_DIR="$INSTALL_DIR/Maximus/tests/h2o/csv-${sf}"
    if [ -d "$CSV_DIR" ]; then
        echo "    Creating $DB from CSV..."
        python3 -c "
import duckdb
conn = duckdb.connect('$DB')
conn.execute(\"CREATE TABLE groupby AS SELECT * FROM read_csv_auto('${CSV_DIR}/groupby.csv')\")
conn.close()
print('    Done: $DB')
"
    else
        echo "    CSV data not found at $CSV_DIR, skipping"
    fi
done

# ClickBench databases
echo "  Generating ClickBench databases..."
mkdir -p click_duckdb
for sf in 1 2; do
    DB="click_duckdb/clickbench_${sf}.duckdb"
    if [ -f "$DB" ]; then
        echo "    $DB already exists, skipping"
        continue
    fi
    CSV_DIR="$INSTALL_DIR/Maximus/tests/clickbench/csv-${sf}"
    if [ -d "$CSV_DIR" ]; then
        echo "    Creating $DB from CSV..."
        python3 -c "
import duckdb
conn = duckdb.connect('$DB')
conn.execute(\"CREATE TABLE t AS SELECT * FROM read_csv_auto('${CSV_DIR}/t.csv')\")
conn.close()
print('    Done: $DB')
"
    else
        echo "    CSV data not found at $CSV_DIR, skipping"
    fi
done

# ── Step 5: Generate GPU query SQL files ─────────────────────────────────────
echo ""
echo "[5/6] Generating Sirius GPU query SQL files..."

cd "$INSTALL_DIR"
python3 "${SCRIPT_DIR}/generate_sirius_sql.py" --output-dir "$INSTALL_DIR"

# ── Step 6: Verify ───────────────────────────────────────────────────────────
echo ""
echo "[6/6] Verifying installation..."

DUCKDB="$SIRIUS_DIR/build/release/duckdb"
echo "  Sirius binary: $DUCKDB"
echo "  Binary size: $(du -h "$DUCKDB" | cut -f1)"

# Quick smoke test
if [ -f "$INSTALL_DIR/tpch_duckdb/tpch_sf1.duckdb" ]; then
    echo "  Running smoke test (TPC-H Q1 on SF=1)..."
    echo '.timer on
call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT count(*) FROM lineitem;");
' | timeout 30 "$DUCKDB" "$INSTALL_DIR/tpch_duckdb/tpch_sf1.duckdb" 2>&1 | tail -3
    echo "  Smoke test done."
fi

echo ""
echo "============================================"
echo "  Sirius setup complete!"
echo ""
echo "  Binary:     $DUCKDB"
echo "  TPC-H DBs:  $INSTALL_DIR/tpch_duckdb/"
echo "  H2O DBs:    $INSTALL_DIR/h2o_duckdb/"
echo "  Click DBs:  $INSTALL_DIR/click_duckdb/"
echo "  SQL files:  $INSTALL_DIR/{tpch,h2o,click}_sql/"
echo ""
echo "  Run benchmarks:"
echo "    python benchmarks/scripts/run_sirius_benchmark.py"
echo "============================================"
