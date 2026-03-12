#!/bin/bash
# Master script to re-run ALL benchmarks for Maximus and Sirius.
# Covers Category A (GPU-data), B (CPU-data), and C (energy sweep).
# Includes both standard benchmarks and microbenchmarks.
#
# On a fresh machine this script will:
#   1. Generate CSV data (TPC-H, H2O, ClickBench) if missing
#   2. Auto-build Sirius DuckDB if not built
#   3. Generate Sirius DuckDB databases + SQL query files if missing
#   4. Run all three categories of experiments
#
# Categories:
#   A – Data on GPU: timing + power/energy metrics (standard + microbench)
#   B – Data on CPU: timing + power/energy metrics
#   C – Energy sweep: 3 GPU power limits × 5 SM clock frequencies
#
# Usage:
#   bash run_all_benchmarks.sh          # Full run
#   bash run_all_benchmarks.sh --test   # Quick smoke test (3 queries per bench)
#
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAXIMUS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$MAXIMUS_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="$RESULTS_DIR/logs_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

# ── Parse arguments ────────────────────────────────────────────────────────
TEST_FLAG=""
for arg in "$@"; do
    case "$arg" in
        --test) TEST_FLAG="--test" ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

MODE="FULL"
[ -n "$TEST_FLAG" ] && MODE="TEST"

DATA_DIR="$MAXIMUS_DIR/benchmarks/data"

echo "========================================================================"
echo "  BENCHMARK SUITE ($MODE MODE)"
echo "  Started: $(date)"
echo "  Results: $RESULTS_DIR"
echo "  Logs:    $LOG_DIR"
echo "========================================================================"

# ══════════════════════════════════════════════════════════════════════════
#  Step 0a: Generate missing Maximus CSV data
# ══════════════════════════════════════════════════════════════════════════
echo ""
echo "======== STEP 0a: Generate missing CSV data ========"

# TPC-H: CSV for Maximus (tests/tpch/csv-{sf})
TPCH_SFS="1 5 10 20"
for sf in $TPCH_SFS; do
    if [ ! -d "$MAXIMUS_DIR/tests/tpch/csv-$sf" ]; then
        echo "  [DATAGEN] TPC-H SF=$sf CSV..."
        (cd "$MAXIMUS_DIR/tests/tpch" && python3 generate_data.py --sf "$sf") 2>&1 | tail -3
    fi
done

# H2O: CSV for Maximus (tests/h2o/csv-{sf})
H2O_SFS="1gb 2gb 3gb 4gb"
H2O_MISSING=""
for sf in $H2O_SFS; do
    if [ ! -d "$MAXIMUS_DIR/tests/h2o/csv-$sf" ]; then
        H2O_MISSING="$H2O_MISSING $sf"
    fi
done
if [ -n "$H2O_MISSING" ]; then
    echo "  [DATAGEN] H2O:$H2O_MISSING ..."
    mkdir -p "$MAXIMUS_DIR/tests/h2o"
    python3 "$DATA_DIR/generate_h2o.py" --format csv \
        -o "$MAXIMUS_DIR/tests/h2o" $H2O_MISSING 2>&1 | tail -5
fi

# ClickBench: CSV for Maximus (tests/clickbench/csv-{sf})
# Requires downloading ~14GB parquet, so skip if no internet or in test mode
CB_SFS="1 5 10 20"
CB_MISSING=""
for sf in $CB_SFS; do
    if [ ! -d "$MAXIMUS_DIR/tests/clickbench/csv-$sf" ]; then
        CB_MISSING="$CB_MISSING $sf"
    fi
done
if [ -n "$CB_MISSING" ]; then
    mkdir -p "$MAXIMUS_DIR/tests/clickbench"
    PARQUET_PATH="$MAXIMUS_DIR/tests/clickbench/clickbench.parquet"
    if [ ! -f "$PARQUET_PATH" ]; then
        echo "  [DATAGEN] Downloading ClickBench parquet (~14GB)..."
        wget -q --show-progress -O "$PARQUET_PATH" \
            "https://datasets.clickhouse.com/hits_compatible/hits.parquet" 2>&1 || true
    fi
    if [ -f "$PARQUET_PATH" ]; then
        echo "  [DATAGEN] ClickBench:$CB_MISSING ..."
        python3 "$DATA_DIR/generate_clickbench.py" --format csv \
            -o "$MAXIMUS_DIR/tests/clickbench" --parquet-path "$PARQUET_PATH" \
            --scales $CB_MISSING 2>&1 | tail -5
    else
        echo "  [WARN] ClickBench: parquet download failed, skipping"
    fi
fi

echo "  [DATAGEN] CSV data done."

cd "$SCRIPT_DIR"

# ══════════════════════════════════════════════════════════════════════════
#  Step 0b: Build binaries (Maximus + Sirius)
# ══════════════════════════════════════════════════════════════════════════
echo ""
echo "======== STEP 0b: Check/build binaries ========"

# Check if maxbench is built
MAXBENCH_BIN="$MAXIMUS_DIR/build/benchmarks/maxbench"
if [ ! -x "$MAXBENCH_BIN" ]; then
    echo "ERROR: maxbench binary not found at $MAXBENCH_BIN"
    echo "       Run: ninja -C build -j\$(nproc)"
    exit 1
fi
echo "  [OK] Maximus maxbench: $MAXBENCH_BIN"

# Check if sirius duckdb binary exists; auto-build if missing
SIRIUS_DUCKDB="$MAXIMUS_DIR/sirius/build/release/duckdb"
if [ ! -x "$SIRIUS_DUCKDB" ] && [ -f "$MAXIMUS_DIR/sirius_patches/build_sirius.sh" ]; then
    echo "  [AUTO] Sirius not built — running build_sirius.sh..."
    bash "$MAXIMUS_DIR/sirius_patches/build_sirius.sh" 2>&1 | tail -20
fi
has_sirius() {
    [ -x "$SIRIUS_DUCKDB" ]
}
if has_sirius; then
    echo "  [OK] Sirius DuckDB: $SIRIUS_DUCKDB"
else
    echo "  [WARN] Sirius DuckDB not available — Sirius steps will be skipped"
fi

# ══════════════════════════════════════════════════════════════════════════
#  Step 0c: Generate Sirius DuckDB databases + SQL query files
# ══════════════════════════════════════════════════════════════════════════
if has_sirius; then
    echo ""
    echo "======== STEP 0c: Generate Sirius data ========"

    # Generate Sirius SQL query files (standard + microbench, idempotent)
    if [ ! -d "$MAXIMUS_DIR/tests/tpch_sql/queries/1" ] || \
       [ ! -d "$MAXIMUS_DIR/tests/h2o_sql/queries/1" ] || \
       [ ! -d "$MAXIMUS_DIR/tests/microbench_tpch_sql/queries/1" ]; then
        echo "  [DATAGEN] Generating Sirius SQL query files..."
        python3 "$SCRIPT_DIR/generate_sirius_sql.py" \
            --output-dir "$MAXIMUS_DIR/tests" 2>&1 | tail -5
    else
        echo "  [OK] Sirius SQL query files already exist"
    fi

    # Generate DuckDB databases from CSV (TPC-H, H2O)
    TPCH_TABLES="lineitem orders customer part partsupp supplier nation region"
    mkdir -p "$MAXIMUS_DIR/tests/tpch_duckdb" "$MAXIMUS_DIR/tests/h2o_duckdb"

    for sf in $TPCH_SFS; do
        DB="$MAXIMUS_DIR/tests/tpch_duckdb/tpch_sf${sf}.duckdb"
        CSV_DIR="$MAXIMUS_DIR/tests/tpch/csv-${sf}"
        if [ -f "$DB" ]; then
            continue
        fi
        if [ -d "$CSV_DIR" ]; then
            echo "  [DATAGEN] Creating tpch_sf${sf}.duckdb..."
            python3 -c "
import duckdb, os
conn = duckdb.connect('$DB')
for table in '$TPCH_TABLES'.split():
    csv_path = os.path.join('$CSV_DIR', table + '.csv')
    if os.path.exists(csv_path):
        conn.execute(f\"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_path}')\")
conn.close()
" 2>&1 | tail -3
        fi
    done

    for sf in $H2O_SFS; do
        DB="$MAXIMUS_DIR/tests/h2o_duckdb/h2o_${sf}.duckdb"
        CSV_DIR="$MAXIMUS_DIR/tests/h2o/csv-${sf}"
        if [ -f "$DB" ]; then
            continue
        fi
        if [ -d "$CSV_DIR" ]; then
            echo "  [DATAGEN] Creating h2o_${sf}.duckdb..."
            python3 -c "
import duckdb
conn = duckdb.connect('$DB')
conn.execute(\"CREATE TABLE groupby AS SELECT * FROM read_csv_auto('${CSV_DIR}/groupby.csv')\")
conn.close()
" 2>&1 | tail -3
        fi
    done

    echo "  [DATAGEN] Sirius data done."
fi

# ── GPU memory check (informational) ─────────────────────────────────────
echo ""
echo "======== GPU Memory Check ========"
GPU_VRAM_MB=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits -i \
    $(python3 -c "from hw_detect import detect_gpu; print(detect_gpu()['index'])") 2>/dev/null | head -1)
echo "  GPU VRAM: ${GPU_VRAM_MB:-unknown} MiB"
echo "  Buffer sizing and benchmark configs are auto-adjusted by hw_detect.py"

# All benchmarks: standard + microbench (both Maximus and Sirius)
ALL_BENCH="tpch h2o clickbench microbench_tpch microbench_h2o microbench_clickbench"

# Helper function
run_step() {
    local step_name="$1"
    shift
    echo ""
    echo "================================================================"
    echo "  STEP: $step_name"
    echo "  Command: $@"
    echo "  Time: $(date)"
    echo "================================================================"
    if "$@" 2>&1 | tee "$LOG_DIR/${step_name}.log"; then
        echo "  DONE: $step_name ($(date))"
    else
        echo "  WARN: $step_name exited non-zero ($(date))"
    fi
}

# ══════════════════════════════════════════════════════════════════════════
#  Category A: Data on GPU (-s gpu) — timing + power/energy metrics
#  Standard SQL benchmarks + microbenchmarks (GPU memory auto-checked)
# ══════════════════════════════════════════════════════════════════════════

echo ""
echo "======== CATEGORY A: Data on GPU (timing + metrics) ========"

# A1: Maximus timing (standard + microbench)
run_step "A1_maximus_timing" \
    python3 run_maximus_benchmark.py $TEST_FLAG --n-reps 3 --results-dir "$RESULTS_DIR" \
    $ALL_BENCH

# A2: Sirius timing (standard SQL only, no microbench)
if has_sirius; then
    run_step "A2_sirius_timing" \
        python3 run_sirius_benchmark.py $TEST_FLAG --results-dir "$RESULTS_DIR" \
        $ALL_BENCH
else
    echo "  [SKIP] A2: Sirius not built"
fi

# A3: Maximus metrics (standard + microbench)
run_step "A3_maximus_metrics" \
    python3 run_maximus_metrics.py $TEST_FLAG --target-time 10 --results-dir "$RESULTS_DIR" \
    $ALL_BENCH

# A4: Sirius metrics (standard SQL only)
if has_sirius; then
    run_step "A4_sirius_metrics" \
        python3 run_sirius_metrics.py $TEST_FLAG --target-time 60 --results-dir "$RESULTS_DIR" \
        $ALL_BENCH
else
    echo "  [SKIP] A4: Sirius metrics: binary not found"
fi

# ══════════════════════════════════════════════════════════════════════════
#  Category B: Data on CPU (-s cpu) — timing + power/energy metrics
# ══════════════════════════════════════════════════════════════════════════

echo ""
echo "======== CATEGORY B: Data on CPU (timing + metrics) ========"

# B1: Maximus CPU-data timing (standard + microbench)
run_step "B1_maximus_cpu_timing" \
    python3 run_maximus_cpu_data.py $TEST_FLAG --timing-only --results-dir "$RESULTS_DIR" \
    $ALL_BENCH

# B2: Maximus CPU-data metrics (standard + microbench)
run_step "B2_maximus_cpu_metrics" \
    python3 run_maximus_cpu_data.py $TEST_FLAG --target-time 10 --results-dir "$RESULTS_DIR" \
    $ALL_BENCH

# B3: Sirius CPU-data timing + metrics (measured together)
if has_sirius; then
    run_step "B3_sirius_cpu_data" \
        python3 run_sirius_cpu_data.py $TEST_FLAG --n-reps 10 --results-dir "$RESULTS_DIR" \
        $ALL_BENCH
else
    echo "  [SKIP] B3: Sirius CPU-data: binary not found"
fi

# ══════════════════════════════════════════════════════════════════════════
#  Category C: Energy sweep (3 GPU power limits × 5 SM clock frequencies)
#  Only for tpch and h2o benchmarks
# ══════════════════════════════════════════════════════════════════════════

echo ""
echo "======== CATEGORY C: Energy Sweep (3 PL × 5 freq) ========"

run_step "C1_energy_sweep" \
    python3 run_energy_sweep.py $TEST_FLAG \
    --benchmarks tpch h2o \
    --results-dir "$RESULTS_DIR/energy_sweep" \
    --resume

# ══════════════════════════════════════════════════════════════════════════
#  Energy Summary: aggregate Category A metrics into unified energy report
# ══════════════════════════════════════════════════════════════════════════

echo ""
echo "======== ENERGY SUMMARY ========"

run_step "energy_summary" \
    python3 compute_energy_summary.py --latest --results-dir "$RESULTS_DIR" \
    --output "$RESULTS_DIR/energy_summary.csv"

echo ""
echo "========================================================================"
echo "  ALL BENCHMARKS COMPLETE ($MODE MODE)"
echo "  Finished: $(date)"
echo "  Results:  $RESULTS_DIR"
echo "  Logs:     $LOG_DIR"
echo "========================================================================"
