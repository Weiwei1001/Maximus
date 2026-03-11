#!/bin/bash
# Master script to re-run ALL benchmarks for Maximus and Sirius.
# Covers Category A (GPU-data), B (CPU-data), and C (freq sweep).
# Includes both standard benchmarks and microbenchmarks.
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

echo "========================================================================"
echo "  BENCHMARK SUITE ($MODE MODE)"
echo "  Started: $(date)"
echo "  Results: $RESULTS_DIR"
echo "  Logs:    $LOG_DIR"
echo "========================================================================"

cd "$SCRIPT_DIR"

# All benchmarks including microbench
ALL_BENCH="tpch h2o clickbench microbench_tpch microbench_h2o microbench_clickbench"
# Only tpch/h2o/clickbench for sirius (no microbench support)
SIRIUS_BENCH="tpch h2o clickbench"

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

# Check if sirius duckdb binary exists
SIRIUS_DUCKDB="$MAXIMUS_DIR/sirius/build/release/duckdb"
has_sirius() {
    [ -x "$SIRIUS_DUCKDB" ]
}

# Check if maxbench is built
MAXBENCH_BIN="$MAXIMUS_DIR/build/benchmarks/maxbench"
if [ ! -x "$MAXBENCH_BIN" ]; then
    echo "ERROR: maxbench binary not found at $MAXBENCH_BIN"
    echo "       Run: ninja -C build -j\$(nproc)"
    exit 1
fi

# ══════════════════════════════════════════════════════════════════════════
#  Category A: Data on GPU (-s gpu) — timing + metrics
# ══════════════════════════════════════════════════════════════════════════

echo ""
echo "======== CATEGORY A: Data on GPU ========"

# A1: Maximus timing (all benchmarks + microbench)
run_step "A1_maximus_timing" \
    python3 run_maximus_benchmark.py $TEST_FLAG --n-reps 3 --results-dir "$RESULTS_DIR" \
    $ALL_BENCH

# A2: Sirius timing
if has_sirius; then
    run_step "A2_sirius_timing" \
        python3 run_sirius_benchmark.py $TEST_FLAG --results-dir "$RESULTS_DIR" \
        $SIRIUS_BENCH
else
    echo "  [SKIP] A2: Sirius not built"
fi

# A3: Maximus metrics (all benchmarks + microbench)
run_step "A3_maximus_metrics" \
    python3 run_maximus_metrics.py $TEST_FLAG --target-time 10 --results-dir "$RESULTS_DIR" \
    $ALL_BENCH

# A4: Sirius metrics
if has_sirius; then
    run_step "A4_sirius_metrics" \
        python3 run_sirius_metrics.py $TEST_FLAG --target-time 60 --results-dir "$RESULTS_DIR" \
        $SIRIUS_BENCH
else
    echo "  [SKIP] A4: Sirius metrics: binary not found"
fi

# ══════════════════════════════════════════════════════════════════════════
#  Category B: Data on CPU (-s cpu) — timing only
# ══════════════════════════════════════════════════════════════════════════

echo ""
echo "======== CATEGORY B: Data on CPU ========"

# B1: Maximus CPU-data timing (all benchmarks + microbench)
run_step "B1_maximus_cpu_data" \
    python3 run_maximus_cpu_data.py $TEST_FLAG --timing-only --results-dir "$RESULTS_DIR" \
    $ALL_BENCH

# B2: Sirius CPU-data timing
if has_sirius; then
    run_step "B2_sirius_cpu_data" \
        python3 run_sirius_cpu_data.py $TEST_FLAG --n-reps 50 --results-dir "$RESULTS_DIR" \
        $SIRIUS_BENCH
else
    echo "  [SKIP] B2: Sirius CPU-data: binary not found"
fi

# ══════════════════════════════════════════════════════════════════════════
#  Category C: Frequency sweep (8x8 CPU x GPU grid)
#  Only for tpch SF=1,10 and h2o SF=1gb,4gb
# ══════════════════════════════════════════════════════════════════════════

echo ""
echo "======== CATEGORY C: Frequency Sweep ========"

# C1: GPU power limit sweep (8 levels)
run_step "C1_energy_sweep" \
    python3 run_energy_sweep.py $TEST_FLAG \
    --benchmarks tpch h2o \
    --results-dir "$RESULTS_DIR/energy_sweep" \
    --resume

# C2: Full frequency sweep (CPU x GPU grid)
run_step "C2_freq_sweep" \
    python3 run_freq_sweep.py $TEST_FLAG \
    --benchmarks tpch h2o \
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
