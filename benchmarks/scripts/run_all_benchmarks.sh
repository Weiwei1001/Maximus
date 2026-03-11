#!/bin/bash
# Master script to re-run ALL benchmarks for Maximus and Sirius
# Both GPU-data and CPU-data modes, timing and metrics
#
# Usage:
#   bash run_all_benchmarks.sh          # Full run (3-6 hours)
#   bash run_all_benchmarks.sh --test   # Quick smoke test (~2 min)
#
# Estimated total runtime: 3-6 hours (full), ~2 min (test)
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAXIMUS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$MAXIMUS_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="$RESULTS_DIR/logs_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

# ── Parse arguments ────────────────────────────────────────────────────────
TEST_MODE=0
for arg in "$@"; do
    case "$arg" in
        --test) TEST_MODE=1 ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

if [ "$TEST_MODE" -eq 1 ]; then
    echo "========================================================================"
    echo "  BENCHMARK SMOKE TEST"
    echo "  Started: $(date)"
    echo "  Results: $RESULTS_DIR"
    echo "  Logs: $LOG_DIR"
    echo "========================================================================"
else
    echo "========================================================================"
    echo "  FULL BENCHMARK RE-RUN"
    echo "  Started: $(date)"
    echo "  Results: $RESULTS_DIR"
    echo "  Logs: $LOG_DIR"
    echo "========================================================================"
fi

cd "$SCRIPT_DIR"

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
    "$@" 2>&1 | tee "$LOG_DIR/${step_name}.log"
    echo "  DONE: $step_name ($(date))"
}

# Helper: check if sirius duckdb binary exists
SIRIUS_DUCKDB="$MAXIMUS_DIR/sirius/build/release/duckdb"
has_sirius() {
    [ -x "$SIRIUS_DUCKDB" ]
}

if [ "$TEST_MODE" -eq 1 ]; then
    # ── Test mode: quick smoke test with minimal data ──────────────────────
    # Run maxbench directly on available TPC-H data (csv-0.01 or csv)
    TPCH_DATA="$MAXIMUS_DIR/tests/tpch/csv-0.01"
    if [ ! -d "$TPCH_DATA" ]; then
        TPCH_DATA="$MAXIMUS_DIR/tests/tpch/csv"
    fi
    MAXBENCH_BIN="$MAXIMUS_DIR/build/benchmarks/maxbench"
    if [ ! -x "$MAXBENCH_BIN" ]; then
        echo "ERROR: maxbench binary not found at $MAXBENCH_BIN"
        exit 1
    fi

    # Test 1: Maximus GPU timing on TPC-H q1,q3,q6 (1 rep)
    run_step "maximus_gpu_test" \
        "$MAXBENCH_BIN" --benchmark tpch -q q1,q3,q6 -d gpu -r 1 \
        --n_reps_storage 1 --path "$TPCH_DATA" -s gpu --engines maximus

    # Test 2: Maximus CPU timing on TPC-H q1 (sanity check)
    run_step "maximus_cpu_test" \
        "$MAXBENCH_BIN" --benchmark tpch -q q1 -d cpu -r 1 \
        --n_reps_storage 1 --path "$TPCH_DATA" -s cpu --engines maximus

    if has_sirius; then
        run_step "sirius_timing_test" \
            python3 run_sirius_benchmark.py --n-passes 1 --results-dir "$RESULTS_DIR" \
            tpch
    else
        echo "  [SKIP] Sirius not built (${SIRIUS_DUCKDB} not found)"
    fi
else
    # ── Full mode ──────────────────────────────────────────────────────────

    # ── 1. Maximus GPU-data timing ─────────────────────────────────────────
    run_step "maximus_timing" \
        python3 run_maximus_benchmark.py --n-reps 5 --results-dir "$RESULTS_DIR" \
        tpch h2o clickbench

    # ── 2. Sirius GPU-data timing ──────────────────────────────────────────
    if has_sirius; then
        run_step "sirius_timing" \
            python3 run_sirius_benchmark.py --results-dir "$RESULTS_DIR" \
            tpch h2o clickbench
    else
        echo "  [SKIP] Sirius not built (${SIRIUS_DUCKDB} not found)"
    fi

    # ── 3. Maximus GPU-data metrics ────────────────────────────────────────
    run_step "maximus_metrics" \
        python3 run_maximus_metrics.py --target-time 10 --results-dir "$RESULTS_DIR" \
        tpch h2o clickbench

    # ── 4. Sirius GPU-data metrics (fixed calibration, target=60s) ─────────
    if has_sirius; then
        run_step "sirius_metrics" \
            python3 run_sirius_metrics.py --target-time 60 --results-dir "$RESULTS_DIR" \
            tpch h2o clickbench
    else
        echo "  [SKIP] Sirius metrics: binary not found"
    fi

    # ── 5. Maximus CPU-data metrics ────────────────────────────────────────
    run_step "maximus_cpu_data" \
        python3 run_maximus_cpu_data.py --target-time 10 --results-dir "$RESULTS_DIR" \
        tpch h2o

    # ── 6. Sirius CPU-data (50 reps for steady-state power) ───────────────
    if has_sirius; then
        run_step "sirius_cpu_data" \
            python3 run_sirius_cpu_data.py --n-reps 50 --results-dir "$RESULTS_DIR" \
            tpch h2o clickbench
    else
        echo "  [SKIP] Sirius CPU-data: binary not found"
    fi
fi

echo ""
echo "========================================================================"
if [ "$TEST_MODE" -eq 1 ]; then
    echo "  SMOKE TEST COMPLETE"
else
    echo "  ALL BENCHMARKS COMPLETE"
fi
echo "  Finished: $(date)"
echo "  Results: $RESULTS_DIR"
echo "  Logs: $LOG_DIR"
echo "========================================================================"
