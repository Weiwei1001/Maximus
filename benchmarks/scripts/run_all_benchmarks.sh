#!/bin/bash
# Master script to re-run ALL benchmarks for Maximus and Sirius
# Both GPU-data and CPU-data modes, timing and metrics
#
# Estimated total runtime: 3-6 hours
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="/home/xzw/gpu_db/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="$RESULTS_DIR/logs_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

echo "========================================================================"
echo "  FULL BENCHMARK RE-RUN"
echo "  Started: $(date)"
echo "  Results: $RESULTS_DIR"
echo "  Logs: $LOG_DIR"
echo "========================================================================"

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

# ── 1. Maximus GPU-data timing ──────────────────────────────────────────────
run_step "maximus_timing" \
    python3 run_maximus_benchmark.py --n-reps 5 --results-dir "$RESULTS_DIR" \
    tpch h2o clickbench

# ── 2. Sirius GPU-data timing ───────────────────────────────────────────────
run_step "sirius_timing" \
    python3 run_sirius_benchmark.py --results-dir "$RESULTS_DIR" \
    tpch h2o clickbench

# ── 3. Maximus GPU-data metrics ─────────────────────────────────────────────
run_step "maximus_metrics" \
    python3 run_maximus_metrics.py --target-time 10 --results-dir "$RESULTS_DIR" \
    tpch h2o clickbench

# ── 4. Sirius GPU-data metrics (fixed calibration, target=60s) ──────────────
run_step "sirius_metrics" \
    python3 run_sirius_metrics.py --target-time 60 --results-dir "$RESULTS_DIR" \
    tpch h2o clickbench

# ── 5. Maximus CPU-data metrics ─────────────────────────────────────────────
run_step "maximus_cpu_data" \
    python3 run_maximus_cpu_data.py --target-time 10 --results-dir "$RESULTS_DIR" \
    tpch h2o

# ── 6. Sirius CPU-data (50 reps for steady-state power) ────────────────────
run_step "sirius_cpu_data" \
    python3 run_sirius_cpu_data.py --n-reps 50 --results-dir "$RESULTS_DIR" \
    tpch h2o clickbench

echo ""
echo "========================================================================"
echo "  ALL BENCHMARKS COMPLETE"
echo "  Finished: $(date)"
echo "  Results: $RESULTS_DIR"
echo "  Logs: $LOG_DIR"
echo "========================================================================"
