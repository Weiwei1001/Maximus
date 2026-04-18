#!/usr/bin/env bash
# =============================================================================
# One-Click Experiment: Category A + B (skip Category C energy sweep).
#
# This single command reproduces the full GPU SQL benchmark on a fresh
# machine: installs dependencies, builds Maximus + Sirius, generates benchmark
# data (TPC-H / H2O / ClickBench), and runs Category A (GPU-data) and
# Category B (CPU-data) timing + metrics.
#
# Scale factors (updated):
#   TPC-H:       1, 5, 10, 20
#   H2O:         1gb, 2gb, 4gb, 8gb
#   ClickBench:  1, 5, 10, 20   (SF = final CSV data size in GB)
#
# Usage:
#   bash run_experiment.sh                 # full A + B + C (energy sweep)
#   bash run_experiment.sh --test          # quick smoke test
#   bash run_experiment.sh --skip-setup    # assume setup already done
#   bash run_experiment.sh --skip-data     # assume data already generated
#   bash run_experiment.sh --skip-category-c   # only A + B
#
# Idempotent: safe to re-run after an interruption.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAXIMUS_DIR="$SCRIPT_DIR"

SKIP_SETUP=0
SKIP_DATA=0
SKIP_CATEGORY_C=0
TEST_FLAG=""

for arg in "$@"; do
    case "$arg" in
        --skip-setup) SKIP_SETUP=1 ;;
        --skip-data) SKIP_DATA=1 ;;
        --skip-category-c|--no-energy-sweep) SKIP_CATEGORY_C=1 ;;
        --test) TEST_FLAG="--test" ;;
        -h|--help)
            sed -n '2,22p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

banner() {
    echo ""
    echo "============================================================"
    echo "  $*"
    echo "============================================================"
}

# ─── Phase 1: setup (deps + build) ──────────────────────────────────────────
if [ "$SKIP_SETUP" -eq 0 ]; then
    if [ -x "$MAXIMUS_DIR/build/benchmarks/maxbench" ] && [ -f "$MAXIMUS_DIR/setup_env.sh" ]; then
        banner "Phase 1: setup — already built, skipping"
    else
        banner "Phase 1: setup (deps + Arrow + cuDF + Maximus + Sirius)"
        SETUP_ARGS=()
        [ "$SKIP_DATA" -eq 1 ] && SETUP_ARGS+=(--skip-data)
        bash "$MAXIMUS_DIR/setup.sh" "${SETUP_ARGS[@]}"
    fi
fi

# ─── Phase 2: environment ───────────────────────────────────────────────────
if [ ! -f "$MAXIMUS_DIR/setup_env.sh" ]; then
    echo "ERROR: setup_env.sh not found; Phase 1 must run successfully first."
    exit 1
fi
# shellcheck source=/dev/null
source "$MAXIMUS_DIR/setup_env.sh"

# ─── Phase 3: data generation (run_all_benchmarks.sh also handles missing) ──
if [ "$SKIP_DATA" -eq 0 ] && [ -f "$MAXIMUS_DIR/benchmarks/data/generate_all.sh" ]; then
    banner "Phase 3: data generation (TPC-H / H2O / ClickBench)"
    bash "$MAXIMUS_DIR/benchmarks/data/generate_all.sh" "$MAXIMUS_DIR/tests" || {
        echo "  [WARN] generate_all.sh returned non-zero — run_all_benchmarks.sh will retry missing datasets"
    }
fi

# ─── Phase 4: experiments (A + B, and optionally C) ─────────────────────────
if [ "$SKIP_CATEGORY_C" -eq 1 ]; then
    banner "Phase 4: Category A + B experiments (Category C skipped)"
    bash "$MAXIMUS_DIR/benchmarks/scripts/run_all_benchmarks.sh" --skip-category-c $TEST_FLAG
else
    banner "Phase 4: Category A + B + C experiments (full run)"
    bash "$MAXIMUS_DIR/benchmarks/scripts/run_all_benchmarks.sh" $TEST_FLAG
fi

banner "DONE — experiments complete"
echo "Results: $MAXIMUS_DIR/results"
