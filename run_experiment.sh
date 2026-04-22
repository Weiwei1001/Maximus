#!/usr/bin/env bash
# =============================================================================
# One-Click Experiment for Maximus + Sirius.
#
# On a fresh machine this single command:
#   1. Installs system packages + Python deps + CUDA 12.6 nvcc.
#   2. Builds Apache Arrow 17, Taskflow, cuDF wheels.
#   3. Builds Maximus (with GPU support).
#   4. Builds Sirius (DuckDB GPU extension) with all its deps.
#   5. Generates TPC-H / H2O / ClickBench CSV + DuckDB data at four scale factors.
#   6. Runs Category A (GPU-resident timing + metrics),
#      Category B (CPU-resident timing + metrics), and
#      Category C (5 × 5 GPU PL/SM energy sweep).
#
# Scale factors:
#   TPC-H:       1, 5, 10, 20       (SF = data size in GB)
#   H2O:         1gb, 2gb, 4gb, 8gb
#   ClickBench:  1, 5, 10, 20       (SF = final CSV size in GB)
#
# Usage:
#   bash run_experiment.sh                      # full A + B + C
#   bash run_experiment.sh --minimum            # 8-hour budget: SF_min+SF_max,
#                                                 no microbench, 3×3 Cat C
#   bash run_experiment.sh --test               # smoke test (1-rep sanity)
#   bash run_experiment.sh --skip-setup         # assume setup done
#   bash run_experiment.sh --skip-data          # assume data generated
#   bash run_experiment.sh --skip-category-c    # A + B only
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
MIN_FLAG=""

for arg in "$@"; do
    case "$arg" in
        --skip-setup) SKIP_SETUP=1 ;;
        --skip-data) SKIP_DATA=1 ;;
        --skip-category-c|--no-energy-sweep) SKIP_CATEGORY_C=1 ;;
        --test) TEST_FLAG="--test" ;;
        --minimum|--min) MIN_FLAG="--minimum" ;;
        -h|--help)
            sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done
EXTRA_FLAGS="$TEST_FLAG $MIN_FLAG"

banner() {
    echo ""
    echo "============================================================"
    echo "  $*"
    echo "============================================================"
}

MAXBENCH_BIN="$MAXIMUS_DIR/build/benchmarks/maxbench"
SIRIUS_BIN="$MAXIMUS_DIR/sirius/build/release/duckdb"

# ─── Phase 1: setup (deps + build) ──────────────────────────────────────────
# We run setup.sh whenever ANY component is missing. setup.sh is idempotent
# and will skip already-built pieces. This avoids the failure mode where a
# previous partial run left maxbench built but Sirius missing and we never
# noticed.
need_setup=0
[ -x "$MAXBENCH_BIN" ] || need_setup=1
[ -x "$SIRIUS_BIN" ]   || need_setup=1
[ -f "$MAXIMUS_DIR/setup_env.sh" ] || need_setup=1

if [ "$SKIP_SETUP" -eq 1 ]; then
    banner "Phase 1: setup — skipped (--skip-setup)"
elif [ "$need_setup" -eq 0 ]; then
    banner "Phase 1: setup — all artifacts present, skipping"
else
    banner "Phase 1: setup (deps + Arrow + cuDF + Maximus + Sirius)"
    SETUP_ARGS=()
    [ "$SKIP_DATA" -eq 1 ] && SETUP_ARGS+=(--skip-data)
    bash "$MAXIMUS_DIR/setup.sh" "${SETUP_ARGS[@]}"
fi

# Fail fast if the required artifacts still aren't there.
missing=""
[ -x "$MAXBENCH_BIN" ] || missing="$missing maxbench"
[ -x "$SIRIUS_BIN" ]   || missing="$missing sirius/duckdb"
[ -f "$MAXIMUS_DIR/setup_env.sh" ] || missing="$missing setup_env.sh"
if [ -n "$missing" ]; then
    echo "ERROR: setup did not produce the required artifacts:$missing"
    echo "  Inspect setup.log (and logs/sirius_cmake.log / logs/sirius_ninja.log"
    echo "   if the failure was in Sirius) and re-run ./run_experiment.sh."
    exit 1
fi

# ─── Phase 2: environment ───────────────────────────────────────────────────
# shellcheck source=/dev/null
source "$MAXIMUS_DIR/setup_env.sh"

# ─── Phase 3: data generation ───────────────────────────────────────────────
if [ "$SKIP_DATA" -eq 0 ]; then
    banner "Phase 3: data generation (TPC-H / H2O / ClickBench)"
    bash "$MAXIMUS_DIR/benchmarks/data/generate_all.sh" "$MAXIMUS_DIR/tests" || {
        echo "  [WARN] generate_all.sh returned non-zero — run_all_benchmarks.sh will retry missing datasets"
    }
fi

# ─── Phase 4: experiments (A + B, and optionally C) ─────────────────────────
if [ "$SKIP_CATEGORY_C" -eq 1 ]; then
    banner "Phase 4: Category A + B experiments (Category C skipped)"
    bash "$MAXIMUS_DIR/benchmarks/scripts/run_all_benchmarks.sh" --skip-category-c $EXTRA_FLAGS
else
    banner "Phase 4: Category A + B + C experiments (full run)"
    bash "$MAXIMUS_DIR/benchmarks/scripts/run_all_benchmarks.sh" $EXTRA_FLAGS
fi

banner "DONE — experiments complete"
echo "Results: $MAXIMUS_DIR/results"
