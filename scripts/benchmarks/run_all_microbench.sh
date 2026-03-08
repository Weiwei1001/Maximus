#!/usr/bin/env bash
# One-command microbench runner: builds, runs all 120 microbench queries with timing+metrics.
#
# Usage:
#   bash scripts/benchmarks/run_all_microbench.sh [--n-reps N] [--skip-build] [--storage-device gpu|cpu]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$REPO_DIR/build"
DATA_DIR="$REPO_DIR/tests"
OUTPUT_DIR="$REPO_DIR/results"

N_REPS=5
SKIP_BUILD=false
STORAGE_DEVICE="gpu"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --n-reps) N_REPS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --storage-device) STORAGE_DEVICE="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Set library paths
export LD_LIBRARY_PATH="/root/arrow_install/lib:/usr/local/lib/python3.10/dist-packages/nvidia/libnvcomp/lib64:/usr/local/lib/python3.10/dist-packages/libkvikio/lib64:/usr/local/lib/python3.10/dist-packages/libcudf/lib64:/usr/local/lib/python3.10/dist-packages/librmm/lib64"

# Build
if [ "$SKIP_BUILD" = false ]; then
    echo "=== Building Maximus ==="
    cd "$BUILD_DIR"

    PB="/usr/local/lib/python3.10/dist-packages"
    export rmm_DIR="${PB}/librmm/lib64/cmake/rmm"
    export nvcomp_DIR="${PB}/nvidia/libnvcomp/lib64/cmake/nvcomp"
    export rapids_logger_DIR="${PB}/rapids_logger/lib64/cmake/rapids_logger"
    export nvtx3_DIR="${PB}/librmm/lib64/cmake/nvtx3"
    export cuco_DIR="${PB}/libcudf/lib64/cmake/cuco"
    export CCCL_DIR="${PB}/libcudf/include/libcudf/lib/rapids/cmake/cccl"
    export fmt_DIR="${PB}/librmm/lib64/cmake/fmt"
    export spdlog_DIR="${PB}/librmm/lib64/cmake/spdlog"

    cmake -C initial_cache.cmake ..
    cmake --build . -j$(nproc)
    echo "=== Build complete ==="
fi

# Run microbench
echo ""
echo "=== Running Microbench (120 queries, ${N_REPS} reps, storage=${STORAGE_DEVICE}) ==="
python3 "$SCRIPT_DIR/run_microbench_maximus.py" \
    --maximus-dir "$REPO_DIR" \
    --data-dir "$DATA_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --n-reps "$N_REPS" \
    --device gpu \
    --storage-device "$STORAGE_DEVICE" \
    --sample-interval 50

echo ""
echo "=== Results ==="
echo "Timing:  $OUTPUT_DIR/microbench_maximus_timing.csv"
echo "Metrics: $OUTPUT_DIR/microbench_maximus_metrics.csv"
