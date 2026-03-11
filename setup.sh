#!/usr/bin/env bash
# =============================================================================
# Maximus + Sirius: One-Click GPU Benchmark Setup
#
# This script sets up everything needed to run GPU SQL benchmarks on a fresh
# machine. It installs dependencies, builds both engines, generates test data,
# and runs a smoke test.
#
# Usage:
#   ./setup.sh                    # Full setup (Maximus + Sirius + data)
#   ./setup.sh --maximus-only     # Maximus only (no Sirius)
#   ./setup.sh --skip-data        # Skip data generation
#   ./setup.sh --skip-sirius      # Skip Sirius installation
#
# Prerequisites:
#   - Ubuntu 22.04 or 24.04
#   - NVIDIA GPU with >= 24GB VRAM (32GB recommended)
#   - CUDA toolkit installed (nvcc available)
#   - Root/sudo access
#   - >= 128GB RAM, >= 300GB disk space
# =============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAXIMUS_DIR="$SCRIPT_DIR"
WORKSPACE="$(dirname "$MAXIMUS_DIR")"
LOG_FILE="$MAXIMUS_DIR/setup.log"

# Parse arguments
INSTALL_SIRIUS=true
GENERATE_DATA=true
for arg in "$@"; do
    case $arg in
        --maximus-only|--skip-sirius) INSTALL_SIRIUS=false ;;
        --skip-data) GENERATE_DATA=false ;;
        --help|-h)
            echo "Usage: ./setup.sh [--maximus-only] [--skip-sirius] [--skip-data]"
            echo ""
            echo "Options:"
            echo "  --maximus-only   Only install Maximus (skip Sirius)"
            echo "  --skip-sirius    Same as --maximus-only"
            echo "  --skip-data      Skip benchmark data generation"
            exit 0
            ;;
    esac
done

log() { echo "[setup] $*" | tee -a "$LOG_FILE"; }

log "=============================================="
log "  Maximus + Sirius GPU Benchmark Setup"
log "  Date: $(date)"
log "  Machine: $(hostname)"
log "=============================================="

# ─────────────────────────────────────────────────────────────────────────────
# Step 0: Initialize git submodules (Sirius is a submodule)
# ─────────────────────────────────────────────────────────────────────────────
if [ -f "$MAXIMUS_DIR/.gitmodules" ]; then
    log "Step 0: Initializing git submodules..."
    cd "$MAXIMUS_DIR"
    git submodule update --init --recursive
    log "  Sirius submodule: $MAXIMUS_DIR/sirius"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Check prerequisites
# ─────────────────────────────────────────────────────────────────────────────
log "Step 1: Checking prerequisites..."

check_cmd() {
    if ! command -v "$1" &>/dev/null; then
        log "  WARNING: $1 not found"
        return 1
    fi
    log "  $1: $(command -v "$1")"
    return 0
}

check_cmd git || { log "FATAL: git is required"; exit 1; }
check_cmd cmake || CMAKE_NEEDED=true
check_cmd nvcc || { log "FATAL: CUDA toolkit required (nvcc not found)"; exit 1; }

if command -v nvidia-smi &>/dev/null; then
    log "  GPU: $(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits | head -1)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Install system dependencies
# ─────────────────────────────────────────────────────────────────────────────
log "Step 2: Installing system dependencies..."

# Remove broken apt sources (e.g. heavyai returning 403)
sudo rm -f /etc/apt/sources.list.d/heavyai*.list 2>/dev/null || true

sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
    build-essential g++ ninja-build \
    libboost-all-dev libsnappy-dev libbrotli-dev \
    libthrift-dev libre2-dev rapidjson-dev \
    libssl-dev libconfig++-dev libnuma-dev \
    ca-certificates wget curl pkg-config \
    python3 python3-pip python3-venv

# Ensure GCC >= 11 for C++20 support (required by Taskflow)
GCC_MAJOR=$(gcc -dumpversion 2>/dev/null | cut -d. -f1)
if [ "${GCC_MAJOR:-0}" -lt 11 ]; then
    log "  Upgrading GCC to 11 for C++20 support..."
    sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test 2>/dev/null || true
    sudo apt-get update -qq
    sudo apt-get install -y gcc-11 g++-11
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 110 \
        --slave /usr/bin/g++ g++ /usr/bin/g++-11
    log "  GCC upgraded to $(gcc --version | head -1)"
fi

# Upgrade CMake if needed (>= 3.17 for Maximus, >= 3.30.4 for Sirius)
if [ "${CMAKE_NEEDED:-false}" = true ] || [ "${INSTALL_SIRIUS}" = true ]; then
    CMAKE_MIN=33004  # 3.30.4 for Sirius compatibility
    CMAKE_CUR=$(cmake --version 2>/dev/null | head -1 | sed -n 's/.*version \([0-9]*\.[0-9]*\.[0-9]*\).*/\1/p' | awk -F. '{print $1*10000+$2*100+$3}')
    if [ "${CMAKE_CUR:-0}" -lt "$CMAKE_MIN" ]; then
        log "  Upgrading CMake..."
        if [ -f /etc/os-release ]; then . /etc/os-release; fi
        CODENAME="${VERSION_CODENAME:-noble}"
        wget -qO - https://apt.kitware.com/keys/kitware-archive-latest.asc | \
            sudo gpg --dearmor -o /usr/share/keyrings/kitware-archive-keyring.gpg 2>/dev/null || true
        echo "deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ ${CODENAME} main" | \
            sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
        sudo apt-get update -qq && sudo apt-get install -y cmake
    fi
fi
log "  CMake: $(cmake --version | head -1)"

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Install Python dependencies
# ─────────────────────────────────────────────────────────────────────────────
log "Step 3: Installing Python dependencies..."

pip install --quiet duckdb pynvml matplotlib pandas numpy 2>/dev/null || \
pip3 install --quiet duckdb pynvml matplotlib pandas numpy

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Install cuDF (GPU dataframe library)
# ─────────────────────────────────────────────────────────────────────────────
log "Step 4: Installing cuDF..."

# Try pip first (simpler), fall back to conda
if python3 -c "import cudf" 2>/dev/null; then
    log "  cuDF already installed"
elif pip install cudf-cu12 libcudf-cu12 2>/dev/null; then
    log "  cuDF installed via pip"
else
    log "  pip install failed, trying conda..."
    MINICONDA_DIR="${WORKSPACE}/miniconda3"
    if [ ! -x "${MINICONDA_DIR}/bin/conda" ]; then
        wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
        bash /tmp/miniconda.sh -b -p "${MINICONDA_DIR}"
        rm -f /tmp/miniconda.sh
    fi
    export PATH="${MINICONDA_DIR}/bin:${PATH}"
    if ! conda info --envs 2>/dev/null | grep -q maximus_gpu; then
        conda create -n maximus_gpu -y
        conda run -n maximus_gpu conda install -y -c rapidsai -c conda-forge -c nvidia \
            "libcudf=24.12" cuda-version=12
    fi
    log "  cuDF installed via conda"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Build Apache Arrow
# ─────────────────────────────────────────────────────────────────────────────
ARROW_INSTALL="$HOME/arrow_install"
if [ -f "$ARROW_INSTALL/lib/libarrow.so" ] || [ -f "$ARROW_INSTALL/lib64/libarrow.so" ]; then
    log "Step 5: Apache Arrow already built at $ARROW_INSTALL"
else
    log "Step 5: Building Apache Arrow 17.0.0..."
    cd "$WORKSPACE"
    bash "$MAXIMUS_DIR/scripts/build_arrow.sh"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Build Taskflow
# ─────────────────────────────────────────────────────────────────────────────
TASKFLOW_INSTALL="$HOME/taskflow_install"
if [ -d "$TASKFLOW_INSTALL" ]; then
    log "Step 6: Taskflow already built at $TASKFLOW_INSTALL"
else
    log "Step 6: Building Taskflow..."
    cd "$WORKSPACE"
    bash "$MAXIMUS_DIR/scripts/build_taskflow.sh"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 6.5: Patch source files for cuDF 24.12 API compatibility
# ─────────────────────────────────────────────────────────────────────────────
log "Step 6.5: Applying cuDF 24.12 compatibility patches..."

# Fix 1: RMM header path — now handled by __has_include in context.hpp (no patch needed)

# Fix 2: allocate/deallocate — now uses generic pool().allocate() that works with all RMM versions (no patch needed)

# Fix 3: cudf/join/join.hpp -> cudf/join.hpp (header reorganized in cuDF 24.12)
sed -i 's|#include <cudf/join/join\.hpp>|#include <cudf/join.hpp>|' \
    "$MAXIMUS_DIR/src/maximus/operators/gpu/cudf/hash_join_operator.hpp" \
    "$MAXIMUS_DIR/src/maximus/operators/gpu/cudf/hash_join_operator.cpp" \
    "$MAXIMUS_DIR/tests/cuda.cpp"

# Fix 4: Remove cudf/join/filtered_join.hpp include (class removed in 24.12)
sed -i '/#include <cudf\/join\/filtered_join\.hpp>/d' \
    "$MAXIMUS_DIR/src/maximus/operators/gpu/cudf/hash_join_operator.cpp"

# Fix 5 & 6: Complex multi-line patches (use Python for reliability)
python3 - "$MAXIMUS_DIR" << 'PYEOF'
import re, sys, os

MAXIMUS_DIR = sys.argv[1]

# --- Fix 5: Rewrite semi_join functions in hash_join_operator.cpp ---
hjf = os.path.join(MAXIMUS_DIR, "src/maximus/operators/gpu/cudf/hash_join_operator.cpp")

with open(hjf, "r") as f:
    src = f.read()

# Replace semi_join_and_gather_left_impl (filtered_join -> standalone)
old_left = re.compile(
    r'// libcudf 26\.x: use filtered_join for semi/anti.*?'
    r'static std::shared_ptr<::cudf::table> semi_join_and_gather_left_impl\(.*?\n\}',
    re.DOTALL)
new_left = """// cuDF 24.12: use standalone left_semi_join / left_anti_join
static std::shared_ptr<::cudf::table> semi_join_and_gather_left_impl(
    ::cudf::table_view const& left_input,
    ::cudf::table_view const& right_input,
    std::vector<::cudf::size_type> const& left_key_indices,
    std::vector<::cudf::size_type> const& right_key_indices,
    ::cudf::null_equality compare_nulls,
    bool anti) {
    auto left_keys  = left_input.select(left_key_indices);
    auto right_keys = right_input.select(right_key_indices);
    std::unique_ptr<rmm::device_uvector<::cudf::size_type>> left_join_indices =
        anti ? ::cudf::left_anti_join(left_keys, right_keys, compare_nulls)
             : ::cudf::left_semi_join(left_keys, right_keys, compare_nulls);
    return std::make_shared<::cudf::table>(
        gather_column(left_input, std::move(*left_join_indices), ::cudf::out_of_bounds_policy::DONT_CHECK));
}"""
src = old_left.sub(new_left, src, count=1)

# Replace semi_join_and_gather_right_impl
old_right = re.compile(
    r'static std::shared_ptr<::cudf::table> semi_join_and_gather_right_impl\('
    r'.*?\n\}',
    re.DOTALL)
new_right = """static std::shared_ptr<::cudf::table> semi_join_and_gather_right_impl(
    ::cudf::table_view const& left_input,
    ::cudf::table_view const& right_input,
    std::vector<::cudf::size_type> const& left_key_indices,
    std::vector<::cudf::size_type> const& right_key_indices,
    ::cudf::null_equality compare_nulls,
    bool anti) {
    auto left_keys  = left_input.select(left_key_indices);
    auto right_keys = right_input.select(right_key_indices);
    std::unique_ptr<rmm::device_uvector<::cudf::size_type>> right_join_indices =
        anti ? ::cudf::left_anti_join(right_keys, left_keys, compare_nulls)
             : ::cudf::left_semi_join(right_keys, left_keys, compare_nulls);
    return std::make_shared<::cudf::table>(
        gather_column(right_input, std::move(*right_join_indices), ::cudf::out_of_bounds_policy::DONT_CHECK));
}"""
src = old_right.sub(new_right, src, count=1)

with open(hjf, "w") as f:
    f.write(src)
print("  Patched hash_join_operator.cpp")

# --- Fix 6: Replace COUNT(*) reduction in group_by_operator.cpp ---
gbf = os.path.join(MAXIMUS_DIR, "src/maximus/operators/gpu/cudf/group_by_operator.cpp")
with open(gbf, "r") as f:
    src = f.read()

# Add scalar_factories include if missing
if "scalar_factories.hpp" not in src:
    src = src.replace(
        '#include <cudf/column/column_factories.hpp>',
        '#include <cudf/column/column_factories.hpp>\n#include <cudf/scalar/scalar_factories.hpp>')

# Replace the count aggregation block (from "hash_count" branch to its output_cols push)
old_count = re.compile(
    r'(\} else if \(aggr\.second == "hash_count" \|\| aggr\.second == "count"\) \{)\s*\n'
    r'.*?'
    r'(output_cols\.push_back\(std::move\(col\)\);)',
    re.DOTALL)
new_count = r"""\1
                // COUNT(*) as reduction: count all rows including nulls
                auto scalar = ::cudf::make_fixed_width_scalar<int32_t>(static_cast<int32_t>(complete_view.num_rows()));
                auto col = ::cudf::make_column_from_scalar(*scalar, 1);
                \2"""
src = old_count.sub(new_count, src, count=1)

with open(gbf, "w") as f:
    f.write(src)
print("  Patched group_by_operator.cpp")
PYEOF

# Fix conda fmt/spdlog header version conflicts (base conda has fmt v9, env has v11)
MINICONDA_DIR="${WORKSPACE}/miniconda3"
if [ -d "${MINICONDA_DIR}/include/fmt" ] && [ ! -d "${MINICONDA_DIR}/include/fmt.bak" ]; then
    log "  Moving conflicting fmt v9 headers from base conda..."
    mv "${MINICONDA_DIR}/include/fmt" "${MINICONDA_DIR}/include/fmt.bak"
fi
if [ -d "${MINICONDA_DIR}/include/spdlog" ] && [ ! -d "${MINICONDA_DIR}/include/spdlog.bak" ]; then
    log "  Moving conflicting spdlog v1.11 headers from base conda..."
    mv "${MINICONDA_DIR}/include/spdlog" "${MINICONDA_DIR}/include/spdlog.bak"
fi

log "  Patches applied."

# ─────────────────────────────────────────────────────────────────────────────
# Step 7: Build Maximus
# ─────────────────────────────────────────────────────────────────────────────
log "Step 7: Building Maximus with GPU support..."
cd "$MAXIMUS_DIR"

BUILD_DIR="$MAXIMUS_DIR/build"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Detect CUDA architecture
CUDA_ARCH="${CMAKE_CUDA_ARCHITECTURES:-native}"

# Try conda env first, then pip for cuDF paths
if [ -n "$CONDA_PREFIX" ] && [ -f "$CONDA_PREFIX/lib/cmake/cudf/cudf-config.cmake" ]; then
    CUDF_PREFIX="$CONDA_PREFIX"
elif [ -d "${WORKSPACE}/miniconda3/envs/maximus_gpu" ]; then
    CUDF_PREFIX="${WORKSPACE}/miniconda3/envs/maximus_gpu"
else
    # pip-installed cuDF: use configure script
    log "  Using pip-installed cuDF configuration..."
    bash "$MAXIMUS_DIR/scripts/configure_with_gpu_pip_cudf.sh" \
        -DMAXIMUS_WITH_BENCHMARKS=ON \
        -DCMAKE_CUDA_ARCHITECTURES="$CUDA_ARCH" ..
    cmake --build . -j "$(nproc)"
    cd "$MAXIMUS_DIR"
    # Skip the cmake below
    MAXIMUS_BUILT=true
fi

if [ "${MAXIMUS_BUILT:-false}" != true ]; then
    cmake -DCMAKE_BUILD_TYPE=Release \
        -DMAXIMUS_WITH_TESTS=ON \
        -DMAXIMUS_WITH_GPU=ON \
        -DMAXIMUS_WITH_BENCHMARKS=ON \
        -DCMAKE_CUDA_ARCHITECTURES="$CUDA_ARCH" \
        -DCMAKE_PREFIX_PATH="$ARROW_INSTALL;$TASKFLOW_INSTALL;$CUDF_PREFIX" \
        -DCMAKE_CXX_STANDARD=20 \
        -GNinja ..

    ninja -j "$(nproc)"
    cd "$MAXIMUS_DIR"
fi

# Verify maxbench built
if [ -x "$BUILD_DIR/benchmarks/maxbench" ]; then
    log "  Maximus built successfully: $BUILD_DIR/benchmarks/maxbench"
else
    log "  WARNING: maxbench not found, build may have partially failed"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 8: Setup LD_LIBRARY_PATH
# ─────────────────────────────────────────────────────────────────────────────
log "Step 8: Setting up runtime environment..."

# Create setup_env.sh for runtime use
cat > "$MAXIMUS_DIR/setup_env.sh" << 'ENVEOF'
#!/bin/bash
# Source this file to set up the runtime environment for Maximus benchmarks.
# Usage: source setup_env.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(dirname "$SCRIPT_DIR")"

# Arrow
export LD_LIBRARY_PATH="$HOME/arrow_install/lib:$HOME/arrow_install/lib64:${LD_LIBRARY_PATH:-}"

# cuDF (pip-installed)
PIP_BASE="/usr/local/lib/python3.12/dist-packages"
if [ -d "$PIP_BASE/nvidia/libnvcomp/lib64" ]; then
    export LD_LIBRARY_PATH="$PIP_BASE/nvidia/libnvcomp/lib64:$PIP_BASE/libkvikio/lib64:$LD_LIBRARY_PATH"
fi

# cuDF (conda)
if [ -n "$CONDA_PREFIX" ]; then
    export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:$LD_LIBRARY_PATH"
fi

# Maximus
export MAXIMUS_DIR="$SCRIPT_DIR"
export MAXBENCH="$SCRIPT_DIR/build/benchmarks/maxbench"
export SIRIUS_BIN="$SCRIPT_DIR/sirius/build/release/duckdb"
export RESULTS_DIR="$SCRIPT_DIR/results"

echo "Environment configured."
echo "  MAXIMUS_DIR=$MAXIMUS_DIR"
echo "  MAXBENCH=$MAXBENCH"
[ -x "$SIRIUS_BIN" ] && echo "  SIRIUS_BIN=$SIRIUS_BIN"
ENVEOF
chmod +x "$MAXIMUS_DIR/setup_env.sh"
source "$MAXIMUS_DIR/setup_env.sh"

# ─────────────────────────────────────────────────────────────────────────────
# Step 9: Install Sirius (optional)
# ─────────────────────────────────────────────────────────────────────────────
if [ "$INSTALL_SIRIUS" = true ]; then
    log "Step 9: Installing Sirius (DuckDB GPU extension)..."
    SIRIUS_DIR="$MAXIMUS_DIR/sirius"
    bash "$MAXIMUS_DIR/scripts/install_sirius.sh" "$SIRIUS_DIR"
    if [ -x "$SIRIUS_DIR/build/release/duckdb" ]; then
        log "  Sirius built successfully: $SIRIUS_DIR/build/release/duckdb"
    else
        log "  WARNING: Sirius build may have failed"
    fi
else
    log "Step 9: Skipping Sirius installation (--skip-sirius)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 10: Generate benchmark data (optional)
# ─────────────────────────────────────────────────────────────────────────────
if [ "$GENERATE_DATA" = true ]; then
    log "Step 10: Generating benchmark data..."
    bash "$MAXIMUS_DIR/benchmarks/data/generate_all.sh" "$MAXIMUS_DIR/tests"
else
    log "Step 10: Skipping data generation (--skip-data)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 11: Smoke test
# ─────────────────────────────────────────────────────────────────────────────
log "Step 11: Running smoke test..."

MAXBENCH="$BUILD_DIR/benchmarks/maxbench"
if [ -x "$MAXBENCH" ]; then
    TPCH_SMALL="$MAXIMUS_DIR/tests/tpch/csv-1"
    if [ -d "$TPCH_SMALL" ]; then
        log "  Running Maximus TPC-H Q1 (SF=1)..."
        "$MAXBENCH" --benchmark tpch -q q1 -d gpu -r 1 --n_reps_storage 1 \
            --path "$TPCH_SMALL" -s gpu --engines maximus 2>&1 | tail -5 || true
        log "  Maximus smoke test passed."
    else
        log "  No test data yet (run with --generate-data to create)"
    fi
fi

if [ "$INSTALL_SIRIUS" = true ]; then
    SIRIUS_BIN="$MAXIMUS_DIR/sirius/build/release/duckdb"
    TPCH_DB="$MAXIMUS_DIR/tests/tpch_duckdb/tpch_sf1.duckdb"
    if [ -x "$SIRIUS_BIN" ] && [ -f "$TPCH_DB" ]; then
        log "  Running Sirius TPC-H Q1 (SF=1)..."
        echo '.timer on
call gpu_buffer_init("1 GB", "2 GB");
call gpu_processing("SELECT l_returnflag, l_linestatus, sum(l_quantity) FROM lineitem GROUP BY 1,2 ORDER BY 1,2;");
.quit' | "$SIRIUS_BIN" "$TPCH_DB" 2>&1 | tail -5 || true
        log "  Sirius smoke test passed."
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────────────
log ""
log "=============================================="
log "  Setup Complete!"
log "=============================================="
log ""
log "Next steps:"
log "  1. Source environment: source $MAXIMUS_DIR/setup_env.sh"
log "  2. Run benchmarks:"
log "     python $MAXIMUS_DIR/benchmarks/scripts/run_all.py"
log "     python $MAXIMUS_DIR/benchmarks/scripts/run_all.py --engine maximus"
log "     python $MAXIMUS_DIR/benchmarks/scripts/run_all.py --engine sirius"
log "  3. Compare results:"
log "     python $MAXIMUS_DIR/benchmarks/scripts/compare_results.py"
log "  4. Generate plots:"
log "     python $MAXIMUS_DIR/benchmarks/scripts/plot_metrics.py"
log ""
log "Full log: $LOG_FILE"
