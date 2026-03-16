#!/bin/bash
# Build Sirius (DuckDB GPU extension) from submodule sources.
# Applies compatibility patches for:
#   - spdlog/fmt namespace conflict with DuckDB's bundled fmt
#   - CUDA 12.6 compat (cudaMemcpySrcAccessOrder is 12.8+)
#   - libconfig++ cmake finder
#   - fmt v10 enum formatting
#
# Usage:
#   bash sirius_patches/build_sirius.sh          # Build release
#   bash sirius_patches/build_sirius.sh --clean   # Clean + rebuild
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SIRIUS_DIR="$REPO_DIR/sirius"
DUCKDB_BIN="$SIRIUS_DIR/build/release/duckdb"

if [ "${1:-}" = "--clean" ]; then
    echo "[build_sirius] Cleaning previous build..."
    rm -rf "$SIRIUS_DIR/build/release"
fi

# Skip if already built
if [ -x "$DUCKDB_BIN" ]; then
    echo "[build_sirius] Sirius DuckDB binary already exists: $DUCKDB_BIN"
    echo "[build_sirius] Use --clean to force rebuild."
    exit 0
fi

echo "[build_sirius] Building Sirius DuckDB..."

# ── 0. Init submodules ──
cd "$REPO_DIR"
git submodule update --init --recursive

# ── 1. Install system deps (requires sudo/root) ──
echo "[build_sirius] Installing system dependencies..."
apt-get update -qq
apt-get install -y -qq libnuma-dev libconfig++-dev >/dev/null

# Ensure CUDA 12.6 nvcc + dev packages (cuDF 26.x CCCL headers need >= 12.6)
NVCC_VER=$(nvcc --version 2>/dev/null | grep "release" | sed 's/.*release //' | sed 's/,.*//')
NVCC_MINOR=$(echo "$NVCC_VER" | cut -d. -f2)
if [ "${NVCC_MINOR:-0}" -lt 6 ]; then
    echo "[build_sirius] Installing CUDA 12.6 nvcc..."
    apt-get install -y -qq cuda-nvcc-12-6 cuda-nvml-dev-12-6 libcurand-dev-12-6 cuda-cudart-dev-12-6 >/dev/null
fi

# Abseil >= 20230125 (for absl::any_invocable)
if ! pkg-config --atleast-version=20230125 absl_any_invocable 2>/dev/null; then
    if [ ! -f /usr/local/lib/cmake/absl/abslConfig.cmake ] || \
       ! grep -q "20240722" /usr/local/lib/cmake/absl/abslConfigVersion.cmake 2>/dev/null; then
        echo "[build_sirius] Building abseil-cpp 20240722..."
        ABSL_TMP=$(mktemp -d)
        git clone --depth 1 --branch 20240722.0 https://github.com/abseil/abseil-cpp.git "$ABSL_TMP/absl"
        cmake -B "$ABSL_TMP/absl/build" -GNinja "$ABSL_TMP/absl" \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
            -DABSL_BUILD_TESTING=OFF \
            -DCMAKE_CXX_STANDARD=17 >/dev/null
        ninja -C "$ABSL_TMP/absl/build" >/dev/null
        ninja -C "$ABSL_TMP/absl/build" install >/dev/null
        rm -rf "$ABSL_TMP"
        echo "[build_sirius] abseil installed."
    fi
fi

# ── 2. Build spdlog from source with bundled fmt ──
#    System spdlog uses fmt 9 which conflicts with DuckDB's duckdb_fmt namespace.
SPDLOG_CONFIG="/usr/local/lib/cmake/spdlog/spdlogConfig.cmake"
if [ ! -f "$SPDLOG_CONFIG" ]; then
    echo "[build_sirius] Building spdlog 1.14.1 with bundled fmt..."
    SPDLOG_TMP=$(mktemp -d)
    git clone --depth 1 --branch v1.14.1 https://github.com/gabime/spdlog.git "$SPDLOG_TMP/spdlog"
    cmake -B "$SPDLOG_TMP/spdlog/build" -GNinja "$SPDLOG_TMP/spdlog" \
        -DCMAKE_BUILD_TYPE=Release \
        -DSPDLOG_FMT_EXTERNAL=OFF \
        -DSPDLOG_BUILD_SHARED=OFF \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON >/dev/null
    ninja -C "$SPDLOG_TMP/spdlog/build" >/dev/null
    ninja -C "$SPDLOG_TMP/spdlog/build" install >/dev/null
    rm -rf "$SPDLOG_TMP"
    echo "[build_sirius] spdlog installed."
fi

# ── 3. Create nvcomp .so symlinks (pip only ships .so.5) ──
SITE_PKGS=$(python3 -c "import sysconfig; print(sysconfig.get_path('purelib'))")
NVCOMP_DIR="$SITE_PKGS/nvidia/libnvcomp/lib64"
if [ -f "$NVCOMP_DIR/libnvcomp.so.5" ] && [ ! -f "$NVCOMP_DIR/libnvcomp.so" ]; then
    ln -sf "$NVCOMP_DIR/libnvcomp.so.5" "$NVCOMP_DIR/libnvcomp.so"
    ln -sf "$NVCOMP_DIR/libnvcomp_cpu.so.5" "$NVCOMP_DIR/libnvcomp_cpu.so"
    echo "[build_sirius] Created nvcomp .so symlinks."
fi

# ── 4. Apply patches ──
echo "[build_sirius] Applying compatibility patches..."

# Copy cmake finder module
cp "$SCRIPT_DIR/Findlibconfig++.cmake" "$SIRIUS_DIR/cmake/"

# Apply sirius source patches
cd "$SIRIUS_DIR"
if git diff --quiet; then
    git apply "$SCRIPT_DIR/sirius.patch"
    echo "  Applied sirius.patch"
else
    echo "  Sirius already has local changes, skipping patch"
fi

# Apply cucascade patches
cd "$SIRIUS_DIR/cucascade"
if git diff --quiet; then
    git apply "$SCRIPT_DIR/cucascade.patch"
    echo "  Applied cucascade.patch"
else
    echo "  cuCascade already has local changes, skipping patch"
fi

# ── 5. Detect GPU architecture ──
GPU_ARCH=""
if command -v nvidia-smi &>/dev/null; then
    # Get compute capability from nvidia-smi
    CC=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1 | tr -d '.')
    if [ -n "$CC" ]; then
        GPU_ARCH="$CC"
    fi
fi
if [ -z "$GPU_ARCH" ]; then
    GPU_ARCH="native"
fi
echo "[build_sirius] GPU architecture: $GPU_ARCH"

# ── 6. Build cmake prefix paths from pip-installed RAPIDS packages ──
CMAKE_PREFIXES=""
for pkg_dir in \
    "$SITE_PKGS/libcudf/lib64/cmake" \
    "$SITE_PKGS/librmm/lib64/cmake" \
    "$SITE_PKGS/nvidia/libnvcomp/lib64/cmake" \
    "$SITE_PKGS/libcudf/lib64/rapids/cmake" \
    "$SITE_PKGS/librmm/lib64/rapids/cmake" \
    "$SITE_PKGS/libkvikio/lib64/cmake" \
    "$SITE_PKGS/rapids_logger/lib64/cmake" \
    "$SITE_PKGS/lib64/cmake" \
    "$SITE_PKGS/nvidia/cuda_cccl/lib/cmake" \
    "$SITE_PKGS/nvidia/libnvcomp/lib64/cmake/nvcomp" \
    "/usr/local/lib/cmake"; do
    if [ -d "$pkg_dir" ]; then
        CMAKE_PREFIXES="${CMAKE_PREFIXES:+$CMAKE_PREFIXES;}$pkg_dir"
    fi
done

# ── 7. Configure ──
echo "[build_sirius] Configuring..."
cd "$SIRIUS_DIR/duckdb"
cmake -B ../build/release -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DEXTENSION_STATIC_BUILD=ON \
    -DDUCKDB_EXTENSION_CONFIGS="../extension_config.cmake" \
    -DCMAKE_CUDA_ARCHITECTURES="$GPU_ARCH" \
    -DCMAKE_PREFIX_PATH="$CMAKE_PREFIXES" \
    -DCMAKE_MODULE_PATH="$SIRIUS_DIR/cmake" \
    2>&1 | tail -5

# ── 8. Build ──
echo "[build_sirius] Building (this may take 5-15 minutes)..."
ninja -C ../build/release -j$(nproc) 2>&1 | tail -5

# ── 9. Verify ──
if [ -x "$DUCKDB_BIN" ]; then
    VERSION=$("$DUCKDB_BIN" --version 2>/dev/null || echo "unknown")
    echo "[build_sirius] SUCCESS: $DUCKDB_BIN ($VERSION)"
else
    echo "[build_sirius] FAILED: duckdb binary not found"
    exit 1
fi
