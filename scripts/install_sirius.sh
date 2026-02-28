#!/usr/bin/env bash
# =============================================================================
# install_sirius.sh
# 按照 Sirius 文档 "Install Manually" 全流程：安装依赖、CMake 升级、libcudf、
# 克隆/更新 Sirius、应用兼容性补丁、编译。
# 使用: ./install_sirius.sh [sirius_安装目录]
# 若不传参则克隆到当前目录下的 sirius/；若目录已存在则只做 submodule 更新与构建。
# =============================================================================

set -e

# 可配置变量
SIRIUS_DIR="${1:-$(pwd)/sirius}"
MINICONDA_DIR="${MINICONDA_DIR:-$(pwd)/miniconda3}"
SIRIUS_REPO_URL="${SIRIUS_REPO_URL:-https://github.com/sirius-db/sirius.git}"
MINICONDA_URL="${MINICONDA_URL:-https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh}"
# Ubuntu 代号，用于 Kitware APT（自动检测）
UBUNTU_CODENAME="${UBUNTU_CODENAME:-}"

# 若在已有 sirius 目录内执行，则 SIRIUS_DIR 设为当前目录
if [[ -f "setup_sirius.sh" && -f "CMakeLists.txt" ]]; then
  SIRIUS_DIR="$(pwd)"
fi

echo "[install_sirius] SIRIUS_DIR=${SIRIUS_DIR}"
echo "[install_sirius] MINICONDA_DIR=${MINICONDA_DIR}"

# -----------------------------------------------------------------------------
# 1. 安装 DuckDB / 系统依赖（文档 Option 1: Install Manually）
# -----------------------------------------------------------------------------
echo "[install_sirius] Step 1: Installing duckdb and system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
  git g++ cmake ninja-build libssl-dev \
  libconfig++-dev \
  libnuma-dev \
  ca-certificates wget

# -----------------------------------------------------------------------------
# 2. 升级 CMake 到 >= 3.30.4（Kitware APT）
# -----------------------------------------------------------------------------
CMAKE_VERSION=$(cmake --version 2>/dev/null | head -1 || true)
if command -v cmake &>/dev/null; then
  CMAKE_VER_NUM=$(cmake --version | head -1 | sed -n 's/.*version \([0-9]*\.[0-9]*\.[0-9]*\).*/\1/p' | awk -F. '{print $1*10000+$2*100+$3}')
  if [[ "${CMAKE_VER_NUM:-0}" -lt 30304 ]]; then
    echo "[install_sirius] Step 2: Upgrading CMake (current: ${CMAKE_VERSION})..."
    if [[ -z "$UBUNTU_CODENAME" ]]; then
      if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        UBUNTU_CODENAME="${VERSION_CODENAME:-noble}"
      else
        UBUNTU_CODENAME="noble"
      fi
    fi
    sudo apt-get install -y gnupg
    wget -qO - https://apt.kitware.com/keys/kitware-archive-latest.asc | \
      sudo gpg --dearmor -o /usr/share/keyrings/kitware-archive-keyring.gpg 2>/dev/null || true
    echo "deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ ${UBUNTU_CODENAME} main" | \
      sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
    sudo apt-get update -qq
    sudo apt-get install -y cmake
  else
    echo "[install_sirius] Step 2: CMake already >= 3.30.4, skip."
  fi
else
  echo "[install_sirius] Step 2: Installing CMake from Kitware..."
  [[ -z "$UBUNTU_CODENAME" ]] && . /etc/os-release 2>/dev/null; UBUNTU_CODENAME="${VERSION_CODENAME:-noble}"
  wget -qO - https://apt.kitware.com/keys/kitware-archive-latest.asc | \
    sudo gpg --dearmor -o /usr/share/keyrings/kitware-archive-keyring.gpg 2>/dev/null || true
  echo "deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ ${UBUNTU_CODENAME} main" | \
    sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
  sudo apt-get update -qq
  sudo apt-get install -y cmake
fi
echo "[install_sirius] CMake: $(cmake --version | head -1)"

# -----------------------------------------------------------------------------
# 3. 安装 Miniconda
# -----------------------------------------------------------------------------
if [[ ! -x "${MINICONDA_DIR}/bin/conda" ]]; then
  echo "[install_sirius] Step 3: Installing Miniconda to ${MINICONDA_DIR}..."
  wget -q "${MINICONDA_URL}" -O /tmp/miniconda.sh
  bash /tmp/miniconda.sh -b -p "${MINICONDA_DIR}"
  rm -f /tmp/miniconda.sh
else
  echo "[install_sirius] Step 3: Miniconda already at ${MINICONDA_DIR}, skip."
fi
export PATH="${MINICONDA_DIR}/bin:${PATH}"

# -----------------------------------------------------------------------------
# 4. 创建 conda 环境并安装 libcudf 26.04 + abseil
# -----------------------------------------------------------------------------
LIBCUDF_ENV_PREFIX="${MINICONDA_DIR}/envs/libcudf-env"
if [[ ! -d "${LIBCUDF_ENV_PREFIX}" ]]; then
  echo "[install_sirius] Step 4: Creating conda env libcudf-env and installing libcudf 26.04..."
  conda create --name libcudf-env -y
  conda run -n libcudf-env conda install -y -c rapidsai-nightly -c rapidsai -c conda-forge -c nvidia "libcudf=26.04*"
  conda run -n libcudf-env conda install -y -c conda-forge abseil-cpp
else
  echo "[install_sirius] Step 4: Conda env libcudf-env already exists, skip."
fi

# 若 conda 把 env 建到别处（例如 CONDA_ENVS_PATH），以实际路径为准
if [[ ! -f "${LIBCUDF_ENV_PREFIX}/lib/cmake/cudf/cudf-config.cmake" ]]; then
  ALTERNATE_ENV=$(conda info --envs 2>/dev/null | awk '$1=="libcudf-env"{print $NF}' | head -1)
  if [[ -n "$ALTERNATE_ENV" && -f "${ALTERNATE_ENV}/lib/cmake/cudf/cudf-config.cmake" ]]; then
    LIBCUDF_ENV_PREFIX="$ALTERNATE_ENV"
  fi
fi
if [[ ! -f "${LIBCUDF_ENV_PREFIX}/lib/cmake/cudf/cudf-config.cmake" ]]; then
  echo "[install_sirius] ERROR: libcudf not found in env. Check conda env libcudf-env." >&2
  exit 1
fi
echo "[install_sirius] LIBCUDF_ENV_PREFIX=${LIBCUDF_ENV_PREFIX}"

# -----------------------------------------------------------------------------
# 5. 克隆或更新 Sirius
# -----------------------------------------------------------------------------
if [[ ! -d "${SIRIUS_DIR}/.git" ]]; then
  echo "[install_sirius] Step 5: Cloning Sirius to ${SIRIUS_DIR}..."
  git clone --recurse-submodules "${SIRIUS_REPO_URL}" "${SIRIUS_DIR}"
else
  echo "[install_sirius] Step 5: Sirius repo exists, updating submodules..."
  (cd "${SIRIUS_DIR}" && git submodule update --init --recursive)
fi

# -----------------------------------------------------------------------------
# 6. 应用兼容性补丁（libconfig++、libcudf 26.04）
# -----------------------------------------------------------------------------
echo "[install_sirius] Step 6: Applying compatibility patches..."

# 6.1 Findlibconfig++.cmake（系统 libconfig++ 无 Config 时使用）
mkdir -p "${SIRIUS_DIR}/cmake"
cat > "${SIRIUS_DIR}/cmake/Findlibconfig++.cmake" << 'FindlibconfigEOF'
# Find libconfig++ using pkg-config (e.g. when installed via apt libconfig++-dev)
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(PC_LIBCONFIG++ QUIET libconfig++)
endif()

find_path(LIBCONFIG++_INCLUDE_DIR
  NAMES libconfig.h++
  HINTS ${PC_LIBCONFIG++_INCLUDE_DIRS}
  PATH_SUFFIXES include)
find_library(LIBCONFIG++_LIBRARY
  NAMES config++
  HINTS ${PC_LIBCONFIG++_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libconfig++
  REQUIRED_VARS LIBCONFIG++_LIBRARY LIBCONFIG++_INCLUDE_DIR)

if(libconfig++_FOUND)
  set(LIBCONFIG++_LIBRARIES ${LIBCONFIG++_LIBRARY})
  set(LIBCONFIG++_INCLUDE_DIRS ${LIBCONFIG++_INCLUDE_DIR})
endif()
FindlibconfigEOF

# 6.2 CMakeLists.txt：在 find_package(cudf) 前加入 CMAKE_MODULE_PATH
if ! grep -q 'list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")' "${SIRIUS_DIR}/CMakeLists.txt"; then
  sed -i '/find_package(cudf REQUIRED CONFIG)/i list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")' "${SIRIUS_DIR}/CMakeLists.txt"
fi

# 6.3 config_option.hpp：libconfig::Setting operator[] 接受 const char*
sed -i 's/current = \&(\*current)\[component\];/current = \&(*current)[component.c_str()];/g' \
  "${SIRIUS_DIR}/src/include/config_option.hpp" 2>/dev/null || true

# 6.4 cudf_aggregate.cu：26.04 使用 reduce 路径（无 distinct_count）
if grep -q 'CUDF_VERSION_NUM > 2504$' "${SIRIUS_DIR}/src/cuda/cudf/cudf_aggregate.cu" 2>/dev/null; then
  sed -i 's/#if CUDF_VERSION_NUM > 2504$/#if CUDF_VERSION_NUM > 2504 \&\& CUDF_VERSION_NUM < 2604/' \
    "${SIRIUS_DIR}/src/cuda/cudf/cudf_aggregate.cu"
fi

# -----------------------------------------------------------------------------
# 7. 构建 Sirius
# -----------------------------------------------------------------------------
echo "[install_sirius] Step 7: Building Sirius..."
export LIBCUDF_ENV_PREFIX="${LIBCUDF_ENV_PREFIX}"
export CMAKE_PREFIX_PATH="${LIBCUDF_ENV_PREFIX}"
export CONDA_PREFIX="${LIBCUDF_ENV_PREFIX}"

cd "${SIRIUS_DIR}"
# setup_sirius.sh 会设置 LDFLAGS 并更新 submodule
source setup_sirius.sh
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-$(nproc)}"
make

echo "[install_sirius] Done. DuckDB binary: ${SIRIUS_DIR}/build/release/duckdb"
echo "[install_sirius] Sirius extension: ${SIRIUS_DIR}/build/release/extension/sirius/sirius.duckdb_extension"
echo ""
echo "To run: ${SIRIUS_DIR}/build/release/duckdb <database>.duckdb"
echo "Then: call gpu_buffer_init(\"1 GB\", \"2 GB\");"
echo "      call gpu_processing(\"SELECT ...\");"
