#!/bin/bash
# Configure Maximus with GPU support using pip-installed libcudf (pip install cudf-cu12).
# Run from Maximus build directory: mkdir build && cd build && bash ../scripts/configure_with_gpu_pip_cudf.sh

set -e

# Auto-detect Python site-packages. Try user-site first (pip install --user),
# then system purelib (pip install as root). The first one that contains a real
# libcudf install wins so PIP_BASE matches where libcudf actually lives.
USER_SITE="$(python3 -m site --user-site 2>/dev/null || true)"
SYS_PURELIB="$(python3 -c 'import sysconfig; print(sysconfig.get_path("purelib"))' 2>/dev/null || true)"

PIP_BASE=""
CUDF_DIR=""
for base in "${USER_SITE}" "${SYS_PURELIB}"; do
    [ -z "${base}" ] && continue
    for candidate in \
        "${base}/libcudf/lib64/cmake/cudf" \
        "${base}/libcudf/lib/cmake/cudf" \
        "${base}/cudf/lib64/cmake/cudf" \
        "${base}/cudf/lib/cmake/cudf"; do
        if [ -f "${candidate}/cudf-config.cmake" ]; then
            PIP_BASE="${base}"
            CUDF_DIR="${candidate}"
            break 2
        fi
    done
done

if [ -z "${CUDF_DIR}" ]; then
    echo "[configure] ERROR: Could not find cudf-config.cmake."
    echo "[configure] Searched user-site (${USER_SITE}) and system purelib (${SYS_PURELIB})"
    echo "[configure] under libcudf/lib64/cmake/cudf, libcudf/lib/cmake/cudf, etc."
    echo "[configure] Make sure libcudf is installed: pip install libcudf-cu12"
    exit 1
fi
echo "[configure] Auto-detected PIP_BASE=${PIP_BASE}"
echo "[configure] Found cudf at: ${CUDF_DIR}"

# Helper: find cmake dir for a package, trying lib64 then lib
find_cmake_dir() {
    local pkg_path="$1"
    local pkg_name="$2"
    for d in "${pkg_path}/lib64/cmake/${pkg_name}" "${pkg_path}/lib/cmake/${pkg_name}"; do
        if [ -d "$d" ]; then echo "$d"; return; fi
    done
    echo "${pkg_path}/lib64/cmake/${pkg_name}"  # fallback
}

nvcomp_DIR="$(find_cmake_dir "${PIP_BASE}/nvidia/libnvcomp" nvcomp)"
rapids_logger_DIR="$(find_cmake_dir "${PIP_BASE}/rapids_logger" rapids_logger)"
nvtx3_DIR="$(find_cmake_dir "${PIP_BASE}/librmm" nvtx3)"
rmm_DIR="$(find_cmake_dir "${PIP_BASE}/librmm" rmm)"
fmt_DIR="$(find_cmake_dir "${PIP_BASE}/librmm" fmt)"
spdlog_DIR="$(find_cmake_dir "${PIP_BASE}/librmm" spdlog)"
cuco_DIR="$(find_cmake_dir "${PIP_BASE}/libcudf" cuco)"

# CCCL (Thrust/CUB/libcudacxx) - libcudf bundles it
CCCL_DIR=""
for candidate in \
    "${PIP_BASE}/libcudf/lib64/rapids/cmake/cccl" \
    "${PIP_BASE}/libcudf/lib/rapids/cmake/cccl" \
    "${PIP_BASE}/libcudf/include/libcudf/lib/rapids/cmake/cccl"; do
    if [ -d "${candidate}" ]; then
        CCCL_DIR="${candidate}"
        break
    fi
done
CCCL_DIR="${CCCL_DIR:-${PIP_BASE}/libcudf/lib64/rapids/cmake/cccl}"

# RMM headers
RMM_INCLUDE_DIR="${PIP_BASE}/librmm/include"

# Arrow and Taskflow: use env vars or default to $HOME/*_install
ARROW_PREFIX="${ARROW_PREFIX:-$HOME/arrow_install}"
TASKFLOW_PREFIX="${TASKFLOW_PREFIX:-$HOME/taskflow_install}"

# libkvikio is a transitive dep of libcudf (DT_NEEDED libkvikio.so). Without
# -rpath-link, ld can't resolve kvikio:: symbols when linking executables
# against libcudf, even though LD_LIBRARY_PATH covers the runtime case.
KVIKIO_LIB_DIR=""
for d in "${PIP_BASE}/libkvikio/lib64" "${PIP_BASE}/libkvikio/lib"; do
    if [ -f "$d/libkvikio.so" ]; then KVIKIO_LIB_DIR="$d"; break; fi
done
LINKER_FLAGS=""
if [ -n "${KVIKIO_LIB_DIR}" ]; then
    LINKER_FLAGS="-Wl,-rpath-link,${KVIKIO_LIB_DIR} -Wl,-rpath,${KVIKIO_LIB_DIR}"
    echo "[configure] libkvikio at: ${KVIKIO_LIB_DIR}"
fi

# CUDA arch: use env var, or detect from GPU, or default to 80
if [ -z "${CMAKE_CUDA_ARCHITECTURES}" ]; then
    # Try to detect GPU compute capability
    GPU_ARCH=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader,nounits 2>/dev/null | head -1 | tr -d '.')
    CMAKE_CUDA_ARCHITECTURES="${GPU_ARCH:-80}"
fi
export CMAKE_CUDA_ARCHITECTURES
echo "[configure] CUDA architectures: ${CMAKE_CUDA_ARCHITECTURES}"

cmake -DCMAKE_BUILD_TYPE=Release \
  -DMAXIMUS_WITH_TESTS=ON \
  -DMAXIMUS_WITH_GPU=ON \
  -DCMAKE_CUDA_ARCHITECTURES="${CMAKE_CUDA_ARCHITECTURES}" \
  -DCMAKE_PREFIX_PATH="${ARROW_PREFIX};${TASKFLOW_PREFIX};${PIP_BASE}/libcudf;${PIP_BASE}/librmm;${PIP_BASE}/libkvikio;${PIP_BASE}/nvidia/libnvcomp" \
  -DCMAKE_EXE_LINKER_FLAGS="${LINKER_FLAGS}" \
  -DCMAKE_SHARED_LINKER_FLAGS="${LINKER_FLAGS}" \
  -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=ON \
  -Dcudf_DIR="${CUDF_DIR}" \
  -Dnvcomp_DIR="${nvcomp_DIR}" \
  -Drapids_logger_DIR="${rapids_logger_DIR}" \
  -Dnvtx3_DIR="${nvtx3_DIR}" \
  -Drmm_DIR="${rmm_DIR}" \
  -Dfmt_DIR="${fmt_DIR}" \
  -Dspdlog_DIR="${spdlog_DIR}" \
  -Dcuco_DIR="${cuco_DIR}" \
  -DCCCL_DIR="${CCCL_DIR}" \
  -DRMM_INCLUDE_DIR="${RMM_INCLUDE_DIR}" \
  "$@"
