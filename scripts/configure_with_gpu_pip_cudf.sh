#!/bin/bash
# Configure Maximus with GPU support using pip-installed libcudf (pip install cudf-cu12).
# Run from Maximus build directory: mkdir build && cd build && bash ../scripts/configure_with_gpu_pip_cudf.sh

set -e
PIP_BASE="/usr/local/lib/python3.12/dist-packages"
# If you use a venv, set PIP_BASE to your site-packages, e.g.:
# PIP_BASE="$(python3 -c 'import libcudf; import os; print(os.path.dirname(libcudf.__file__))')"
# PIP_BASE="${PIP_BASE%/libcudf}"

CUDF_DIR="${PIP_BASE}/libcudf/lib64/cmake/cudf"
nvcomp_DIR="${PIP_BASE}/nvidia/libnvcomp/lib64/cmake/nvcomp"
rapids_logger_DIR="${PIP_BASE}/rapids_logger/lib64/cmake/rapids_logger"
nvtx3_DIR="${PIP_BASE}/librmm/lib64/cmake/nvtx3"
rmm_DIR="${PIP_BASE}/librmm/lib64/cmake/rmm"
cuco_DIR="${PIP_BASE}/libcudf/lib64/cmake/cuco"
# CCCL (Thrust/CUB/libcudacxx) - libcudf bundles it
CCCL_DIR="${PIP_BASE}/libcudf/lib64/rapids/cmake/cccl"
# RMM headers (pip librmm doesn't set INTERFACE_INCLUDE_DIRECTORIES on rmm::rmm)
RMM_INCLUDE_DIR="${PIP_BASE}/librmm/include"

ARROW_PREFIX="/root/arrow_install"
TASKFLOW_PREFIX="/root/taskflow_install"

# CUDA arch: nvcc does not support NATIVE. Use 70 (Volta), 80 (Ampere), 90 (Ada), or e.g. "70;80" for multiple.
export CMAKE_CUDA_ARCHITECTURES="${CMAKE_CUDA_ARCHITECTURES:-70}"

cmake -DCMAKE_BUILD_TYPE=Release \
  -DMAXIMUS_WITH_TESTS=ON \
  -DMAXIMUS_WITH_GPU=ON \
  -DCMAKE_CUDA_ARCHITECTURES="${CMAKE_CUDA_ARCHITECTURES}" \
  -DCMAKE_PREFIX_PATH="${ARROW_PREFIX};${TASKFLOW_PREFIX};${PIP_BASE}/libcudf;${PIP_BASE}/librmm;${PIP_BASE}/nvidia/libnvcomp" \
  -DCUDF_DIR="${CUDF_DIR}" \
  -Dnvcomp_DIR="${nvcomp_DIR}" \
  -Drapids_logger_DIR="${rapids_logger_DIR}" \
  -Dnvtx3_DIR="${nvtx3_DIR}" \
  -Drmm_DIR="${rmm_DIR}" \
  -Dcuco_DIR="${cuco_DIR}" \
  -DCCCL_DIR="${CCCL_DIR}" \
  -DRMM_INCLUDE_DIR="${RMM_INCLUDE_DIR}" \
  "$@"
