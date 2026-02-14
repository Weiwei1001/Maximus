# Maximus GPU Build and Installation Guide

## Quick Start (One-Command Deployment)

```bash
bash <(curl -s https://raw.githubusercontent.com/Weiwei1001/Maximus/main/scripts/deploy_gpu.sh)
```

## Prerequisites

- NVIDIA GPU with CUDA 12.0+
- Ubuntu 20.04+
- git, cmake 3.17+, gcc/g++ 11+
- NVIDIA CUDA Toolkit 12.0+

## Manual Installation

### 1. Install System Dependencies

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential cmake ninja-build \
    libboost-all-dev libsnappy-dev libbrotli-dev \
    libthrift-dev thrift-compiler libre2-dev \
    git wget curl
```

### 2. Install CUDA and cuDF via Conda

```bash
# Install Miniforge (if not already installed)
wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
bash Miniforge3-Linux-x86_64.sh -b -p $HOME/miniforge3

# Create cuDF environment
$HOME/miniforge3/bin/conda create -y -n maximus_gpu -c rapidsai -c conda-forge \
    libcudf=24.12 cuda-version=12 python=3.10

# Activate environment
source $HOME/miniforge3/etc/profile.d/conda.sh
conda activate maximus_gpu
```

### 3. Build Dependencies from Source

```bash
# Clone Maximus
git clone https://github.com/YOUR_USERNAME/Maximus.git
cd Maximus

# Set up build directory for dependencies
export DEP_BUILD_DIR=$HOME/maximus_deps
mkdir -p $DEP_BUILD_DIR

# Build Arrow 17.0.0
bash scripts/build_arrow.sh
# Arrow installs to /arrow_install by default

# Build Taskflow
bash scripts/build_taskflow.sh
# Taskflow installs to $HOME/taskflow_install
```

### 4. Build Maximus with GPU Support

```bash
cd ~/Maximus
mkdir -p build && cd build

export CC=$(which gcc)
export CXX=$(which g++)
export CUDA_TOOLKIT_PATH=/usr/local/cuda

cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_STANDARD=20 \
      -DMAXIMUS_WITH_GPU=ON \
      -DMAXIMUS_WITH_TESTS=ON \
      -DMAXIMUS_WITH_BENCHMARKS=ON \
      -DCMAKE_PREFIX_PATH="/arrow_install;$CONDA_PREFIX;$HOME/taskflow_install" \
      ..

make -j $(nproc)
```

## Testing

### Run TPC-H Benchmark

```bash
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:/arrow_install/lib:$(pwd)/lib:$LD_LIBRARY_PATH"

# Single query test
./benchmarks/maxbench --benchmark=tpch --queries=q1 \
    --device=gpu --storage_device=cpu \
    --engines=maximus --n_reps=1 \
    --path=../tests/tpch/csv-0.01

# All 22 queries (3 repetitions)
./benchmarks/maxbench --benchmark=tpch \
    --queries=q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22 \
    --device=gpu --storage_device=cpu \
    --engines=maximus --n_reps=3 \
    --path=../tests/tpch/csv-0.01
```

## Environment Setup (for future sessions)

```bash
# Add to ~/.bashrc or ~/.zshrc
export CONDA_PREFIX=$HOME/miniforge3/envs/maximus_gpu
export LD_LIBRARY_PATH="/arrow_install/lib:$CONDA_PREFIX/lib:$LD_LIBRARY_PATH"
alias maximus-gpu='cd ~/Maximus/build && source setup_env.sh'
```

## Troubleshooting

### CUDA/cuDF Version Mismatch
```bash
# Check CUDA version
nvcc --version

# Check cuDF compatibility
conda list -n maximus_gpu | grep cudf
```

### Arrow or Taskflow Build Failures
- Ensure cmake 3.17+ is installed: `cmake --version`
- Check ninja availability: `ninja --version`
- Try building with fewer jobs: `ninja -j 4`

### Runtime Library Errors
```bash
# Set LD_LIBRARY_PATH before running
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:/arrow_install/lib:$(pwd)/lib:$LD_LIBRARY_PATH"
```

## Known Issues Fixed in This Version

1. **cuDF 24.12 API Migration**: Updated from deprecated `cudf::from_arrow(arrow::Table)` to C Data Interface
2. **Join Operations**: Fixed function pointer calls to include explicit stream and memory resource parameters
3. **Schema Nullability**: Fixed mismatch between cuDF "not null" annotations and Arrow nullable schemas
4. **Taskflow Compatibility**: Set C++20 standard for Taskflow v3.11

## Results

All 22 TPC-H queries pass on NVIDIA A100 GPU (SF=0.01):
- Avg query time: 3-5ms (after warmup)
- First run includes CUDA kernel compilation overhead
- Data loading: ~40ms

See `BENCHMARKS.md` for detailed performance numbers.
