#!/bin/bash
# Maximus GPU One-Command Deployment Script
# This script automates the entire build process for Maximus with GPU support

set -e  # Exit on error

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
MAXIMUS_REPO="${MAXIMUS_REPO:-https://github.com/Weiwei1001/Maximus.git}"
MAXIMUS_HOME="${MAXIMUS_HOME:-$HOME/Maximus}"
MINIFORGE_HOME="${MINIFORGE_HOME:-$HOME/miniforge3}"
CONDA_ENV="maximus_gpu"
NUM_JOBS=${NUM_JOBS:-$(nproc)}

echo -e "${GREEN}=== Maximus GPU Build Script ===${NC}"
echo "Installation directory: $MAXIMUS_HOME"
echo "Conda environment: $CONDA_ENV"
echo "Build jobs: $NUM_JOBS"
echo ""

# Function to print status
status() {
    echo -e "${GREEN}[*]${NC} $1"
}

error() {
    echo -e "${RED}[!]${NC} $1"
    exit 1
}

# Check prerequisites
status "Checking prerequisites..."

command -v git >/dev/null 2>&1 || error "git not found. Please install git."
command -v cmake >/dev/null 2>&1 || error "cmake not found. Please install cmake 3.17+."
command -v nvcc >/dev/null 2>&1 || error "NVIDIA CUDA not found. Please install CUDA 12.0+."

CMAKE_VERSION=$(cmake --version | head -1 | grep -oP '\d+\.\d+' | head -1)
CUDA_VERSION=$(nvcc --version | grep release | grep -oP '\d+\.\d+')
echo "  ✓ cmake $CMAKE_VERSION"
echo "  ✓ CUDA $CUDA_VERSION"
echo ""

# Install system dependencies
status "Installing system dependencies..."
if ! command -v ninja &> /dev/null; then
    sudo apt-get update
    sudo apt-get install -y \
        build-essential cmake ninja-build \
        libboost-all-dev libsnappy-dev libbrotli-dev \
        libthrift-dev thrift-compiler libre2-dev \
        git wget curl
else
    echo "  ✓ Dependencies already installed"
fi
echo ""

# Install/setup Miniforge and cuDF
status "Setting up Conda environment..."
if [ ! -f "$MINIFORGE_HOME/bin/conda" ]; then
    echo "  Installing Miniforge..."
    wget -q https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
    bash Miniforge3-Linux-x86_64.sh -b -p "$MINIFORGE_HOME"
    rm Miniforge3-Linux-x86_64.sh
else
    echo "  ✓ Miniforge already installed"
fi

# Create conda environment
if ! "$MINIFORGE_HOME/bin/conda" env list | grep -q "^$CONDA_ENV "; then
    echo "  Creating conda environment $CONDA_ENV..."
    "$MINIFORGE_HOME/bin/conda" create -y -n "$CONDA_ENV" \
        -c rapidsai -c conda-forge \
        libcudf=24.12 cuda-version=12 python=3.10 2>&1 | grep -v "Collecting\|Downloading"
else
    echo "  ✓ Conda environment already exists"
fi

# Activate conda
source "$MINIFORGE_HOME/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"
export CONDA_PREFIX=$MINIFORGE_HOME/envs/$CONDA_ENV
echo "  ✓ Conda environment activated"
echo ""

# Clone Maximus
status "Cloning Maximus repository..."
if [ ! -d "$MAXIMUS_HOME" ]; then
    echo "  Cloning $MAXIMUS_REPO..."
git clone "$MAXIMUS_REPO" "$MAXIMUS_HOME"
else
    echo "  ✓ Repository already cloned"
fi
cd "$MAXIMUS_HOME"
echo ""

# Build Arrow
status "Building Apache Arrow 17.0.0..."
if [ ! -f "/arrow_install/lib/libarrow.so" ]; then
    bash scripts/build_arrow.sh 2>&1 | tail -5
    echo "  ✓ Arrow built successfully"
else
    echo "  ✓ Arrow already built"
fi
echo ""

# Build Taskflow
status "Building Taskflow..."
if [ ! -f "$HOME/taskflow_install/lib/cmake/Taskflow/TaskflowConfig.cmake" ]; then
    bash scripts/build_taskflow.sh 2>&1 | tail -5
    echo "  ✓ Taskflow built successfully"
else
    echo "  ✓ Taskflow already built"
fi
echo ""

# Build Maximus
status "Building Maximus with GPU support..."
mkdir -p build
cd build

# Clean previous build
if [ -f "build.ninja" ] || [ -f "CMakeCache.txt" ]; then
    echo "  Cleaning previous build..."
    rm -rf *
fi

export CC=$(which gcc)
export CXX=$(which g++)
export CMAKE_PREFIX_PATH="/arrow_install:$CONDA_PREFIX:$HOME/taskflow_install"

echo "  Running cmake..."
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_STANDARD=20 \
      -DMAXIMUS_WITH_GPU=ON \
      -DMAXIMUS_WITH_TESTS=ON \
      -DMAXIMUS_WITH_BENCHMARKS=ON \
      -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH" \
      .. 2>&1 | grep -E "Found|Configuring done"

echo "  Compiling (using $NUM_JOBS jobs)..."
make -j "$NUM_JOBS" 2>&1 | tail -10

if [ -f "benchmarks/maxbench" ]; then
    echo "  ✓ Maximus built successfully"
else
    error "Build failed"
fi
echo ""

# Create environment setup script
status "Creating setup script..."
cat > setup_env.sh << 'EOF'
#!/bin/bash
export CONDA_PREFIX=$HOME/miniforge3/envs/maximus_gpu
export LD_LIBRARY_PATH="/arrow_install/lib:$CONDA_PREFIX/lib:$(pwd)/lib:$LD_LIBRARY_PATH"
export CMAKE_PREFIX_PATH="/arrow_install:$CONDA_PREFIX:$HOME/taskflow_install"
echo "Maximus GPU environment loaded"
echo "Build directory: $(pwd)"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
EOF
chmod +x setup_env.sh
echo "  ✓ Created setup_env.sh"
echo ""

# Summary
status "Installation complete!"
echo ""
echo "Quick start:"
echo "  cd $MAXIMUS_HOME/build"
echo "  source setup_env.sh"
echo "  ./benchmarks/maxbench --benchmark=tpch --queries=q1 --device=gpu --storage_device=cpu --engines=maximus --n_reps=1 --path=../tests/tpch/csv-0.01"
echo ""
echo "For all 22 TPC-H queries:"
echo "  ./benchmarks/maxbench --benchmark=tpch --queries=q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22 --device=gpu --storage_device=cpu --engines=maximus --n_reps=3 --path=../tests/tpch/csv-0.01"
echo ""
echo -e "${GREEN}✓ Ready to benchmark!${NC}"
