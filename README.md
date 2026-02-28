# Maximus: GPU-Accelerated SQL Benchmark Suite

A comprehensive GPU SQL benchmarking platform comparing **Maximus** (standalone GPU query engine) and **Sirius** (DuckDB GPU extension), developed in the Systems Group at ETH Zurich.

## Overview

This repository contains:

- **Maximus Engine** — A modular, accelerated query engine integrating Apache Acero (CPU) and cuDF (GPU) operators through operator-level integration. Push-based execution with columnar Arrow storage.
- **Sirius Integration** — Setup, build, and benchmark scripts for the DuckDB GPU extension that offloads queries to GPU via `gpu_processing()` calls.
- **Three Benchmark Suites** — TPC-H (OLAP), H2O groupby (aggregation), ClickBench (web analytics).
- **One-Click Deployment** — `./setup.sh` handles everything from dependencies to smoke testing on a new machine.

## Quick Start

```bash
# Clone the repository (with Sirius submodule)
git clone --recurse-submodules https://github.com/Weiwei1001/gpu_db.git
cd gpu_db

# One-click setup (Maximus + Sirius + data generation)
./setup.sh

# Or Maximus only
./setup.sh --maximus-only

# Source the runtime environment
source setup_env.sh

# Run benchmarks
python benchmarks/scripts/run_all.py                    # Both engines
python benchmarks/scripts/run_all.py --engine maximus   # Maximus only
python benchmarks/scripts/run_all.py --engine sirius    # Sirius only
```

## Benchmark Results (RTX 5090, 32GB VRAM)

### TPC-H Performance (ms per query)

| Query | Sirius SF1 | Maximus SF1 | Sirius SF2 | Maximus SF2 | Sirius SF10 | Maximus SF10 |
|-------|-----------|------------|-----------|------------|------------|-------------|
| q1 | 17.69 | 18 | 19.15 | 36 | 70.65 | 148 |
| q2 | 36.41 | 5 | 31.56 | 7 | 35.73 | 26 |
| q3 | 9.67 | 13 | 10.88 | 25 | 22.81 | 100 |
| q4 | 9.44 | 9 | 7.79 | 16 | 22.35 | 65 |
| q5 | 17.99 | 15 | 16.10 | 40 | 27.27 | 194 |
| q6 | 6.78 | 10 | 5.41 | 19 | 15.48 | — |
| q7 | 23.05 | 16 | 20.74 | 29 | 33.60 | — |
| q8 | 24.27 | 17 | 23.07 | 48 | 39.57 | — |
| q9 | 35.90 | 21 | 20.66 | 60 | 42.96 | — |
| q10 | 22.35 | 14 | 17.95 | 28 | 37.29 | — |
| q12 | 11.26 | 58 | 10.73 | 114 | 26.35 | — |
| q13 | 11.20 | 21 | 10.17 | 40 | 30.71 | — |
| q14 | 8.22 | 49 | 9.25 | 99 | 17.02 | — |
| q15 | 8.64 | 48 | 9.13 | 95 | 17.36 | — |
| q16 | 27.30 | 30 | 26.50 | 37 | 45.95 | — |
| q17 | 17.16 | 52 | 25.84 | 156 | 29.73 | — |
| q18 | 29.24 | 61 | 14.64 | 177 | 39.33 | — |
| q19 | 21.79 | 58 | 14.61 | 170 | 39.95 | — |
| q20 | 18.10 | 59 | 18.20 | 114 | 29.88 | — |
| q21 | 43.12 | 93 | 26.48 | 185 | 62.63 | — |
| q22 | 8.47 | 17 | 9.79 | 26 | 12.99 | — |

**Key observations:**
- Sirius shows better overhead amortization (100 reps with shared init vs Maximus 50 reps)
- Maximus excels at metadata-heavy queries (q2) but is slower on scan-heavy queries (q12, q14)
- Both engines complete all 22 TPC-H queries at SF1 and SF2

### H2O Groupby Performance (ms per query, Sirius)

| Query | 1gb | 2gb | 3gb | 4gb |
|-------|-----|-----|-----|-----|
| q1 | 129 | 892 | 1,035 | 409 |
| q2 | 764 | 323 | 1,228 | 520 |
| q3 | 414 | 713 | 2,616 | 1,171 |
| q4 | 1,066 | 240 | 1,203 | 500 |
| q5 | 171 | 239 | 332 | 526 |
| q6 | 270 | 242 | 363 | 449 |
| q7 | 343 | 540 | 1,706 | 969 |
| q9 | 852 | 303 | 1,447 | 723 |
| q10 | 1,916 | 4,515 (FB) | 10,438 (FB) | 12,286 (FB) |

*FB = FALLBACK (fell back to CPU execution)*

### Completion Status

| Benchmark | Scale Factor | Sirius Timing | Sirius Metrics | Maximus Timing | Maximus Metrics |
|-----------|-------------|---------------|----------------|----------------|-----------------|
| TPC-H | SF 1 | 25/25 OK | Done | 22/22 OK | Done |
| TPC-H | SF 2 | 25/25 OK | Done | 22/22 OK | Done |
| TPC-H | SF 10 | 25/25 OK | Done | 20/22 (q21-22 crash) | Done |
| TPC-H | SF 20 | 24/25 OK, 1 FB | Done | In progress | — |
| H2O | 1-4gb | 40/40 (3 FB) | Done | 36/36 OK | Done |
| ClickBench | SF 1-2 | — | — | 78/78 OK | Done |
| ClickBench | SF 10-100 | 168/172 (12 FB) | Done | — | — |

### Key Findings

- **GPU Power**: P1 idle ~67W, compute ~250-350W, P8 deep idle ~13W
- **VRAM Usage**: TPC-H SF10 ~3-5GB, ClickBench SF100 ~7-14GB
- **RMM Pool**: Maximus uses 20% of free VRAM (~6.4 GiB on RTX 5090)
- **Memory Allocation**: 10MB-10GB allocation has negligible effect on idle power (~67-70W)
- **Energy Measurement**: pynvml vs Zeus within 1% for compute-heavy queries

## Architecture

### Maximus

```
SQL Query → Parser → Logical Plan → Physical Plan → Execution Engine
                                                          ↓
                                          ┌───────────────┴───────────────┐
                                          │ Apache Acero (CPU)            │
                                          │ cuDF operators (GPU)          │
                                          │ Push-based execution          │
                                          │ Arrow columnar storage        │
                                          │ RMM GPU memory pool           │
                                          └───────────────────────────────┘
```

- **Operator-level integration**: Each operator can independently run on CPU (Acero) or GPU (cuDF)
- **Storage modes**: `-s cpu` (transfer data per query) vs `-s gpu` (pre-load to VRAM)
- **Memory**: RMM pool with 20% initial, unlimited max, 4 GiB pinned host

### Sirius

```
DuckDB CLI → gpu_buffer_init(VRAM, host) → gpu_processing("SQL") → Results
                     ↓                              ↓
              GPU buffer allocation          Query offloading to GPU
              (cascading memory)            via cuDF backend
```

- **DuckDB extension**: Transparent GPU acceleration for standard SQL
- **Three-pass methodology**: Warm up GPU, stabilize, then measure
- **Batch processing**: 10 queries per batch to manage GPU memory

## Manual Installation

### Prerequisites

| Requirement | Version |
|------------|---------|
| Ubuntu | 22.04 or 24.04 |
| NVIDIA GPU | >= 24GB VRAM (32GB recommended) |
| CUDA Toolkit | >= 12.0 |
| CMake | >= 3.17 (Maximus), >= 3.30.4 (Sirius) |
| GCC/G++ | >= 11 |
| RAM | >= 128GB |
| Disk | >= 300GB |

### Build Maximus Only

```bash
# 1. Build dependencies
bash scripts/build_arrow.sh       # Apache Arrow 17.0.0
bash scripts/build_taskflow.sh    # Taskflow

# 2. Install cuDF
pip install cudf-cu12 libcudf-cu12
# OR: conda install -c rapidsai -c conda-forge libcudf=24.12 cuda-version=12

# 3. Build Maximus
mkdir build && cd build
bash ../scripts/configure_with_gpu_pip_cudf.sh -DMAXIMUS_WITH_BENCHMARKS=ON ..
make -j$(nproc)
```

### Install Sirius

```bash
# One-command Sirius installation
bash scripts/install_sirius.sh [install_directory]

# This handles:
# - System dependencies
# - CMake upgrade to >= 3.30.4
# - Miniconda + libcudf 26.04 (conda)
# - Clone + patch + build Sirius
```

## Data Generation

```bash
# Generate all benchmark data (TPC-H, H2O, ClickBench)
bash benchmarks/data/generate_all.sh

# Or generate individually:
python benchmarks/data/generate_tpch.py -o tests/tpch_duckdb -sf 1 2 10 20
python benchmarks/data/generate_h2o.py --output-dir tests/h2o --format both 1gb 2gb 3gb 4gb
python benchmarks/data/generate_clickbench.py --output-dir tests/clickbench --format both --scales 1 2 10 20
```

### Data Requirements

| Benchmark | Scale Factor | Approx Size | RAM Needed |
|-----------|-------------|-------------|------------|
| TPC-H | SF 1 | ~1 GB | ~8 GB |
| TPC-H | SF 10 | ~10 GB | ~64 GB |
| TPC-H | SF 20 | ~20 GB | ~145 GB |
| H2O | 1gb | ~1 GB | ~8 GB |
| H2O | 4gb | ~4 GB | ~32 GB |
| ClickBench | Full | ~14 GB (parquet) | ~32 GB |

## Running Benchmarks

### Combined Runner (Recommended)

```bash
source setup_env.sh

# Run both engines on all benchmarks
python benchmarks/scripts/run_all.py

# Maximus only, specific benchmarks
python benchmarks/scripts/run_all.py --engine maximus --benchmarks tpch h2o

# Sirius only
python benchmarks/scripts/run_all.py --engine sirius --benchmarks tpch
```

### Individual Runners

```bash
# Maximus timing benchmark
python benchmarks/scripts/run_maximus_benchmark.py tpch h2o clickbench

# Maximus GPU metrics
python benchmarks/scripts/run_maximus_metrics.py tpch --scale-factors 1 2

# Sirius timing benchmark
python benchmarks/scripts/run_sirius_benchmark.py tpch h2o

# Direct maxbench usage
./build/benchmarks/maxbench --benchmark tpch -q q1,q2,q3 -d gpu -r 3 \
    --path tests/tpch/csv-1 -s gpu --engines maximus
```

### GPU Metrics Collection

GPU metrics are collected using `nvidia-smi` sampling at ~50ms intervals during benchmark execution. Metrics include:
- Power draw (W)
- GPU utilization (%)
- Memory utilization (%)
- GPU memory used (MB)
- PCIe throughput (MB/s)

```bash
python benchmarks/scripts/run_maximus_metrics.py tpch --scale-factors 1 2 10
```

## Analysis and Visualization

```bash
# Compare Sirius vs Maximus results
python benchmarks/scripts/compare_results.py \
    --sirius results/sirius_benchmark.csv \
    --maximus results/maximus_benchmark.csv

# Generate visualization plots (10 charts)
python benchmarks/scripts/plot_metrics.py --results-dir results/

# Plots generated:
#   tpch_timing_by_sf.png       - TPC-H timing grouped by scale factor
#   h2o_timing_by_sf.png        - H2O timing grouped by scale factor
#   clickbench_timing_by_sf.png - ClickBench timing grouped by scale factor
#   tpch_gpu_memory.png         - GPU memory usage per TPC-H query
#   h2o_gpu_memory.png          - GPU memory usage per H2O query
#   clickbench_gpu_memory.png   - GPU memory usage per ClickBench query
#   gpu_power_by_benchmark.png  - Power consumption box plots
#   tpch_time_vs_memory.png     - Execution time vs GPU memory scatter
#   timing_overview_heatmap.png - Full timing heatmap across all benchmarks
#   tpch_scaling.png            - Query scaling with data size
```

## GPU Configuration

### RMM Memory Pool (Maximus)

Maximus uses RAPIDS Memory Manager (RMM) for GPU memory allocation:
- **Initial pool**: 20% of free VRAM (~6.4 GiB on RTX 5090 with 32GB)
- **Maximum pool**: Unlimited (grows on demand)
- **Pinned host memory**: 4 GiB

### Storage Modes (Maximus)

| Flag | Mode | Description |
|------|------|-------------|
| `-s cpu` | CPU storage | Transfer data from host to GPU per query (includes PCIe overhead) |
| `-s gpu` | GPU storage | Pre-load all data to VRAM (pure GPU compute time) |

### GPU Buffer (Sirius)

```sql
-- Initialize GPU buffer: gpu_buffer_init(VRAM_size, host_size)
call gpu_buffer_init("20 GB", "10 GB");

-- Execute query on GPU
call gpu_processing("SELECT ... FROM ...");
```

### Estimated Maximum Scale Factors (RTX 5090, 32GB VRAM)

| Benchmark | Maximus (GPU storage) | Sirius |
|-----------|----------------------|--------|
| TPC-H | SF 10-15 | SF 30-40 |
| H2O | 10-15 GB | 10-15 GB |
| ClickBench | SF 3-5 | SF 50-100 |

### Known GPU Limitations

- `minute()` — not supported on cuDF GPU backend (ClickBench q18, q42)
- `utf8_length()` / `STRLEN()` — not supported (ClickBench q27)
- `REGEXP_REPLACE()` — not supported (ClickBench q28)
- H2O q8 — not implemented in Maximus
- TPC-H q21-q22 at SF10 — crash on Maximus (GPU OOM)

## Project Structure

```
Maximus/
├── README.md                           # This file
├── setup.sh                            # One-click deployment script
├── setup_env.sh                        # Runtime environment (generated)
├── CMakeLists.txt                      # Build system
├── .gitignore
│
├── sirius/                             # Sirius DuckDB GPU extension (git submodule)
│   ├── src/                            #   GPU acceleration source code
│   ├── duckdb/                         #   DuckDB submodule
│   ├── CMakeLists.txt
│   └── Makefile
│
├── src/maximus/                        # Maximus engine source code
│   ├── context.hpp/.cpp                #   Execution context + RMM config
│   ├── database.hpp/.cpp               #   Table management
│   ├── operators/                      #   Query operators (Acero + cuDF)
│   ├── types/                          #   Data type system
│   ├── gpu/                            #   GPU-specific code
│   ├── exec/                           #   Execution engine
│   └── io/, frontend/, utils/
│
├── scripts/
│   ├── install_sirius.sh               # Sirius full installation
│   ├── deploy_gpu.sh                   # Maximus GPU deployment
│   ├── build_arrow.sh                  # Build Apache Arrow 17.0.0
│   ├── build_taskflow.sh               # Build Taskflow
│   ├── configure_with_gpu_pip_cudf.sh  # CMake config for pip cuDF
│   └── test_gpu.sh                     # GPU testing script
│
├── benchmarks/
│   ├── maxbench.cpp                    # Benchmark binary source
│   ├── scripts/
│   │   ├── run_all.py                  # Master runner (both engines)
│   │   ├── run_maximus_benchmark.py    # Maximus timing benchmark
│   │   ├── run_maximus_metrics.py      # Maximus GPU metrics
│   │   ├── run_sirius_benchmark.py     # Sirius timing benchmark
│   │   ├── compare_results.py          # Cross-engine comparison
│   │   ├── plot_metrics.py             # Visualization (10 charts)
│   │   ├── generate_sirius_sql.py      # SQL generation for Sirius
│   │   └── setup_sirius.sh            # Sirius benchmark setup
│   └── data/
│       ├── generate_tpch.py            # TPC-H data generation
│       ├── generate_h2o.py             # H2O data generation
│       ├── generate_clickbench.py      # ClickBench data generation
│       └── generate_all.sh            # Generate all data
│
├── results/                            # Benchmark results
│   ├── README.md                       # Results documentation
│   ├── *.csv                           # Timing and metrics data
│   └── plots/                          # Visualization PNGs
│
├── tests/                              # Unit tests
│   ├── tpch/, h2o/, clickbench/        # Test data (generated, not in git)
│   └── *.cpp                           # C++ test files
│
├── docs/
│   ├── BENCHMARKS.md                   # Benchmark methodology details
│   ├── INSTALL_GPU.md                  # GPU installation guide
│   ├── NEW_MACHINE.md                  # New machine setup guide
│   └── GPU_NOTES.md                    # GPU configuration notes
│
└── third_party/                        # Third-party dependencies
    ├── cxxopts/                        # Command-line parsing
    └── sqlparser/                      # SQL parser
```

## Testing

```bash
# Run all unit tests
cd build && ctest --verbose

# Run specific test suites
./build/tests/test_acero        # CPU operator tests
./build/tests/test_cuda         # GPU operator tests
./build/tests/test_sql          # SQL parsing tests
./build/tests/test_tpch         # TPC-H query tests
./build/tests/test_tpch_gpu     # TPC-H GPU tests
```

## Troubleshooting

### cuDF not found during build

```bash
# If using pip:
pip install cudf-cu12 libcudf-cu12
bash scripts/configure_with_gpu_pip_cudf.sh ..

# If using conda:
conda install -c rapidsai -c conda-forge libcudf=24.12 cuda-version=12
```

### GPU Out of Memory

- Reduce scale factor
- Use `-s cpu` instead of `-s gpu` (adds PCIe transfer overhead but uses less VRAM)
- Maximus RMM pool: adjust in `src/maximus/context.cpp`
- Sirius: reduce `gpu_buffer_init` sizes

### Sirius build fails

Common issues:
1. **CMake too old**: Need >= 3.30.4. The `install_sirius.sh` script handles this.
2. **libconfig++ not found**: Install with `sudo apt install libconfig++-dev`
3. **cuDF version mismatch**: Sirius needs cuDF 26.04 via conda (not pip)

### LD_LIBRARY_PATH errors

```bash
source setup_env.sh
# Or manually:
export LD_LIBRARY_PATH="$HOME/arrow_install/lib:$LD_LIBRARY_PATH"
```

### Segfault on large scale factors

- TPC-H SF20 requires ~145GB RAM
- Ensure sufficient system memory
- Monitor with `nvidia-smi` during benchmark runs

## License

Apache License 2.0 — See individual source files for details.

## Acknowledgments

- [Apache Arrow](https://arrow.apache.org/) 17.0.0 — Columnar data format
- [NVIDIA cuDF](https://github.com/rapidsai/cudf) — GPU DataFrames
- [Taskflow](https://github.com/taskflow/taskflow) — Task-parallel programming
- [DuckDB](https://duckdb.org/) — In-process SQL OLAP database
- [Sirius](https://github.com/sirius-db/sirius) — DuckDB GPU extension
