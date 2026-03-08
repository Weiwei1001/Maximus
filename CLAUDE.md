# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**gpu_db** is a GPU-accelerated SQL query engine with two components:
- **Maximus**: Standalone GPU query engine built on Apache Arrow Acero + NVIDIA cuDF
- **Sirius**: DuckDB GPU extension (git submodule at `sirius/`)

## Build Instructions

### Prerequisites
- CUDA 12.6+ (nvcc required; install via `apt install cuda-nvcc-12-6`)
- Apache Arrow 17.0.0 (build from source, install to `/root/arrow_install`)
- Taskflow (build from source, install to `/root/taskflow_install`)
- cuDF 24.12.0 + librmm 24.12.1 (`pip install libcudf-cu12==24.12.0 librmm-cu12==24.12.1`)
- NVIDIA A100 80GB (or equivalent)

### CRITICAL: CCCL Version Must Match cuDF

cuDF 24.12 was built with **CCCL 2.5.0**. Using a newer CCCL (e.g., 2.8.2 from `nvidia-cuda-cccl` pip package) causes ABI mismatch in `cuda::mr::async_resource_ref`, resulting in null function pointer crashes in `PinnedMemoryPool::do_allocate`.

**Fix**: Use the CCCL bundled with cuDF:
```
CCCL_DIR="/usr/local/lib/python3.10/dist-packages/libcudf/include/libcudf/lib/rapids/cmake/cccl"
```

### CMake Configuration

The build uses an initial cache file for reliable semicolon handling:

```bash
cd /workspace/gpu_db/build

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
```

The `initial_cache.cmake` file is at `build/initial_cache.cmake` and sets:
- `CMAKE_BUILD_TYPE=Release`
- `MAXIMUS_WITH_TESTS=ON`, `MAXIMUS_WITH_GPU=ON`, `MAXIMUS_WITH_BENCHMARKS=ON`
- `CMAKE_CUDA_ARCHITECTURES=80` (A100)
- `CMAKE_CUDA_COMPILER=/usr/local/cuda-12.6/bin/nvcc`
- All `*_DIR` paths for pip-installed dependencies

### Why Export + Cache?

cmake's `find_dependency()` inside `cudf-config.cmake` doesn't see cache variables set by `-C`. The exported env vars ensure transitive dependencies (rmm, nvcomp, etc.) are found.

### Running

```bash
export LD_LIBRARY_PATH="/root/arrow_install/lib:/usr/local/lib/python3.10/dist-packages/nvidia/libnvcomp/lib64:/usr/local/lib/python3.10/dist-packages/libkvikio/lib64:/usr/local/lib/python3.10/dist-packages/libcudf/lib64:/usr/local/lib/python3.10/dist-packages/librmm/lib64"

# Single query
./build/benchmarks/maxbench --benchmark=tpch --queries=q6 --device=gpu --storage_device=gpu --engines=maximus --n_reps=3 --path=tests/tpch/sf1

# Run tests
cd build && ctest
```

## Benchmark Suites

### Data Layout
```
tests/
├── tpch/sf{1,10,20}/       # 8 CSV tables each (lineitem, orders, etc.)
├── h2o/sf{1,2,3,4}/        # groupby.csv
└── clickbench/sf{1,10,20}/ # t.csv (sampled from hits.parquet)
```

### Timing Benchmarks
```bash
python3 scripts/benchmarks/run_timing.py \
  --maximus-dir /workspace/gpu_db \
  --data-dir /workspace/gpu_db/tests \
  --output-dir /workspace/gpu_db/results \
  --n-reps 3 --storage-device gpu \
  --benchmarks tpch h2o clickbench
```

### Metrics Benchmarks (GPU power/utilization sampling)
```bash
python3 scripts/benchmarks/run_metrics.py \
  --maximus-dir /workspace/gpu_db \
  --data-dir /workspace/gpu_db/tests \
  --output-dir /workspace/gpu_db/results \
  --n-reps 3 --storage-device gpu --sample-interval 50 \
  --benchmarks tpch h2o clickbench
```

### Query Counts
| Suite      | Queries | Scale Factors | Timing Tests | Succeeded | Failed |
|------------|---------|---------------|-------------|-----------|--------|
| TPC-H      | 22 (q1-q22) | sf1,sf10,sf20 | 66 | 61 | 5 OOM (sf20) |
| H2O        | 9 (q1-q7,q9,q10) | sf1,sf2,sf3,sf4 | 36 | 33 | 3 OOM (sf4) |
| ClickBench | 43 (q0-q42) | sf1,sf10,sf20 | 129 | 117 | 12 (4 unimplemented × 3SF) |
| **Total**  | **74** | | **231** | **211** | **20** |

Notes:
- sf1-sf10 use `--storage_device=gpu` (all data on GPU, fastest)
- sf20 (TPC-H) and sf4 (H2O) use `--storage_device=cpu` to avoid OOM
- ClickBench q18, q27, q28, q42 throw "Unsupported function call" / "Not implemented yet"
- Metrics benchmarks also completed (174 tests) with GPU power/utilization sampling

### Microbenchmarks (120 queries)
Fine-grained workload-typed queries across all three benchmark suites.

| Suite      | Queries | Workloads |
|------------|---------|-----------|
| H2O        | 35      | w1(scan/agg), w2(filter), w3(low-card GB), w4(high-card GB), w6(sort) |
| TPC-H      | 55      | w1-w4, w5a(2-3 table joins), w5b(5-6 table joins), w6(sort/limit) |
| ClickBench | 30      | w1-w4, w6 (includes 5 cross-benchmark queries from TPC-H) |
| **Total**  | **120** | All passed on GPU (sf1 H2O, sf1 TPC-H, sf10 ClickBench) |

```bash
# Run microbench via Maximus
./build/benchmarks/maxbench --benchmark=microbench_h2o --queries=w1_001 \
  --device=gpu --storage_device=gpu --engines=maximus --n_reps=5 \
  --path=tests/h2o/sf1

# Run DuckDB baseline
python3 scripts/benchmarks/run_microbench_duckdb.py \
  --data-dir tests --output-dir results --n-reps 5

# One-command: build + run all 120 microbench with timing & GPU metrics
bash scripts/benchmarks/run_all_microbench.sh --n-reps 5
bash scripts/benchmarks/run_all_microbench.sh --skip-build --n-reps 3
```

Source files: `src/maximus/microbench/microbench_{h2o,tpch,clickbench}.{hpp,cpp}`

### GPU Memory Limits (A100 80GB)
- TPC-H sf10: fits comfortably
- TPC-H sf20: OOM on most queries (data load ~5.3s consumes most memory)
- H2O sf4 q2+: OOM in batch timing mode; works in per-query metrics mode
- ClickBench sf20: fits (14.8GB CSV)

## Architecture

### Key Directories
- `src/maximus/` — Core engine library (libmaximus.so)
- `src/maximus/operators/gpu/cudf/` — cuDF GPU operator implementations (hash join, group by, filter, project)
- `src/maximus/gpu/` — GPU context, table management, CUDA API wrappers
- `benchmarks/` — maxbench binary and benchmark queries (SQL files in `benchmarks/queries/`)
- `scripts/benchmarks/` — Python timing/metrics runners and data generators
- `tests/` — GTest test suites and benchmark data

### Execution Flow
1. `maxbench` creates a `MaximusContext` (initializes RMM GPU pool, pinned memory pool, CUDA streams)
2. CSV data loaded → Arrow tables → optionally copied to GPU (`storage_device=gpu`)
3. SQL queries parsed → operator tree built (with optional operator fusion)
4. GPU operators execute via cuDF (hash join, group by, filter, project, sort)
5. Results exported back to Arrow tables

### Important Classes
- `MaximusContext` (`src/maximus/context.hpp`) — Central context: memory pools, CUDA streams, RMM pool
- `PinnedMemoryPool` (`src/maximus/memory_pool.hpp`) — Host-pinned memory for fast H2D transfers
- `GpuOperator` (`src/maximus/operators/gpu/`) — Base class for cuDF operators
- `Schema` / `DeviceTable` — Arrow-compatible table abstractions

### Config via Environment Variables
- `MAXIMUS_NUM_OUTER_THREADS` / `MAXIMUS_NUM_INNER_THREADS` — Thread counts
- `MAXIMUS_CSV_BATCH_SIZE` — CSV read batch size
- `MAXIMUS_MAX_PINNED_POOL_SIZE` — Pinned memory pool (default: 4GB)
- `MAXIMUS_OPERATORS_FUSION` — Enable/disable operator fusion

## Troubleshooting

### "Maximum pool size exceeded" (OOM)
The RMM pool allocates 50% of GPU memory initially and grows up to 90%. If datasets are too large, reduce scale factor or use `--storage_device=cpu` (slower but uses less GPU memory).

### Segfault in PinnedMemoryPool::do_allocate
CCCL version mismatch. Ensure `CCCL_DIR` points to cuDF's bundled CCCL 2.5.0, not the standalone pip package.

### cmake can't find cudf (case-sensitive)
Use `find_package(cudf REQUIRED)` (lowercase) — the config file is `cudf-config.cmake`, not `CUDFConfig.cmake`.

### libkvikio.so not found at runtime
Add `/usr/local/lib/python3.10/dist-packages/libkvikio/lib64` to `LD_LIBRARY_PATH`.

### setup.sh patches
`setup.sh` step 6.5 applies cuDF compatibility patches designed for specific versions. If using cuDF 24.12, skip patches or run `git checkout -- src/ tests/` to revert them.

## Results Location
All benchmark results are in `/workspace/gpu_db/results/`:
- `tpch_timing.csv` — TPC-H sf1/sf2/sf10 timing (gpu storage)
- `tpch_timing_sf20_cpu.csv` — TPC-H sf20 timing (cpu storage)
- `h2o_timing.csv` — H2O sf1/sf2/sf3 timing (gpu storage)
- `h2o_timing_sf4_cpu.csv` — H2O sf4 timing (cpu storage)
- `clickbench_timing_full.csv` — ClickBench 43q × 4SF timing (per-query)
- `*_metrics_samples.csv` — GPU metrics time-series (power, utilization, memory)
- `*_metrics_timings.csv` — Per-query timing from metrics runs
- `*_raw_*.txt` — Raw maxbench output per scale factor
