# gpu_db: GPU-Accelerated SQL Query Engine

Comparing **Maximus** (standalone GPU query engine, Apache Arrow Acero + cuDF) and **Sirius** (DuckDB GPU extension) across TPC-H, H2O, and ClickBench.

## Quick Start

```bash
git clone --recurse-submodules https://github.com/Weiwei1001/gpu_db.git
cd gpu_db
./setup.sh            # one-click: deps + build + data
source setup_env.sh

# Run standard benchmarks
python3 scripts/benchmarks/run_timing.py \
    --maximus-dir /workspace/gpu_db --data-dir /workspace/gpu_db/tests \
    --output-dir /workspace/gpu_db/results --n-reps 3 --storage-device gpu

# Run all 120 microbench queries (one command)
bash scripts/benchmarks/run_all_microbench.sh --n-reps 5
```

## Build

### Prerequisites

- CUDA 12.6+ (`apt install cuda-nvcc-12-6`)
- Apache Arrow 17.0.0 (build from source, install to `/root/arrow_install`)
- Taskflow (build from source, install to `/root/taskflow_install`)
- cuDF 24.12.0 + librmm 24.12.1 (`pip install libcudf-cu12==24.12.0 librmm-cu12==24.12.1`)
- NVIDIA A100 80GB (or equivalent)

### CRITICAL: CCCL Version Must Match cuDF

cuDF 24.12 was built with **CCCL 2.5.0**. Using a newer CCCL (e.g., 2.8.2 from `nvidia-cuda-cccl` pip) causes ABI mismatch in `cuda::mr::async_resource_ref`, resulting in segfault in `PinnedMemoryPool::do_allocate`.

**Fix**: Use the CCCL bundled with cuDF:
```
CCCL_DIR="/usr/local/lib/python3.10/dist-packages/libcudf/include/libcudf/lib/rapids/cmake/cccl"
```

### CMake Configuration

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

The `initial_cache.cmake` sets: `CMAKE_BUILD_TYPE=Release`, `MAXIMUS_WITH_GPU=ON`, `MAXIMUS_WITH_BENCHMARKS=ON`, `CMAKE_CUDA_ARCHITECTURES=80`.

**Why Export + Cache?** cmake's `find_dependency()` inside `cudf-config.cmake` doesn't see cache variables set by `-C`. The exported env vars ensure transitive dependencies (rmm, nvcomp, etc.) are found.

### Running

```bash
export LD_LIBRARY_PATH="/root/arrow_install/lib:/usr/local/lib/python3.10/dist-packages/nvidia/libnvcomp/lib64:/usr/local/lib/python3.10/dist-packages/libkvikio/lib64:/usr/local/lib/python3.10/dist-packages/libcudf/lib64:/usr/local/lib/python3.10/dist-packages/librmm/lib64"

# Single query
./build/benchmarks/maxbench --benchmark=tpch --queries=q6 --device=gpu \
    --storage_device=gpu --engines=maximus --n_reps=3 --path=tests/tpch/sf1

# Run tests
cd build && ctest
```

## Measurement Methodology

### Timing

**Maximus** uses the `maxbench` C++ binary. Each query is bracketed by CUDA stream barriers and timed with `std::chrono::high_resolution_clock`:

```
barrier()  →  start_time  →  execute()  →  barrier()  →  end_time
```

Data is pre-loaded with `--storage_device=gpu` and stays in VRAM across repetitions — timings exclude PCIe transfer.

### Energy & Power

GPU power is sampled via `nvidia-smi` at **50ms intervals** during query execution. Steady-state compute region is detected via GPU utilization threshold, and per-query energy is computed as:

```
E_query = P_steady × t_query
```

## Benchmark Suites

### Standard Benchmarks (74 queries)

| Suite | Queries | Scale Factors | Pass Rate |
|-------|---------|---------------|-----------|
| TPC-H | 22 (q1-q22) | sf1, sf10, sf20 | 61/66 (92.4%) |
| H2O | 9 (q1-q7, q9, q10) | sf1, sf2, sf3, sf4 | 33/36 (91.7%) |
| ClickBench | 43 (q0-q42) | sf1, sf10, sf20 | 117/129 (90.7%) |
| **Total** | **74** | | **211/231 (91.3%)** |

**Failures:**
- TPC-H sf20: q17, q18, q19, q21 — GPU OOM on complex multi-way joins
- H2O sf4: q3, q7, q10 — GPU OOM on high-memory group-by queries
- ClickBench: q18, q27, q28, q42 — unsupported functions (minute, utf8_length, not implemented)

### Microbenchmarks (120 queries)

Fine-grained, workload-typed queries for detailed performance characterization.

| Suite | Queries | Workloads |
|-------|---------|-----------|
| H2O | 35 | w1(scan/agg), w2(filter), w3(low-card groupby), w4(high-card groupby), w6(sort) |
| TPC-H | 55 | w1-w4, w5a(2-3 table joins), w5b(5-6 table joins), w6(sort/limit) |
| ClickBench | 30 | w1-w4, w6 (includes 5 cross-benchmark queries) |
| **Total** | **120** | All passed on GPU at default scale factors |

```bash
# One-command: build + run all 120 microbench with timing & GPU metrics
bash scripts/benchmarks/run_all_microbench.sh --n-reps 5

# Skip build
bash scripts/benchmarks/run_all_microbench.sh --skip-build --n-reps 3

# Individual query
./build/benchmarks/maxbench --benchmark=microbench_h2o --queries=w1_001 \
    --device=gpu --storage_device=gpu --engines=maximus --n_reps=5 \
    --path=tests/h2o/sf1

# DuckDB baseline
python3 scripts/benchmarks/run_microbench_duckdb.py \
    --data-dir tests --output-dir results --n-reps 5
```

### TPC-H Results (ms, GPU storage, min of 3 reps)

| Query | sf1 | sf10 | sf20 | Query | sf1 | sf10 | sf20 |
|-------|-----|------|------|-------|-----|------|------|
| q1 | 5 | 39 | 428 | q12 | 3 | 19 | 1243 |
| q2 | 4 | 6 | 55 | q13 | 21 | 201 | 639 |
| q3 | 2 | 17 | 267 | q14 | 3 | 12 | 891 |
| q4 | 2 | 14 | 171 | q15 | 3 | 11 | 862 |
| q5 | 4 | 32 | 504 | q16 | 13 | 54 | 375 |
| q6 | 1 | 7 | 192 | q17 | 5 | 19 | OOM |
| q7 | 3 | 18 | 313 | q18 | 2 | 16 | OOM |
| q8 | 3 | 22 | 1482 | q19 | 23 | 226 | OOM |
| q9 | 6 | 45 | 3485 | q20 | 6 | 21 | 1152 |
| q10 | 3 | 13 | 260 | q21 | 53 | 473 | OOM |
| q11 | 3 | 20 | 276 | q22 | 3 | 7 | 427 |

### GPU Memory Limits (A100 80GB)

- sf1-sf10: fit comfortably with `--storage_device=gpu`
- sf20 (TPC-H): OOM on q17/q18/q19/q21 (use `--storage_device=cpu`)
- sf4 (H2O): OOM on q3/q7/q10 (use `--storage_device=cpu`)
- sf20 (ClickBench): fits (14.8GB CSV)

## Project Structure

```
gpu_db/
├── setup.sh                           # One-click deployment
├── sirius/                            # DuckDB GPU extension (git submodule)
├── src/maximus/                       # Maximus engine source
│   ├── operators/gpu/cudf/            #   cuDF operator implementations
│   ├── gpu/                           #   GPU context, CUDA wrappers
│   └── microbench/                    #   120 microbench query plans (C++)
├── benchmarks/
│   ├── maxbench.cpp                   # C++ benchmark binary
│   └── utils.hpp                      # Benchmark dispatch (standard + microbench)
├── microbench/                        # 123 SQL files (DuckDB baseline)
│   ├── h2o/                           #   35 H2O queries
│   ├── tpch/                          #   55 TPC-H queries
│   └── clickbench/                    #   25 ClickBench queries
├── scripts/benchmarks/
│   ├── run_all_microbench.sh          # One-command microbench runner
│   ├── run_timing.py                  # Standard timing benchmarks
│   ├── run_metrics.py                 # GPU metrics benchmarks
│   ├── run_microbench_maximus.py      # Microbench Maximus runner
│   ├── run_microbench_duckdb.py       # Microbench DuckDB baseline
│   ├── generate_tpch_data.py          # TPC-H data generator
│   ├── generate_h2o_data.py           # H2O data generator
│   └── generate_clickbench_data.py    # ClickBench data generator
├── results/                           # Benchmark result CSVs
├── tests/                             # Benchmark data (CSV)
└── third_party/                       # cxxopts, sqlparser
```

## Key Bug Fixes & Lessons Learned

### 1. CCCL ABI Mismatch (Segfault)
**Symptom**: Segfault in `PinnedMemoryPool::do_allocate` with null function pointer.
**Root cause**: cuDF 24.12 was built with CCCL 2.5.0, but pip `nvidia-cuda-cccl` installs CCCL 2.8.2. The ABI for `cuda::mr::async_resource_ref` changed between versions.
**Fix**: Set `CCCL_DIR` to cuDF's bundled CCCL at `libcudf/include/libcudf/lib/rapids/cmake/cccl`.

### 2. find_package Case Sensitivity
**Symptom**: `cmake` can't find cudf package.
**Root cause**: `find_package(CUDF)` looks for `CUDFConfig.cmake`, but the actual file is `cudf-config.cmake` (lowercase).
**Fix**: Use `find_package(cudf REQUIRED)` (lowercase).

### 3. H2O Data Generator INT32 Overflow
**Symptom**: DuckDB crashes generating H2O data at scale >= 5GB (`n_rows * 7` overflows INT32).
**Root cause**: `hash(i + offset + n_rows*7)` where `n_rows=325000000` and multiplier=7 exceeds INT32 range.
**Fix**: Cast all arithmetic to BIGINT: `CAST(i AS BIGINT) + CAST({offset} AS BIGINT) + CAST({n_rows} AS BIGINT)*N`.

### 4. Aggregate Function Names
**Symptom**: `std::runtime_error` when using `hash_min_max` aggregate in query plans.
**Root cause**: `hash_min_max` is not a valid aggregate function name in Maximus. Min and max must be specified separately.
**Fix**: Replace `aggregate("hash_min_max", ...)` with separate `aggregate("min", col, alias)` and `aggregate("max", col, alias)` calls.

### 5. GPU Memory Contention in Parallel Runs
**Symptom**: Queries that pass at sf10 fail with OOM at sf5.
**Root cause**: Multiple benchmark processes launched in parallel share the same GPU. Each process's RMM pool allocates 50-90% of GPU memory, causing contention.
**Fix**: Run benchmarks sequentially, not in parallel. Even with `--storage_device=cpu`, cuDF GPU computation still allocates GPU memory.

## Troubleshooting

### "Maximum pool size exceeded" (OOM)
Reduce scale factor or use `--storage_device=cpu` (slower but uses less GPU memory).

### Segfault in PinnedMemoryPool::do_allocate
CCCL version mismatch. See Bug Fix #1 above.

### libkvikio.so not found at runtime
Add `/usr/local/lib/python3.10/dist-packages/libkvikio/lib64` to `LD_LIBRARY_PATH`.

## License

Apache License 2.0
