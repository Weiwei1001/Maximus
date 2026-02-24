<!---
[![pipeline status](https://gitlab.inf.ethz.ch/PUB-SYSTEMS/eth-dataprocessing/Maximus/badges/main/pipeline.svg)](https://gitlab.inf.ethz.ch/PUB-SYSTEMS/eth-dataprocessing/Maximus/-/commits/main)
--->

<div>
<centering>
<p align="center"><img src="./assets/maximus-logo.svg" width="70%"></p>
</centering>
</div>

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Dependencies](#dependencies)
- [Installation](#installation)
- [GPU Setup with pip cuDF](#gpu-setup-with-pip-cudf)
- [RMM GPU Memory Pool](#rmm-gpu-memory-pool)
- [Benchmarking](#benchmarking)
- [Benchmark Scripts](#benchmark-scripts)
- [GPU Metrics Measurement](#gpu-metrics-measurement)
- [Benchmark Data](#benchmark-data)
- [Sirius Comparison](#sirius-comparison)
- [Known GPU Limitations](#known-gpu-limitations)
- [Testing](#testing)

## Overview

Maximus is a modular, accelerated query engine for data analytics (OLAP) on Heterogeneous Systems, developed in the Systems Group at ETH Zurich (Swiss Federal Institute of Technology).
Through the concept of operator-level integration, Maximus can use operators from third-party engines and achieve even better performance with these operators than when they are used within their native engines.
In the current version, Maximus integrates operators from Apache Acero (CPU) and cuDF (GPU), but its modular design and flexibility allow it to easily integrate operators from other engines, as well.
Maximus supports all TPC-H queries on both the CPU and the GPU and can achieve performance comparable to that of the best systems available but with a far higher degree of completeness and flexibility.

## Features

Maximus has the following features:
- modular and flexible design
- operator-level integration:
  - integrates CPU operators from Apache Acero
  - integrates GPU operators from cuDF
- push-based query execution
- uses columnar storage (arrow format)
- accelerators support (GPUs)

## Dependencies

The installation requires the following dependencies:
- C/C++ compiler (tested with clang and gcc)
- Apache Arrow, which can be installed with [this script](./scripts/build_arrow.sh). If you had issues in installing arrow see [here](./Installation_issues.md)
- Taskflow, which can be installed with [this script](./scripts/build_taskflow.sh).
- cuDF (optional, only used in a GPU version), which can be installed by following [these instructions](./scripts/build_cudf.md).
- Caliper Profiler (optional, only used when profiling enabled), which can be installed using [this script](./scripts/build_caliper.sh).

## Installation

To build and install Maximus, do the following steps:
```bash
###############
# get Maximus
###############
git clone https://gitlab.inf.ethz.ch/PUB-SYSTEMS/eth-dataprocessing/Maximus maximus && cd maximus

##############################
# build and install Maximus
##############################
mkdir build && cd build

# set up the compiler you want to use, e.g. with:
export CC=`which cc`
export CXX=`which CC`

# run cmake with optional parameters
cmake -DCMAKE_BUILD_TYPE=Release -DMAXIMUS_WITH_TESTS=ON -DCMAKE_PREFIX_PATH=<path where dependencies installed> ..

# compile
make -j 8

# install
make install
```

The overview of available CMake options is given in the table below. The default values are given in **bold**. These options can be passed in the `cmake` command with the prefix `-D`, as in `cmake -DCMAKE_BUILD_TYPE=Release`.

CMAKE OPTION | POSSIBLE VALUES | DESCRIPTION
| :------------------- | :------------------- |:------------------- |
`CMAKE_BUILD_TYPE` | **Release**, Debug | Whether to build in the release or debug mode.
`CMAKE_INSTALL_PREFIX` | any path, by default `./` | The path where to install Maximus.
`CMAKE_PREFIX_PATH` | any path, defined by CMake | The path(s) where to look for dependencies
`BUILD_SHARED_LIBS` | **ON**, OFF | Whether to build Maximus as a shared library (ON) or as a static library (OFF).
`MAXIMUS_WITH_TESTS` | **ON**, OFF | Whether to build the tests as well.
`MAXIMUS_WITH_EXAMPLES` | ON, **OFF** | Whether to build the examples as well.
`MAXIMUS_WITH_BENCHMARKS` | ON, **OFF** | Whether to build the benchmarks as well.
`MAXIMUS_WITH_GPU` | ON, **OFF** | Whether to build the GPU backend.
`MAXIMUS_WITH_PROFILING` | ON, **OFF** | Whether to enable profiling.
`MAXIMUS_WITH_SANITIZERS` | ON, **OFF** | Enable the sanitizers for all the targets.

## GPU Setup with pip cuDF

For GPU support using pip-installable cuDF (instead of building from source), use the provided configure script:

```bash
# Install cuDF and dependencies via pip
pip install cudf-cu12 libcudf-cu12

# Configure and build with GPU support
bash scripts/configure_with_gpu_pip_cudf.sh
cd build && make -j$(nproc)
```

The script automatically detects pip-installed cuDF libraries and sets the correct CMake paths. See [scripts/build_cudf.md](./scripts/build_cudf.md) for more details.

**Note:** At runtime, you may need to set `LD_LIBRARY_PATH` to include the pip-installed NVIDIA libraries:
```bash
export LD_LIBRARY_PATH=/usr/local/lib/python3.12/dist-packages/nvidia/libnvcomp/lib64:/usr/local/lib/python3.12/dist-packages/libkvikio/lib64:$LD_LIBRARY_PATH
```

## RMM GPU Memory Pool

Maximus uses [RMM (RAPIDS Memory Manager)](https://github.com/rapidsai/rmm) for GPU memory allocation. The pool is configured in `src/maximus/context.hpp`:

```cpp
rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource> pool_mr{
    &cuda_mr, rmm::percent_of_free_device_memory(20)};
```

| Parameter | Value | Description |
|:----------|:------|:------------|
| Initial size | 20% of free VRAM | ~6.4 GiB on a 32GB GPU with no other processes |
| Maximum size | Unlimited | Pool grows dynamically via upstream `cudaMalloc` |
| Pinned host memory | 4 GiB (default) | Configurable via `MAXIMUS_MAX_PINNED_POOL_SIZE` env var |

**Storage device flag (`-s`):**
- `-s cpu`: Tables stored in CPU memory, transferred to GPU per query. Safe for any dataset size.
- `-s gpu`: Tables pre-loaded to GPU VRAM. Faster queries (no transfer overhead), but limited by VRAM.

**VRAM capacity examples (RTX 5090, 32GB):**

| Dataset | CSV Size | Fits with `-s gpu`? |
|:--------|:---------|:--------------------|
| ClickBench SF=10 | ~7 GiB | Yes (~7 GiB on GPU) |
| ClickBench SF=20 | ~14 GiB | Yes on clean GPU (~14 GiB + pool overhead) |
| TPC-H SF=20 | ~15 GiB (multi-table) | Depends on query (JOINs need intermediate space) |

**Troubleshooting OOM errors:**
- `"maximum pool size exceeded"` means `cudaMalloc` failed upstream, not a pool config limit.
- Check `nvidia-smi` to verify no other processes are using GPU memory.
- Try `-s cpu` as a fallback (slower but avoids VRAM pressure).
- The pinned pool size can be increased: `export MAXIMUS_MAX_PINNED_POOL_SIZE=8589934592` (8 GiB).

## Benchmarking

Running the full TPC-H benchmark is possible by running the `maxbench` executable. Within the `build` folder, it can be run e.g. with:
```bash
./benchmarks/maxbench --benchmark=tpch --queries=q1,q2,q3 --device=cpu --storage_device=cpu --engines=maximus --n_reps=5 --path=<path to CSV files>
```
Possible values for each of these options are:
- `benchmark`: the benchmarking suite to you, can be one of `tpch`, `clickbench` or `h2o`.
- `queries`: any comma-separated list of queries. For example, for the tpch benchmark, any subset of `q1, q2, ... , q22` is valid.
- `device`: `cpu`, `gpu`. This describes where the query will be executed.
- `storage_device`: `cpu`, `cpu-pinned`, `gpu`. This describes where the tables are initially stored. The cpu-pinned memory requires the GPU-backend.
- `engines`: `maximus`, `acero`. This specifies which engine to use.
- `path`: any path where the tables are located.
- `csv_batch_size`: a positive integer, or in a form of a power e.g. `2^20`. specifying the batch size of the tables that are initially stored on the CPU.

Running this e.g. for a query q1, will yield the following output:
```bash
===================================
          LOADING TABLES
===================================
Loading tables to:                   cpu
Loading times over repetitions [ms]: 25,
===================================
    MAXBENCH TPCH BENCHMARK:    
===================================
---> benchmark:                TPCH
---> queries:                  q1
---> Tables path:              /../maximus/tests/tpch/csv-0.01
---> Engines:                  maximus
---> Number of reps:           1
---> Device:                   cpu
---> Storage Device:           cpu
---> Num. outer threads:       1
---> Num. inner threads:       8
---> Operators Fusion:         OFF
---> CSV Batch Size (string):  2^30
---> CSV Batch (number):       1073741824
---> Tables initially pinned:  NO
---> Tables as single chunk:   NO
===================================
            QUERY q1
===================================
---> TPC-H query: q1
---> Query Plan:
NATIVE::QUERY PLAN ROOT
    └── NATIVE::TABLE SINK
        └── ACERO::ORDER BY
            └── ACERO::GROUP BY
                └── ACERO::PROJECT
                    └── ACERO::FILTER
                        └── NATIVE::TABLE SOURCE

Query Plan after scheduling:
NATIVE::QUERY PLAN ROOT
    └── NATIVE::TABLE SINK
        └── ACERO::ORDER BY
            └── ACERO::GROUP BY
                └── ACERO::PROJECT
                    └── ACERO::FILTER
                        └── NATIVE::TABLE SOURCE

===================================
              RESULTS
===================================
maximus RESULTS (top 10 rows):
l_returnflag(utf8),l_linestatus(utf8),sum_qty(double),sum_base_price(double),sum_disc_price(double),sum_charge(double),avg_qty(double),avg_price(double),avg_disc(double),count_order(int64)
A,F,380456.000000,532348211.649999,505822441.486100,526165934.000840,25.575155,35785.709307,0.050081,14876
N,F,8971.000000,12384801.370000,11798257.208000,12282485.056933,25.778736,35588.509684,0.047759,348
N,O,742847.000000,1041580998.800000,989812549.690601,1029499565.063830,25.455658,35692.584429,0.049931,29182
R,F,381449.000000,534594445.350000,507996454.406700,528524219.358904,25.597168,35874.006533,0.049828,14902


===================================
              TIMINGS
===================================
Execution times [ms]:
- MAXIMUS TIMINGS [ms]: 	2
Execution stats (min, max, avg):
- MAXIMUS STATS: MIN = 2 ms; 	MAX = 2 ms; 	AVG = 2 ms
===================================
        SUMMARIZED TIMINGS
===================================
--->Results saved to ./results.csv
cpu,maximus,q1,2,
```

### Benchmark Suites

Maximus supports three benchmark suites:

| Suite | Queries | Scale Factors | Notes |
|:------|:--------|:--------------|:------|
| **TPC-H** | q1 - q22 (22 queries) | SF 1, 2, 10, 20 | All 22 queries supported on GPU |
| **H2O** | q1 - q10 (9 queries) | 1gb, 2gb, 3gb, 4gb | q8 not implemented |
| **ClickBench** | q0 - q42 (39 queries) | SF 1, 2, 10, 20 | q18, q27, q28, q42 unsupported on GPU |

## Benchmark Scripts

Automated benchmark runners are provided in `benchmarks/scripts/`:

| Script | Description |
|:-------|:------------|
| `run_maximus_benchmark.py` | Maximus GPU timing benchmark (3 reps, min time, data via `-s cpu`) |
| `run_maximus_metrics.py` | Maximus GPU steady-state metrics (power, energy, GPU util via nvidia-smi) |
| `run_sirius_benchmark.py` | Sirius GPU benchmark runner (3 passes, 10 queries/batch, 3rd pass timing) |
| `compare_results.py` | Per-query comparison table with ratio and winner |
| `setup_sirius.sh` | Download, build, and setup Sirius (DuckDB GPU extension) |
| `generate_sirius_sql.py` | Generate Sirius-format GPU query SQL files |

### Running Maximus Benchmarks

```bash
# Run all benchmarks (TPC-H, H2O, ClickBench)
python benchmarks/scripts/run_maximus_benchmark.py

# Run specific benchmarks
python benchmarks/scripts/run_maximus_benchmark.py tpch clickbench

# Custom repetitions and output directory
python benchmarks/scripts/run_maximus_benchmark.py --n-reps 5 --results-dir ./results tpch
```

### Running Sirius Benchmarks

```bash
# Setup Sirius (one-time)
bash benchmarks/scripts/setup_sirius.sh

# Run all benchmarks
python benchmarks/scripts/run_sirius_benchmark.py

# Run specific benchmarks with custom settings
python benchmarks/scripts/run_sirius_benchmark.py tpch --n-passes 3 --batch-size 10
```

### Running GPU Metrics

The metrics script measures steady-state GPU power consumption and energy per query:

```bash
# Measure ClickBench SF=10 metrics (default)
python benchmarks/scripts/run_maximus_metrics.py clickbench --sf 10

# Measure all configured SFs for a benchmark
python benchmarks/scripts/run_maximus_metrics.py tpch

# Custom target time (default: 10s sustained execution per query)
python benchmarks/scripts/run_maximus_metrics.py --target-time 15 clickbench
```

### Comparing Results

```bash
python benchmarks/scripts/compare_results.py \
    --sirius benchmark_results/sirius_benchmark.csv \
    --maximus benchmark_results/maximus_benchmark.csv
```

## GPU Metrics Measurement

The `run_maximus_metrics.py` script measures steady-state GPU power and energy consumption with the following methodology:

**Phase 1 — Calibration:** Each query is run 3 times with `-s gpu` (data on GPU) to measure base latency.

**Phase 2 — Calculate repetitions:** For each query, `n_reps = ceil(10s / query_latency)` so that total execution exceeds 10 seconds. This ensures the GPU is under sustained load long enough for accurate power sampling.

**Phase 3 — Metrics collection:** Each query runs `n_reps` times while `nvidia-smi` samples GPU telemetry at 50ms intervals. The first and last 10% of samples are trimmed to isolate steady-state behavior.

**Metrics collected per query:**

| Metric | Unit | Description |
|:-------|:-----|:------------|
| `min_ms` / `avg_ms` | ms | Query execution time (min and average across reps) |
| `avg_power_w` | Watts | Average GPU power draw during steady state |
| `max_power_w` | Watts | Peak GPU power draw |
| `energy_j` | Joules | Total energy = avg_power * elapsed_time |
| `avg_gpu_util` | % | Average GPU compute utilization |
| `max_mem_mb` | MiB | Peak GPU memory usage |

**Why `-s gpu` matters for metrics:** With `-s cpu`, each query iteration includes a CPU-to-GPU data transfer, which dominates execution time for fast queries and results in near-idle GPU utilization readings. With `-s gpu`, data is pre-loaded to GPU VRAM, so all `n_reps` iterations are pure GPU compute — giving accurate steady-state power readings (typically 200-400W under load vs ~50W idle).

**Example output (ClickBench SF=10, RTX 5090):**
```
q2 (667 reps, -s gpu)... 15ms, 28.2s, 255W, 97%util, 31468MB, 7173J [OK]
q3 (770 reps, -s gpu)... 13ms, 30.6s, 266W, 97%util, 31468MB, 8129J [OK]
q4 (5000 reps, -s gpu)... 2ms, 116.4s, 375W, 95%util, 32045MB, 43648J [OK]
```

## Benchmark Data

Benchmark results are stored in `benchmark_results/` (packaged as `sirius_benchmark_package.tar.gz`, 1.3 MB).

### Sirius Timing Data

| File | Rows | Size | Description |
|:-----|-----:|-----:|:------------|
| `sirius_timing_per_query.csv` | 312 | 14 KB | All Sirius query timing (TPC-H + H2O + ClickBench) |
| `tpch_timing.csv` | 100 | 4.1 KB | TPC-H: SF 1/2/10/20, 25 queries each |
| `h2o_timing.csv` | 40 | 1.7 KB | H2O: 1gb/2gb/3gb/4gb, 10 queries each |
| `clickbench_timing.csv` | 172 | 7.8 KB | ClickBench: SF 10/20/50/100, 43 queries each |

### Sirius GPU Metrics (nvidia-smi samples)

| File | Samples | Size | Description |
|:-----|--------:|-----:|:------------|
| `tpch_metrics_samples.csv` | 21,880 | 1.2 MB | TPC-H GPU telemetry (power, util%, mem, PCIe) |
| `h2o_metrics_samples.csv` | 14,057 | 728 KB | H2O GPU telemetry |
| `clickbench_metrics_samples.csv` | 45,614 | 2.4 MB | ClickBench GPU telemetry |
| `tpch_metrics_samples_summary.csv` | 100 | 4.1 KB | TPC-H per-query summary |
| `h2o_metrics_samples_summary.csv` | 40 | 1.7 KB | H2O per-query summary |
| `clickbench_metrics_samples_summary.csv` | 172 | 6.9 KB | ClickBench per-query summary |

### Maximus Timing Data

| File | Rows | Size | Description |
|:-----|-----:|-----:|:------------|
| `maximus_adaptive.csv` | 202 | 44 KB | All Maximus results (TPC-H SF 1/2/10/20, H2O 1-4gb, ClickBench SF 1/2) |
| `maximus_tpch_sf1_corrected.csv` | 22 | 1.2 KB | TPC-H SF=1 corrected re-run |

### Data Totals

- **Sirius**: 312 query timing records + 81,551 GPU metric samples across 12 benchmark/SF combinations
- **Maximus**: 224 query timing records across 10 benchmark/SF combinations
- **Total data points**: 82,087 rows, ~4.5 MB uncompressed

## Sirius Comparison

[Sirius](https://github.com/sirius-db/sirius) is a GPU extension for DuckDB. The benchmark scripts allow head-to-head comparison between Maximus and Sirius across all three benchmark suites.

To set up Sirius for benchmarking:

```bash
bash benchmarks/scripts/setup_sirius.sh [--install-dir /path/to/install]
```

Prerequisites for Sirius:
- CUDA >= 12.x with `nvcc` in PATH
- CMake >= 3.30.4 (install with `pip install cmake` if needed)
- ninja-build
- Python 3 with `duckdb` package

The setup script handles cloning, dependency installation (libcudf, abseil, libconfig++, libnuma), building, generating benchmark databases, and creating GPU query SQL files.

## Estimated Maximum Scale Factors (RTX 5090, 32GB VRAM)

| Benchmark | Tested SFs | Maximus Success | Sirius Success | Est. Max SF (all queries pass) |
|:----------|:-----------|:----------------|:---------------|:-------------------------------|
| **TPC-H** | SF 1, 2, 10, 20 | SF1-2: 22/22, SF10: 21/22, SF20: 17/22 | SF1-10: 22/22, SF20: 21/22 | Maximus: ~SF 10-15, Sirius: ~SF 30-40 |
| **H2O** | 1gb - 4gb | All: 9/9 | 1gb: 10/10, 2-4gb: 9/10 | Maximus: ~15gb, Sirius: ~10gb |
| **ClickBench** | SF 1, 2, 10, 20 | SF1-10: 39/39, SF20: 39/39 (-s cpu) | SF10-100: 43/43 | Maximus: ~SF 20+ (-s gpu), Sirius: ~SF 100+ |

**Notes:**
- The bottleneck is GPU VRAM (32GB). Simple scans/aggregations can handle larger SFs, but complex JOINs and correlated subqueries produce large intermediate results.
- TPC-H: At SF=20, Maximus fails on q17-q21 (complex correlated subqueries); Sirius only falls back on q01.
- H2O: Single-table GROUP BY queries are memory-efficient. q10 (GROUP BY all 6 columns) is the first to fail on Sirius.
- ClickBench: High-cardinality GROUP BY queries (q31, q32) are likely the first to hit memory limits at larger SFs.

## Known GPU Limitations

The following operations are not supported on the cuDF GPU backend and will cause query failures:
- `minute()` function (used in ClickBench q18, q42)
- `utf8_length()` / `STRLEN()` on GPU (used in ClickBench q27, q28)
- `REGEXP_REPLACE()` on GPU (used in ClickBench q28)

These queries must be run on the CPU backend or excluded from GPU benchmarks.

## Testing

To run the unit tests, run the following inside the build folder:
```bash
make test
```
This will run all the tests and report for each of them if it failed or not. However, each of these tests might include multiple smaller tests. To get the overview of all the tests, run:
```bash
ctest --verbose make check
```

If some of the tests have failed, the output can be seen by running `vim ./Testing/Temporary/LastTest.log` inside the `build` folder.

Alternatively, a specific test can be run, e.g. by:
```bash
./tests/test.tpch
```
