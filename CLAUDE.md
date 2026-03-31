# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GPU-accelerated SQL benchmark suite comparing **Maximus** (standalone GPU query engine built on Arrow Acero + cuDF) and **Sirius** (DuckDB GPU extension). Benchmarks: TPC-H (22 queries), H2O groupby (9 queries, q8 unimplemented), ClickBench (39 of 43 queries; q18/q27/q28/q42 unsupported on cuDF), plus 120 microbench queries for fine-grained workload characterization.

## Build Commands

```bash
# Full setup from scratch (deps + build + test data)
./setup.sh && source setup_env.sh

# Configure with GPU support
cmake -B build -GNinja \
  -DMAXIMUS_WITH_GPU=ON \
  -DMAXIMUS_WITH_BENCHMARKS=ON \
  -DMAXIMUS_WITH_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CUDA_ARCHITECTURES=90  # 90=Blackwell/5080, 80=Ampere, 70=Volta

# Build
ninja -C build -j$(nproc)

# Run all tests
cd build && ctest
# or: ninja -C build test

# Run a single test (tests are gtest binaries)
./build/tests/maximus_test --gtest_filter="TpchGpu.*"
```

**Important**: Always `source setup_env.sh` before running anything — it sets `LD_LIBRARY_PATH` for Arrow, cuDF, RMM, and other shared libraries.

## Running Benchmarks

```bash
# Master orchestrator (both engines, all benchmarks)
python benchmarks/scripts/run_all.py
python benchmarks/scripts/run_all.py --engine maximus --benchmarks tpch h2o

# Individual engine timing
python benchmarks/scripts/run_maximus_benchmark.py tpch h2o clickbench
python benchmarks/scripts/run_sirius_benchmark.py tpch h2o clickbench

# Power/energy measurement (nvidia-smi sampling at 50ms)
python benchmarks/scripts/run_maximus_metrics.py tpch --scale-factors 1 2
python benchmarks/scripts/run_sirius_metrics.py tpch --scale-factors 1 2

# Direct maxbench usage
./build/benchmarks/maxbench --benchmark tpch -q q1,q2,q3 -d gpu -r 50 \
    --path tests/tpch/csv-1 -s gpu --engines maximus

# Microbench (120 fine-grained queries)
bash scripts/benchmarks/run_all_microbench.sh --n-reps 5
./build/benchmarks/maxbench --benchmark=microbench_tpch --queries=w1_001 \
    --device=gpu --storage_device=gpu --engines=maximus --n_reps=5 --path=tests/tpch/csv-1
```

### GPU Frequency / Energy Sweep Scripts

```bash
# Energy sweep: 3 power limits × 3 SM clocks = 9 GPU configs
python benchmarks/scripts/run_energy_sweep.py

# Frequency sweep: 4 CPU/GPU frequency configs (baseline, cpu_low, gpu_low, both_low)
python benchmarks/scripts/run_freq_sweep.py
python benchmarks/scripts/run_freq_sweep_cpu_storage.py  # same but --storage_device=cpu
```

## Architecture

### Query Execution Pipeline
```
SQL string → hsql parser (third_party/sql-parser)
  → MaxSQL AST → QueryPlan (DAG of QueryNodes)
  → Executor → Pipeline(s)
  → Operators (CPU via Acero OR GPU via cuDF)
  → Arrow Table result
```

### Key Source Layout
- `src/maximus/sql/parser.cpp` — SQL→query plan translation
- `src/maximus/dag/` — query plan DAG (QueryPlan, QueryNode, Edge)
- `src/maximus/exec/` — executor and pipeline scheduling
- `src/maximus/operators/acero/` — CPU operators (Arrow Acero backend)
- `src/maximus/operators/gpu/cudf/` — GPU operators (filter, project, group_by, hash_join, order_by, table_source)
- `src/maximus/gpu/cudf/cudf_expr.cpp` — expression compilation to RAPIDS/cuDF
- `src/maximus/gpu/gtable/` — GPU table/column abstractions (GTable, GColumn)
- `src/maximus/frontend/query_plan_api.cpp` — programmatic query construction API
- `src/maximus/tpch/`, `h2o/`, `clickbench/` — benchmark query definitions (hardcoded query plans)
- `src/maximus/microbench/` — 120 microbench query plans (C++, workload-typed)
- `microbench/` — SQL files for DuckDB baseline (h2o/35, tpch/55, clickbench/25+5 cross-benchmark)

### Operator Abstraction
Abstract base classes (`abstract_*.hpp` in `src/maximus/operators/`) define the interface. Implementations live in `acero/` (CPU), `gpu/cudf/` (GPU), or `native/` (custom CPU). Operator selection is by `DeviceType::CPU|GPU`.

### Dual-Engine Design
- **Maximus**: C++ engine, queries defined as programmatic query plans in `*_queries.cpp`, run via `maxbench` binary
- **Sirius**: DuckDB GPU extension (`sirius/` git submodule), queries generated as SQL by `generate_sirius_sql.py`, run via DuckDB CLI at `sirius/build/release/duckdb`

## Benchmark Scale Factors

| Benchmark | Scale Factors | Data Path |
|-----------|--------------|-----------|
| TPC-H | 1, 2, 10, 20 | `tests/tpch/csv-{sf}` |
| H2O | 1gb, 2gb, 3gb, 4gb | `tests/h2o/csv-{sf}` |
| ClickBench | 10, 20, 50, 100 | `tests/clickbench/csv-{sf}` |

## GPU Memory and Storage Device

Use `--storage_device=gpu` (default) to pre-load data into VRAM for fastest timings. Use `--storage_device=cpu` when datasets exceed GPU memory. Even with CPU storage, cuDF still allocates GPU memory for computation — **never run multiple benchmark processes in parallel** as RMM pools will contend for GPU memory and cause OOM.

Known OOM limits (16GB RTX 5080): TPC-H sf20 q17/q18/q19/q21, H2O sf4 q3/q7/q10.

## Measurement Methodology

**Timing**: Maximus uses CUDA stream barriers + chrono (reports min of 50 reps). Sirius uses DuckDB `.timer` (3 passes × 100 reps, reports last pass).

**Power/Energy**: nvidia-smi sampled at 50ms intervals. Steady-state detected by GPU utilization threshold (avg_util across all samples). Energy = P_steady × query_latency.

## Key CMake Options

| Option | Default | Purpose |
|--------|---------|---------|
| `MAXIMUS_WITH_GPU` | OFF | Enable CUDA/cuDF support |
| `MAXIMUS_WITH_TESTS` | ON | Build gtest targets |
| `MAXIMUS_WITH_BENCHMARKS` | OFF | Build maxbench binary |
| `MAXIMUS_WITH_PROFILING` | OFF | Enable Caliper profiling |
| `CMAKE_CUDA_ARCHITECTURES` | — | GPU compute capability |

## Dependencies

- Apache Arrow 17.0.0 (Acero + Parquet)
- cuDF 24.12+ (pip install, for GPU)
- RMM (RAPIDS memory manager)
- Taskflow v3.11.0
- CUDA 12.0+, C++20, CMake 3.17+, Ninja

**Critical**: cuDF 24.12 was built with CCCL 2.5.0. Using a newer CCCL (e.g., 2.8.2 from `nvidia-cuda-cccl` pip) causes ABI mismatch → segfault in `PinnedMemoryPool::do_allocate`. Always use the CCCL bundled with cuDF.

## Hardware Environment

- GPU: NVIDIA RTX 5080 (index 1, 16GB) + T400 (index 0, 2GB)
- CPU: Intel Xeon w5-2455X (12C/24T)
- Maximus install: `/home/xzw/Maximus/`
- Sirius install: `/home/xzw/sirius/`

## Code Style

Google C++ style (`.clang-format`), 4-space indent, ~100 char line width, C++20.
