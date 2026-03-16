# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GPU-accelerated SQL benchmark suite comparing **Maximus** (standalone GPU query engine built on Arrow Acero + cuDF) and **Sirius** (DuckDB GPU extension). Benchmarks: TPC-H (22 queries), H2O groupby (9 queries, q8 unimplemented), ClickBench (39 of 43 queries; q18/q27/q28/q42 unsupported on cuDF).

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

### Operator Abstraction
Abstract base classes (`abstract_*.hpp`) define the interface. Implementations live in `acero/` (CPU), `gpu/cudf/` (GPU), or `native/` (custom CPU). Operator selection is by `DeviceType::CPU|GPU`.

### Dual-Engine Design
- **Maximus**: C++ engine, queries defined as programmatic query plans in `*_queries.cpp`, run via `maxbench` binary
- **Sirius**: DuckDB GPU extension (`sirius/` git submodule), queries generated as SQL by `generate_sirius_sql.py`, run via DuckDB CLI

## Benchmark Scale Factors

| Benchmark | Scale Factors | Data Path |
|-----------|--------------|-----------|
| TPC-H | 1, 5, 10, 20 | `tests/tpch/csv-{sf}` |
| H2O | 1gb, 2gb, 3gb, 4gb | `tests/h2o/csv-{sf}` |
| ClickBench | 1, 5, 10, 20 | `tests/clickbench/csv-{sf}` |

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

## Hardware Environment

- GPU: NVIDIA RTX 5080 (index 1, 16GB) + T400 (index 0, 2GB)
- CPU: Intel Xeon w5-2455X (12C/24T)
- Maximus install: `/home/xzw/Maximus/`
- Sirius install: `/home/xzw/sirius/`
- Sirius DuckDB binary: `sirius/build/release/duckdb`

## Code Style

Google C++ style (`.clang-format`), 4-space indent, ~100 char line width, C++20.

## Energy Sweep Experiment (completed 2026-03-05)

**Goal**: Explore GPU (power-limit, SM-clock) configurations to find energy-optimal settings for each engine/benchmark combination.

**Script**: `benchmarks/scripts/run_energy_sweep.py`

**Config grid**: 3 power limits × 3 SM clocks = 9 configs, all DONE.

**Results locations**:
- Run 1 (with clickbench): `results/energy_sweep/pl250w_clk{0600,1200,1800}mhz/`
- Run 3 (tpch+h2o only): `benchmarks/scripts/results/energy_sweep/pl*/`
- Summary CSV: `benchmarks/scripts/results/energy_sweep/energy_sweep_summary.csv` (1164 rows)
- Log: `results/energy_sweep/sweep.log`

**Best configurations** (lowest total energy per benchmark):

| Engine | Benchmark | SF | PL(W) | CLK(MHz) | Avg E(J) |
|--------|-----------|-----|-------|----------|----------|
| maximus | clickbench | 5 | 250 | 600 | 3.13 |
| maximus | h2o | 1gb | 450 | 1800 | 2.26 |
| maximus | h2o | 2gb | 300 | 1800 | 5.17 |
| maximus | tpch | 1 | 300 | 1800 | 0.92 |
| maximus | tpch | 2 | 300 | 1800 | 1.75 |
| sirius | h2o | 1gb | 250 | 1800 | 0.24 |
| sirius | h2o | 2gb | 250 | 1800 | 0.47 |
| sirius | tpch | 1 | 450 | 1800 | 0.12 |
| sirius | tpch | 2 | 250 | 1800 | 0.18 |

**Run history**: Total 3 runs. Run 3 completed 2026-03-05 09:47 (9h13m, 9/9 configs, 0 failures).
