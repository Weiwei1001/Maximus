# gpu_db: GPU-Accelerated SQL Query Engine

Comparing **Maximus** (standalone GPU query engine, Apache Arrow Acero + cuDF) and **Sirius** (DuckDB GPU extension) across TPC-H, H2O, and ClickBench benchmark suites.

## Quick Start

```bash
git clone --recurse-submodules https://github.com/Weiwei1001/gpu_db.git
cd gpu_db

# Full pipeline: install deps, build, generate data, run ALL experiments
./setup.sh
source setup_env.sh
bash benchmarks/scripts/run_all_benchmarks.sh

# Already installed? Skip setup, auto-detect Maximus/Sirius paths from repo
bash benchmarks/scripts/run_all_benchmarks.sh --no-install

# Quick smoke test (3 queries per benchmark, smallest SF only)
bash benchmarks/scripts/run_all_benchmarks.sh --test

# Skip energy sweep experiments (Category C)
bash benchmarks/scripts/run_all_benchmarks.sh --baseline

# Only run GPU frequency/power experiments (Category C, no CPU freq changes)
bash benchmarks/scripts/run_all_benchmarks.sh --gpu-only
```

### Command-Line Flags

| Flag | Description |
|------|-------------|
| *(no flags)* | Full pipeline: install + build + data + **all experiments** (Category A + B + C) |
| `--no-install` | Skip installation/build; auto-detect `maxbench` and `sirius/build/release/duckdb` from repo root |
| `--test` | Smoke test mode: 3 queries per benchmark, smallest SF, all categories |
| `--baseline` | Skip Category C entirely (no frequency/power sweeps) |
| `--gpu-only` | Category C uses only GPU settings (skip CPU frequency changes) |
| `--maximus-only` / `--skip-sirius` | Skip Sirius engine installation and benchmarks |
| `--skip-data` | Skip benchmark data generation |

Flags are combinable: `--test --baseline` runs a minimal smoke test without energy sweeps.

---

## Experiment Design

### Query Inventory

| Type | TPC-H | H2O | ClickBench | Total |
|------|-------|-----|------------|-------|
| **Standard bench** | 22 | 9 (q8 unimpl.) | 39 (q18/q27/q28/q42 unimpl.) | **70** |
| **Microbench** | 55 | 35 | 30 | **120** |
| **Total** | 77 | 44 | 69 | **190** |

### Scale Factors

| Benchmark | Scale Factors | Data Path |
|-----------|--------------|-----------|
| TPC-H | 1, 5, 10, 20 | `tests/tpch/csv-{sf}` |
| H2O | 1gb, 2gb, 3gb, 4gb | `tests/h2o/csv-{sf}` |
| ClickBench | 1, 5, 10, 20 | `tests/clickbench/csv-{sf}` |

### Three Experiment Categories

#### Category A -- Data on GPU (`-s gpu`)

Data is pre-loaded to GPU VRAM before timing. Measures pure GPU compute performance.

| Measurement | Maximus | Sirius | Notes |
|-------------|---------|--------|-------|
| **Timing** | 3 reps, report min | 3 passes of all queries, report 3rd pass | `-s gpu` always for Maximus |
| **Metrics** | nvidia-smi sampling at 50ms | Batched in groups of 10 queries | Sirius: avoid memory leaks by limiting reps |

**Per engine**: 190 queries x 4 SFs = **760 tests** (timing) + **760 tests** (metrics)
**Both engines**: **3,040 tests**

#### Category B -- Data on CPU (`-s cpu`)

Data stays in CPU memory; includes PCIe transfer overhead. **Timing only, no metrics.**

| Engine | Method |
|--------|--------|
| **Maximus** | Same as Cat-A but with `-s cpu`, 3 reps |
| **Sirius** | Report 1st pass time (cold, includes data transfer) |

**Both engines**: **1,520 tests**

#### Category C -- Energy Optimization Sweep

Explores CPU frequency x GPU frequency/power-limit configurations to find energy-optimal settings.

**C1: CPU x GPU Frequency Sweep** (8 CPU levels x 8 GPU levels = 64 configs)
- Benchmarks: TPC-H (sf=1, 10) + H2O (sf=1, 4) + corresponding microbench
- Per config per engine: (22+9)x2 + (55+35)x2 = **242 tests**
- Total: 64 x 242 x 2 engines = **30,976 tests**

**C2: GPU Power Limit Sweep** (8 levels, percentage-based)
- Same query set as C1
- Total: 8 x 242 x 2 engines = **3,872 tests**

**Important**: CPU and GPU frequency levels are **not hardcoded**. The script reads the machine's available scaling range and divides it into 8 evenly-spaced levels. This ensures portability across different hardware.

**`--gpu-only`**: When the machine lacks CPU frequency scaling permissions (no `cpufreq` access), use this flag to run only GPU power/clock sweeps (Category C2 + GPU-only portion of C1, CPU stays at default).

**`--baseline`**: Skip Category C entirely. Useful when only timing/metrics comparisons are needed.

### Experiment Count Summary

| Category | Description | Full | Test Mode |
|----------|-------------|------|-----------|
| **A** | GPU data, timing + metrics | 3,040 | 288 |
| **B** | CPU data, timing only | 1,520 | 144 |
| **C1** | 8x8 CPU x GPU freq sweep | 30,976 | 1,536 |
| **C2** | 8-level GPU power limit | 3,872 | 192 |
| **Total** | | **39,408** | **2,160** |

Test mode reduces each benchmark to **3 representative queries** and uses the **smallest 2 scale factors**, giving an ~18x reduction.

---

## Measurement Methodology

### Timing

**Maximus** uses `maxbench` with CUDA stream barriers + `std::chrono::high_resolution_clock`:

```
barrier() -> start_time -> execute() -> barrier() -> end_time
```

Data pre-loaded with `-s gpu` stays in VRAM across repetitions (timings exclude PCIe transfer).

**Sirius** runs 3 passes of all queries in batches of 10, reports 3rd pass timing (warm cache). For Category B (CPU data), reports 1st pass timing instead.

### Energy & Power

GPU power sampled via `nvidia-smi` at **50ms intervals**. Steady-state detected via GPU utilization threshold:

```
E_query = P_steady x t_query
```

**Sirius memory leak note**: Sirius has a known GPU memory leak. Metrics runs batch queries in groups of 10 and limit total repetitions to prevent OOM. The measurement window must still be long enough for reliable power sampling.

---

## Benchmark Suites

### Standard Benchmarks (70 executable queries)

| Suite | Total | Executable | Unimplemented | Scale Factors |
|-------|-------|------------|---------------|---------------|
| TPC-H | 22 (q1-q22) | 22 | -- | 1, 5, 10, 20 |
| H2O | 10 (q1-q10) | 9 | q8 | 1gb, 2gb, 3gb, 4gb |
| ClickBench | 43 (q0-q42) | 39 | q18, q27, q28, q42 | 1, 5, 10, 20 |

Unimplemented queries are due to missing cuDF functions (not OOM). These are excluded from benchmark runs.

### Microbenchmarks (120 queries)

Fine-grained workload-typed queries for detailed performance characterization.

| Suite | Queries | Workloads |
|-------|---------|-----------|
| H2O | 35 | w1(scan/agg), w2(filter), w3(low-card groupby), w4(high-card groupby), w6(sort) |
| TPC-H | 55 | w1-w4, w5a(2-3 table joins), w5b(5-6 table joins), w6(sort/limit) |
| ClickBench | 30 | w1-w4, w6 (includes 5 cross-benchmark queries) |

```bash
# Run individual microbench query
./build/benchmarks/maxbench --benchmark=microbench_h2o --queries=w1_001 \
    --device=gpu --storage_device=gpu --engines=maximus --n_reps=5 \
    --path=tests/h2o/csv-1gb
```

---

## Build

### Prerequisites

- CUDA 12.0+ (`nvcc` required)
- Apache Arrow 17.0.0 (built from source by `setup.sh`)
- Taskflow v3.11.0 (built from source by `setup.sh`)
- cuDF 24.12+ (`pip install` or conda)
- C++20 compiler (GCC >= 11)

### Manual Build (if not using setup.sh)

```bash
cmake -B build -GNinja \
  -DMAXIMUS_WITH_GPU=ON \
  -DMAXIMUS_WITH_BENCHMARKS=ON \
  -DMAXIMUS_WITH_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CUDA_ARCHITECTURES=native

ninja -C build -j$(nproc)

# Run tests
cd build && ctest
```

### CRITICAL: CCCL Version Must Match cuDF

cuDF 24.12 was built with **CCCL 2.5.0**. Using a newer CCCL causes ABI mismatch resulting in segfault. `setup.sh` handles this automatically. For manual builds, set:
```
CCCL_DIR="<python-site-packages>/libcudf/include/libcudf/lib/rapids/cmake/cccl"
```

---

## Running Benchmarks Individually

```bash
source setup_env.sh

# Maximus single query
./build/benchmarks/maxbench --benchmark tpch -q q1,q6 -d gpu -r 3 \
    --path tests/tpch/csv-1 -s gpu --engines maximus

# Full benchmark suite (both engines)
python3 benchmarks/scripts/run_all.py
python3 benchmarks/scripts/run_all.py --engine maximus --benchmarks tpch h2o

# Individual engine timing
python3 benchmarks/scripts/run_maximus_benchmark.py tpch h2o clickbench
python3 benchmarks/scripts/run_sirius_benchmark.py tpch h2o clickbench

# Metrics (GPU power/utilization sampling)
python3 benchmarks/scripts/run_maximus_metrics.py tpch --scale-factors 1 2
python3 benchmarks/scripts/run_sirius_metrics.py tpch --scale-factors 1 2

# CPU-data mode (Category B)
python3 benchmarks/scripts/run_maximus_cpu_data.py tpch h2o
python3 benchmarks/scripts/run_sirius_cpu_data.py tpch h2o clickbench

# Energy sweep (Category C)
python3 benchmarks/scripts/run_energy_sweep.py
python3 benchmarks/scripts/run_energy_sweep.py --power-limits 250,300,360 --sm-clocks 1200,2400
```

---

## Project Structure

```
gpu_db/
├── setup.sh                           # One-click: deps + build + data
├── setup_env.sh                       # Runtime environment (source this)
├── sirius/                            # DuckDB GPU extension (git submodule)
├── src/maximus/                       # Maximus engine source
│   ├── operators/gpu/cudf/            #   cuDF operator implementations
│   ├── gpu/                           #   GPU context, CUDA wrappers
│   ├── tpch/, h2o/, clickbench/       #   Benchmark query plans (C++)
│   └── microbench/                    #   120 microbench query plans (C++)
├── benchmarks/
│   ├── maxbench.cpp                   # C++ benchmark binary
│   ├── scripts/                       # Python/bash benchmark runners
│   │   ├── run_all_benchmarks.sh      #   Master orchestrator (A + B + C)
│   │   ├── run_all.py                 #   Maximus + Sirius timing
│   │   ├── run_maximus_benchmark.py   #   Maximus timing
│   │   ├── run_sirius_benchmark.py    #   Sirius timing
│   │   ├── run_maximus_metrics.py     #   Maximus GPU metrics
│   │   ├── run_sirius_metrics.py      #   Sirius GPU metrics
│   │   ├── run_maximus_cpu_data.py    #   Maximus Category B
│   │   ├── run_sirius_cpu_data.py     #   Sirius Category B
│   │   ├── run_energy_sweep.py        #   Category C energy sweep
│   │   └── run_freq_sweep.py          #   Category C freq sweep
│   └── data/                          # Data generation scripts
├── microbench/                        # SQL files (DuckDB baseline)
├── results/                           # Benchmark output CSVs
├── tests/                             # Benchmark data (CSV)
└── third_party/                       # cxxopts, sqlparser
```

---

## GPU Memory Notes

- **sf1-sf10**: fit comfortably with `-s gpu` on 80GB GPUs
- **sf20 (TPC-H)**: some queries OOM (auto-retries with `-s cpu`)
- **sf4 (H2O)**: some queries OOM at batch level (works per-query)
- The script automatically falls back to `-s cpu` on OOM

## Troubleshooting

### "Maximum pool size exceeded" (OOM)
Reduce scale factor or use `--storage_device=cpu`.

### Segfault in PinnedMemoryPool::do_allocate
CCCL version mismatch. Ensure `CCCL_DIR` points to cuDF's bundled CCCL 2.5.0. See `setup.sh` step 6.5.

### libkvikio.so not found at runtime
Run `source setup_env.sh` to set `LD_LIBRARY_PATH`.

### cmake can't find cudf
Use `find_package(cudf REQUIRED)` (lowercase).

### GPU memory contention
Never run multiple benchmark processes in parallel on the same GPU.

## License

Apache License 2.0
