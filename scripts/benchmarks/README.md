# Benchmark Scripts

End-to-end benchmark scripts for evaluating Maximus GPU query engine performance across three standard OLAP benchmarks and 120 microbenchmark queries.

## Scripts Overview

| Script | Purpose |
|--------|---------|
| `run_all_microbench.sh` | One-command: build + run all 120 microbench with timing & metrics |
| `run_timing.py` | Run standard timing benchmarks (74 queries, multiple SFs) |
| `run_metrics.py` | Run standard metrics benchmarks (per-query GPU sampling) |
| `run_microbench_maximus.py` | Run microbench via Maximus (120 queries, timing + GPU metrics) |
| `run_microbench_duckdb.py` | Run microbench via DuckDB baseline (120 SQL queries) |
| `generate_tpch_data.py` | Generate TPC-H data (CSV) using DuckDB |
| `generate_h2o_data.py` | Generate H2O groupby synthetic data (CSV) |
| `generate_clickbench_data.py` | Download & sample ClickBench data (CSV) |

## Prerequisites

- **Maximus** built with GPU support (`MAXIMUS_WITH_GPU=ON`, `MAXIMUS_WITH_BENCHMARKS=ON`)
- **Python 3.8+** with `duckdb` package
- **NVIDIA GPU** with drivers and `nvidia-smi`
- **CUDA 12.6+** and **cuDF 24.12**

## Quick Start

### One-command microbench (recommended)

```bash
# Build + run all 120 microbench queries with timing & GPU metrics
bash scripts/benchmarks/run_all_microbench.sh --n-reps 5

# Skip build if already compiled
bash scripts/benchmarks/run_all_microbench.sh --skip-build --n-reps 3

# Use CPU storage for large datasets
bash scripts/benchmarks/run_all_microbench.sh --skip-build --storage-device cpu
```

Output:
- `results/microbench_maximus_timing.csv` — per-query timing
- `results/microbench_maximus_metrics.csv` — GPU power/utilization samples

### Standard benchmarks

```bash
DATA_DIR=/workspace/gpu_db/tests
MAXIMUS_DIR=/workspace/gpu_db

# Timing (all queries together per scale factor)
python3 scripts/benchmarks/run_timing.py \
    --maximus-dir $MAXIMUS_DIR --data-dir $DATA_DIR \
    --output-dir results --n-reps 3 --storage-device gpu

# Metrics (per-query GPU sampling)
python3 scripts/benchmarks/run_metrics.py \
    --maximus-dir $MAXIMUS_DIR --data-dir $DATA_DIR \
    --output-dir results --n-reps 3 --sample-interval 50
```

### DuckDB baseline

```bash
python3 scripts/benchmarks/run_microbench_duckdb.py \
    --data-dir tests --output-dir results --n-reps 5
```

## Data Generation

### TPC-H

Uses DuckDB's built-in TPC-H data generator. Creates 8 CSV tables per scale factor.

```bash
python3 scripts/benchmarks/generate_tpch_data.py \
    --output-dir tests/tpch --scale-factors 1 10 20
```

```
tests/tpch/
├── sf1/     (~1 GB, 8 CSV tables: lineitem, orders, customer, part, partsupp, supplier, nation, region)
├── sf10/    (~11 GB)
└── sf20/    (~22 GB)
```

### H2O Groupby

Generates synthetic groupby benchmark data. Schema: id1-id3 (VARCHAR), id4-id6 (INTEGER), v1-v2 (INTEGER), v3 (DOUBLE).

```bash
python3 scripts/benchmarks/generate_h2o_data.py \
    --output-dir tests/h2o --scales 1 2 4 8
```

```
tests/h2o/
├── sf1/groupby.csv     (~1 GB, 65M rows)
├── sf2/groupby.csv     (~2 GB, 130M rows)
├── sf3/groupby.csv     (~3 GB, 190M rows)
└── sf4/groupby.csv     (~4 GB, 250M rows)
```

**Note**: For scales >= 5, the generator uses BIGINT casts to avoid INT32 overflow in DuckDB's `hash()` function.

### ClickBench

Downloads ClickBench `hits.parquet` (~15 GB) and creates sampled subsets with timestamp conversion and newline sanitization.

```bash
python3 scripts/benchmarks/generate_clickbench_data.py \
    --output-dir tests/clickbench --percentages 10 20
```

```
tests/clickbench/
├── hits.parquet    (downloaded, ~15 GB)
├── sf10/t.csv      (10% sample, ~7.5 GB)
└── sf20/t.csv      (20% sample, ~15 GB)
```

## Supported Queries

### Standard Benchmarks

| Suite | Queries | Notes |
|-------|---------|-------|
| TPC-H | q1-q22 (22 queries) | sf20: q17/q18/q19/q21 OOM |
| H2O | q1-q7, q9, q10 (9 queries) | sf4: q3/q7/q10 OOM; q8 not supported |
| ClickBench | q0-q42 (43 queries) | q18/q27/q28/q42 unsupported functions |

### Microbenchmarks (120 queries)

| Suite | Queries | Workload Types |
|-------|---------|----------------|
| H2O | 35 | w1(scan/agg), w2(filter), w3(low-card groupby), w4(high-card groupby), w6(sort) |
| TPC-H | 55 | w1(scan/agg), w2(filter), w3(low-card groupby), w4(high-card groupby), w5a(2-3 table joins), w5b(5-6 table joins), w6(sort/limit) |
| ClickBench | 30 | w1-w4, w6 (includes 5 cross-benchmark queries from TPC-H schema) |

Microbench source files:
- C++ query plans: `src/maximus/microbench/microbench_{h2o,tpch,clickbench}.{hpp,cpp}`
- SQL files (DuckDB): `microbench/{h2o,tpch,clickbench}/*.sql`

## Performance Notes

### `--storage-device=gpu` vs `--storage-device=cpu`

| Setting | Data Location | Best For |
|---------|--------------|----------|
| `gpu` | GPU VRAM (pre-loaded) | Production benchmarking (fastest) |
| `cpu` | System RAM | Large datasets that don't fit in GPU memory |

Always use `--storage-device gpu` for performance benchmarking. Example: TPC-H q12 sf10 is **19ms** (gpu) vs **722ms** (cpu).

### GPU Memory Limits (A100 80GB)

- sf1-sf10: fit comfortably with `--storage_device=gpu`
- sf20 (TPC-H): use `--storage_device=cpu` to avoid OOM on complex joins
- sf4 (H2O): use `--storage_device=cpu` for high-memory queries (q3/q7/q10)
- **Never run multiple benchmark processes in parallel** — each RMM pool allocates 50-90% of GPU memory

### GPU Metrics Collected

| Metric | Unit | Source |
|--------|------|--------|
| `power_w` | Watts | `nvidia-smi power.draw` |
| `gpu_util_pct` | % | `nvidia-smi utilization.gpu` |
| `mem_used_mb` | MiB | `nvidia-smi memory.used` |
| `pcie_gen` | - | `nvidia-smi pcie.link.gen.current` |

## Troubleshooting

### `maxbench not found`
Build with benchmarks enabled:
```bash
cmake -C initial_cache.cmake .. && cmake --build . -j$(nproc)
```

### GPU out of memory
- Reduce scale factors
- Use `--storage-device cpu`
- Ensure no other GPU processes are running (check `nvidia-smi`)

### No timing extracted
Check raw output files (`results/*_raw_*.txt`) for error details.

### ClickBench CSV parser error
Regenerate CSV data with `generate_clickbench_data.py` — it sanitizes newlines in string fields.
