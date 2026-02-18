# Maximus Benchmark Suite

End-to-end benchmark scripts for evaluating Maximus GPU query engine performance across three standard OLAP benchmarks: **TPC-H**, **H2O groupby**, and **ClickBench**.

## Overview

| Script | Purpose |
|--------|---------|
| `run_all.py` | Full pipeline: data generation + timing + metrics |
| `generate_tpch_data.py` | Generate TPC-H data (CSV) using DuckDB |
| `generate_h2o_data.py` | Generate H2O groupby synthetic data (CSV) |
| `generate_clickbench_data.py` | Download & sample ClickBench data (CSV) |
| `run_timing.py` | Run timing benchmarks (all queries per scale) |
| `run_metrics.py` | Run metrics benchmarks (per-query GPU sampling) |

## Prerequisites

- **Maximus** built with GPU support (`MAXIMUS_WITH_GPU=ON`)
- **Python 3.8+** with `duckdb` package (`pip install duckdb`)
- **NVIDIA GPU** with drivers and `nvidia-smi` available
- **CUDA 12.0+** and **cuDF** installed

## Quick Start

### One-command full pipeline

```bash
# Run everything: generate data, run timing, run metrics
python scripts/benchmarks/run_all.py \
    --maximus-dir /path/to/Maximus \
    --data-dir /path/to/benchmark_data \
    --n-reps 3
```

### Step-by-step

```bash
DATA_DIR=/path/to/benchmark_data
MAXIMUS_DIR=/path/to/Maximus

# Step 1: Generate data
python scripts/benchmarks/generate_tpch_data.py \
    --output-dir $DATA_DIR/tpch \
    --scale-factors 1 2 10 20

python scripts/benchmarks/generate_h2o_data.py \
    --output-dir $DATA_DIR/h2o \
    --scales 1 2 3 4

python scripts/benchmarks/generate_clickbench_data.py \
    --output-dir $DATA_DIR/clickbench \
    --percentages 10 20

# Step 2: Run timing benchmarks
python scripts/benchmarks/run_timing.py \
    --maximus-dir $MAXIMUS_DIR \
    --data-dir $DATA_DIR \
    --n-reps 3 \
    --storage-device gpu

# Step 3: Run metrics benchmarks
python scripts/benchmarks/run_metrics.py \
    --maximus-dir $MAXIMUS_DIR \
    --data-dir $DATA_DIR \
    --n-reps 3 \
    --sample-interval 50
```

## Data Generation

### TPC-H

Uses DuckDB's built-in TPC-H data generator. Creates 8 CSV tables per scale factor.

```
$DATA_DIR/tpch/
├── sf1/          (~1 GB)
│   ├── lineitem.csv
│   ├── orders.csv
│   ├── customer.csv
│   ├── part.csv
│   ├── partsupp.csv
│   ├── supplier.csv
│   ├── nation.csv
│   └── region.csv
├── sf2/          (~2 GB)
├── sf10/         (~11 GB)
└── sf20/         (~22 GB)
```

**Default scale factors**: SF 1, 2, 10, 20

### H2O Groupby

Generates synthetic groupby benchmark data with the standard schema:

| Column | Type | Description |
|--------|------|-------------|
| id1-id3 | VARCHAR | Categorical string columns |
| id4-id6 | INTEGER | Categorical integer columns |
| v1-v2 | INTEGER | Value columns (1-100) |
| v3 | DOUBLE | Value column (0-1000) |

```
$DATA_DIR/h2o/
├── sf1/groupby.csv     (~1 GB, 65M rows)
├── sf2/groupby.csv     (~2 GB, 130M rows)
├── sf3/groupby.csv     (~3 GB, 190M rows)
└── sf4/groupby.csv     (~4 GB, 250M rows)
```

**Default scales**: 1, 2, 3, 4 (in GB)

### ClickBench

Downloads the ClickBench `hits.parquet` (~15 GB) and creates sampled subsets. Handles timestamp conversion (`EventTime`/`EventDate`) and newline sanitization for Maximus compatibility.

```
$DATA_DIR/clickbench/
├── hits.parquet        (downloaded, ~15 GB)
├── sf10/t.csv          (10% sample, ~7.5 GB)
└── sf20/t.csv          (20% sample, ~15 GB)
```

**Default percentages**: 10%, 20%

## Benchmark Execution

### Timing (`run_timing.py`)

Runs all queries together per scale factor for accurate throughput measurement.

**Key options:**
- `--storage-device gpu` (recommended): Keep data in GPU memory for best performance
- `--storage-device cpu`: Data stays in CPU memory (includes PCIe transfer overhead)
- `--n-reps 3`: Number of repetitions per query

**Output**: `{benchmark}_timing.csv` with columns:
```
benchmark, scale, query, min_ms, avg_ms, reps
tpch, sf1, q1, 5, 17, "43,5,5"
tpch, sf1, q2, 4, 6, "10,4,4"
...
```

Results are **appended in real-time** after each scale factor completes.

### Metrics (`run_metrics.py`)

Runs each query individually while sampling GPU metrics via `nvidia-smi`.

**Key options:**
- `--sample-interval 50`: Sample GPU every 50ms (default)
- `--n-reps 3`: Repetitions per query

**Output files:**

1. `{benchmark}_metrics_timings.csv` — Per-query timing:
   ```
   benchmark, scale, query, min_ms, avg_ms, reps
   ```

2. `{benchmark}_metrics_samples.csv` — GPU samples during execution:
   ```
   benchmark, scale, query, time_offset_ms, power_w, gpu_util_pct, mem_used_mb, pcie_gen
   ```

Both files are **appended in real-time** after each query completes.

### GPU Metrics Collected

| Metric | Unit | Description |
|--------|------|-------------|
| `power_w` | Watts | GPU power draw |
| `gpu_util_pct` | % | GPU compute utilization |
| `mem_used_mb` | MiB | GPU memory used |
| `pcie_gen` | - | Current PCIe link generation |

## Supported Queries

### TPC-H (22 queries)

All 22 standard TPC-H queries: q1-q22

**Note**: Some queries may fail at SF20 due to GPU memory constraints on complex joins (q3-q5, q7-q10, q12, q18-q21).

### H2O (9 queries)

Standard H2O groupby queries: q1-q7, q9, q10 (q8 not supported by Maximus)

| Query | Description |
|-------|-------------|
| q1 | `GROUP BY id1, SUM(v1)` |
| q2 | `GROUP BY id1, id2, SUM(v1)` |
| q3 | `GROUP BY id3, SUM(v1), MEAN(v3)` |
| q4 | `MEAN(v1), MEAN(v2), MEAN(v3) BY id6` |
| q5 | `GROUP BY id4, SUM(v1), SUM(v2), SUM(v3)` |
| q6 | `GROUP BY id4, id5, MEDIAN(v3), STDDEV(v3)` |
| q7 | `GROUP BY id3, MAX(v1)-MIN(v2)` |
| q9 | `GROUP BY id2, id4, top 2 (v1)` |
| q10 | `GROUP BY id1-id6, SUM(v3)` (most expensive) |

### ClickBench (25 GPU-supported queries)

q3, q6, q8-q17, q19, q21-q26, q30-q35

**Unsupported on GPU**: q0-q2, q4-q5, q7, q18, q20, q27-q29, q36-q42 (require aggregation functions not available in cuDF)

## Performance: `storage_device=gpu` vs `storage_device=cpu`

The `--storage-device` flag has a **dramatic impact** on performance:

| Setting | Data Location | Transfer Overhead | Best For |
|---------|--------------|-------------------|----------|
| `gpu` | GPU VRAM | None | Production benchmarking |
| `cpu` | System RAM | PCIe transfer each query | Testing PCIe bandwidth |

**Example (TPC-H q12, SF10):**
- `storage_device=cpu`: 722ms (includes ~15s CSV load + PCIe transfer)
- `storage_device=gpu`: **19ms** (data pre-loaded in VRAM)

**Always use `--storage-device gpu` for performance benchmarking.**

## Example Results (A100-80GB)

### TPC-H SF10, storage_device=gpu, 3 reps

| Query | Min (ms) | Query | Min (ms) |
|-------|---------|-------|---------|
| q1 | 39 | q12 | 19 |
| q2 | 7 | q13 | 202 |
| q3 | 17 | q14 | 13 |
| q4 | 14 | q15 | 11 |
| q5 | 33 | q16 | 54 |
| q6 | 7 | q17 | 19 |
| q7 | 18 | q18 | 16 |
| q8 | 22 | q19 | 226 |
| q9 | 46 | q20 | 22 |
| q10 | 14 | q21 | 474 |
| q11 | 20 | q22 | 8 |
| **Total** | **1,301ms** | | |

## Estimated Runtime

Full pipeline (data generation + timing + metrics) on A100-80GB:

| Phase | Time |
|-------|------|
| TPC-H data generation (SF 1,2,10,20) | ~5 min |
| H2O data generation (1-4 GB) | ~5 min |
| ClickBench data (download + sample) | ~10 min |
| Timing benchmarks (all 3) | ~5 min |
| Metrics benchmarks (all 3) | ~15 min |
| **Total** | **~40 min** |

## Troubleshooting

### `maxbench not found`
Build Maximus with benchmarks enabled:
```bash
cmake -DMAXIMUS_WITH_GPU=ON -DMAXIMUS_WITH_BENCHMARKS=ON ..
make -j$(nproc)
```

### GPU out of memory
- Reduce scale factors (e.g., `--tpch-scales 1 2` instead of `1 2 10 20`)
- Use `--storage-device cpu` (slower but uses less VRAM)

### ClickBench CSV parser error
The `generate_clickbench_data.py` script automatically sanitizes newlines in string fields. If you see "CSV parser got out of sync", regenerate the CSV data.

### No timing extracted
Some queries may crash silently on large datasets. Check the raw output files (`*_raw_*.txt`) for error details.
