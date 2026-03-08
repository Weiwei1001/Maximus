# Benchmark Results

GPU-accelerated SQL engine benchmark results on **NVIDIA A100 80GB**.

## Engine

| Engine | Type | Stack |
|--------|------|-------|
| **Maximus** | Standalone GPU query engine | Apache Arrow Acero + cuDF 24.12 |

## Benchmark Suites

### Standard Benchmarks

| Suite | Queries | Scale Factors | Tests | Pass | Fail |
|-------|---------|---------------|-------|------|------|
| TPC-H | 22 (q1-q22) | sf1, sf10, sf20 | 66 | 61 | 5 (OOM sf20) |
| H2O | 9 (q1-q7, q9, q10) | sf1, sf2, sf3, sf4 | 36 | 33 | 3 (OOM sf4) |
| ClickBench | 43 (q0-q42) | sf1, sf10, sf20 | 129 | 117 | 12 (4 unimplemented x 3SF) |
| **Total** | **74** | | **231** | **211** | **20** |

### Microbenchmarks

| Suite | Queries | Scale Factors | Tests | Pass | Fail |
|-------|---------|---------------|-------|------|------|
| Micro H2O | 35 | sf1, sf5 | 70 | 66 | 4 (OOM sf5) |
| Micro TPC-H | 55 | sf1, sf5 | 110 | 110 | 0 |
| Micro ClickBench | 30 | sf5, sf10 | 60 | 60 | 0 |
| **Total** | **120** | | **240** | **236** | **4** |

### Overall

| Category | Pass/Total | Rate |
|----------|-----------|------|
| Standard | 211/231 | 91.3% |
| Microbench | 236/240 | 98.3% |
| **All** | **447/471** | **94.9%** |

## Timing Methodology

- `maxbench` binary with `--storage_device=gpu` (data pre-loaded to VRAM)
- 3-5 repetitions per query, **minimum time** reported
- CUDA stream barriers before/after each query ensure true GPU execution time

## Data Files

### Standard Benchmark Results

| File | Description |
|------|-------------|
| `tpch_timing.csv` | TPC-H sf1/sf10 timing (gpu storage) |
| `tpch_timing_sf20_cpu.csv` | TPC-H sf20 timing (cpu storage) |
| `h2o_timing.csv` | H2O sf1/sf2/sf3 timing (gpu storage) |
| `h2o_timing_sf4_cpu.csv` | H2O sf4 timing (cpu storage) |
| `clickbench_timing_full.csv` | ClickBench 43q x 4SF timing |
| `tpch_metrics_timings.csv` | TPC-H per-query metrics timing |
| `h2o_metrics_timings.csv` | H2O per-query metrics timing |
| `clickbench_metrics_timings.csv` | ClickBench per-query metrics timing |

### Microbench Results

| File | Description |
|------|-------------|
| `microbench_maximus_timing.csv` | 120 Maximus microbench timing (sf1/sf10) |
| `microbench_maximus_metrics.csv` | GPU metrics samples during microbench |
| `microbench_maximus_sf5_timing.csv` | 120 Maximus microbench timing (sf5) with metrics |
| `microbench_duckdb_timing.csv` | 120 DuckDB baseline timing |
| `microbench_duckdb_metrics.csv` | GPU metrics during DuckDB microbench |

### GPU Metrics Files

| File | Description |
|------|-------------|
| `*_metrics_samples.csv` | Raw 50ms GPU samples (power, utilization, memory) |
| `*_metrics_timings.csv` | Per-query timing from metrics runs |
| `*_raw_*.txt` | Raw maxbench stdout per scale factor |

### CSV Column Formats

**Timing CSV**: `benchmark, scale, query, min_ms, avg_ms, reps`

**Metrics Samples CSV**: `benchmark, scale, query, time_offset_ms, power_w, gpu_util_pct, mem_used_mb, pcie_gen`

**Microbench Timing CSV**: `engine, benchmark, query_id, workload, min_ms, avg_ms, max_ms, reps`
