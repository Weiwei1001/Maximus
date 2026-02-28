# Benchmark Results

GPU-accelerated SQL engine benchmark results on **NVIDIA RTX 5090** (32 GB GDDR7 VRAM).

## Systems Tested

| Engine | Type | Version |
|--------|------|---------|
| **Maximus** | Standalone GPU query engine (Apache Arrow + cuDF) | v0.2.0 |
| **Sirius** | DuckDB GPU extension | dev branch |

## Benchmark Suites

| Suite | Queries | Sirius Scale Factors | Maximus Scale Factors |
|-------|---------|---------------------|----------------------|
| TPC-H | 22 (Q1-Q22) | SF 1, 2, 10, 20 | SF 1, 2, 10 |
| H2O groupby | 9-10 | 1gb, 2gb, 3gb, 4gb | 1gb, 2gb, 3gb, 4gb |
| ClickBench | 39-43 | SF 10, 20, 50, 100 | SF 1, 2 |

## Timing Methodology

### Sirius
- Each (benchmark, SF) runs in a **separate DuckDB process** to avoid GPU memory leaks
- 3 passes, queries in batches of 10, **3rd pass timing recorded**
- Overhead subtracted: 8.3644s (TPC-H/ClickBench), 15.9511s (H2O)
- Status: `OK` (GPU success) or `FALLBACK` (fell back to CPU)

### Maximus
- `maxbench` binary with `-s gpu` (pre-loaded to VRAM)
- 50-100 repetitions per query, **minimum time reported**
- Load and run phases measured separately

## Data Files

| File | Description | Rows |
|------|-------------|------|
| `sirius_timing_per_query.csv` | Sirius per-query timing (all suites) | 312 |
| `maximus_adaptive.csv` | Maximus timing (all suites, 50 reps) | ~200 |
| `maximus_tpch_sf1_corrected.csv` | Maximus TPC-H SF1 (corrected) | 22 |
| `tpch_timing.csv` | Sirius TPC-H timing summary | 100 |
| `h2o_timing.csv` | Sirius H2O timing summary | 40 |
| `clickbench_timing.csv` | Sirius ClickBench timing summary | 172 |
| `*_metrics_samples_summary.csv` | GPU metrics summaries (power, memory, utilization) | varies |
| `plots/*.png` | Visualization charts | 10 files |

## Plots

| Plot | Description |
|------|-------------|
| `tpch_timing_by_sf.png` | TPC-H query timing grouped by scale factor |
| `h2o_timing_by_sf.png` | H2O query timing grouped by scale factor |
| `clickbench_timing_by_sf.png` | ClickBench query timing grouped by scale factor |
| `tpch_gpu_memory.png` | TPC-H max GPU memory per query |
| `h2o_gpu_memory.png` | H2O max GPU memory per query |
| `clickbench_gpu_memory.png` | ClickBench max GPU memory per query |
| `gpu_power_by_benchmark.png` | GPU power consumption box plots |
| `tpch_time_vs_memory.png` | TPC-H execution time vs GPU memory scatter |
| `timing_overview_heatmap.png` | All benchmarks/SFs timing heatmap |
| `tpch_scaling.png` | TPC-H scaling with data size |
