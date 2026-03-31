# Experiment Categories

GPU 数据库 Energy-Performance 优化实验分为四个层次，从硬件到数据路径逐步深入。

## Category Overview

| Category | Focus | Knobs | Affects Power? | Affects Latency? | Status |
|----------|-------|-------|---------------|-----------------|--------|
| **A** | GPU Hardware | Power limit (250-450W), SM clock (600-3090 MHz) | **Yes** | **Yes** | ✅ Done |
| **B** | Memory/CPU Clock | Memory clock (405-15001 MHz), CPU frequency | **Yes** | **Yes** | ✅ Done |
| **C** | CUDA Software | Memory placement, operator fusion, CUDA env vars | No | **Partial** | ✅ Done |
| **D** | Data Path | D1: transfer compression; D2: CSV vs Parquet | No | **Yes** | 📋 Planned |

## Key Findings Chain

```
A: 硬件 knobs 定义了 energy-latency 的 Pareto front
   → 最优配置: PL=300-360W, CLK=1800-2400MHz (benchmark-dependent)

B: Memory clock 对 memory-bound 查询影响显著 (~2x between min/max)
   CPU frequency 对 CPU-storage 模式影响 ~30%

C: 软件 knobs 中只有 memory placement 有效 (pinned 2-4x faster)
   其余 CUDA runtime 参数无可测量差异
   → 结论: PCIe 传输是 CPU-storage 的核心瓶颈

D: (planned) 压缩 + 格式优化，减少传输量
   → 预期: LZ4 compressed transfer 20-50% faster
   → 预期: Parquet 加载速度 >> CSV
```

## Documents

- [Category C: CUDA Software Knobs](category_c_software_knobs.md)
- [Category D: Data Path Optimization](category_d_data_path_optimization.md)
- Category A/B: Results in `results/energy_sweep_v2/`, `results/mem_clock_sweep/`, `results/freq_sweep/`

## Result Directories

| Directory | Category | Content |
|-----------|----------|---------|
| `results/energy_sweep_v2/` | A | 12 GPU configs (4 PL × 3 CLK), Maximus + Sirius |
| `results/energy_sweep_v2_cpu/` | A | Same configs, CPU storage mode |
| `results/mem_clock_sweep/` | B | 15 configs (3 CLK × 5 MEM CLK) |
| `results/freq_sweep/` | B | 4 CPU/GPU frequency configs |
| `results/freq_experiment/` | B | Detailed frequency analysis |
| `results/cuda_knob_sweep/` | C | 14 software knob configs, SF1 + SF10 |
| `results/compression_experiment/` | D1 | (planned) 4 codecs × 2 storage × 3 SF |
| `results/format_experiment/` | D2 | (planned) 5 formats × 2 engines × 2 SF |
