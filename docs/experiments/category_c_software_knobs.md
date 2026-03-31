# Category C: CUDA Software Knob Impact on Latency and Energy

## Overview

**目标:** 测量 CUDA 软件层 knobs 对 GPU 数据库查询延迟和能耗的影响。与 Category A/B（硬件 knobs: power limit, SM clock, memory clock, CPU frequency）不同，本类实验聚焦于 CUDA runtime 和内存管理层面的软件可调参数。

**核心发现:** 在所有测试的 CUDA 软件 knobs 中，**memory placement 策略（pinned vs pageable vs gpu-resident）是唯一具有显著影响的 knob**，其余 CUDA runtime 参数在 GPU 数据库 workload 下基本无可测量差异。

**实验日期:** 2026-03-30

---

## Hypothesis

软件层面的 knobs 不直接影响 GPU 瞬时功率（power），但通过改变查询延迟（latency）间接影响总能耗（energy = power × time）。预期影响排序：

| Rank | Knob | 预估影响 | 原因 |
|------|------|---------|------|
| 1 | Pinned vs Pageable Memory | 20-60% | H2D DMA 直传 vs OS page fault |
| 2 | Operator Fusion | 10-40% | 减少中间结果 + kernel launch 开销 |
| 3 | RMM Pinned Pool Size | 2-10% | 影响分配延迟 |
| 4 | CUDA_DEVICE_MAX_CONNECTIONS | 0-5% | 并发 kernel 硬件队列数 |
| 5 | CUDA_AUTO_BOOST | 0-3% | GPU 自动 boost 频率 |

ECC mode 未测试（RTX 5080 不支持 ECC）。

---

## Methodology

### 测试配置

**Benchmark:** TPC-H, Scale Factor 1 和 10
**Queries:** q1 (scan+aggregation), q6 (filter+aggregation), q3 (2-way join), q9 (complex multi-join), q12 (join+aggregation, SF10 only)
**Hardware:** NVIDIA RTX 5080 (16GB, GPU index 1), Intel Xeon w5-2455X
**GPU State:** SM clock 未锁定 (auto boost), Power limit 360W (default)
**Measurement:** min of N reps (SF1: 30 reps, SF10: 10 reps)

### 测试 Knobs

| Knob | 测试值 | 控制方式 |
|------|--------|---------|
| Storage Device | `gpu`, `cpu`, `cpu-pinned` | maxbench `-s` flag |
| Operator Fusion | `true`, `false` | `MAXIMUS_OPERATORS_FUSION` env var |
| Pinned Pool Size | 1GB, 4GB, 8GB, 12GB | `MAXIMUS_MAX_PINNED_POOL_SIZE` env var |
| CUDA Max Connections | 1, 8, 32 | `CUDA_DEVICE_MAX_CONNECTIONS` env var |
| CUDA Auto Boost | 0, 1 | `CUDA_AUTO_BOOST` env var |

### 实验脚本

- SF1 sweep: `scripts/run_cuda_knob_sweep.py` (14 configs × 4 queries × 30 reps)
- SF10 sweep: `scripts/run_cuda_knob_sweep_sf10.py` (12 configs × 5 queries × 10 reps)

---

## Results

### SF1 (TPC-H, ~1.1GB CSV, 数据可完全放入 GPU)

```
Config                        q1     q6     q3     q9    (min_ms)
─────────────────────────────────────────────────────────────────
storage_gpu                    4      0      2      3    ← baseline
storage_cpu (pageable)        28     15     20     32    ← 7-15x slower
storage_cpu-pinned             9      3      6      9    ← 2-3x vs gpu

fusion_true                    4      1      2      3    ← no difference
fusion_false                   4      0      2      3

pinned_pool_1gb                4      0      2      3    ← no difference
pinned_pool_4gb                4      0      2      3
pinned_pool_8gb                4      0      2      3
pinned_pool_12gb               4      0      2      3

max_conn_1                     4      0      2      3    ← no difference
max_conn_8                     4      0      2      3
max_conn_32                    4      0      2      3

auto_boost_0                   4      0      2      3    ← no difference
auto_boost_1                   4      0      2      3
```

### SF10 (TPC-H, ~11GB CSV, 数据无法完全放入 GPU)

注意: `storage=gpu` 在 SF10 下 OOM (11GB CSV > 16GB VRAM after RMM pool allocation)。

```
Config                        q1     q6     q3     q9    q12   (min_ms)
──────────────────────────────────────────────────────────────────────
storage_cpu (pageable)       307    159    201    318    906
storage_cpu-pinned           128     38     53    330    352
  → pinned speedup:        2.4x   4.2x   3.8x   ~1x   2.6x

pinned_1gb  (cpu-pinned)     103     41     53    322    349
pinned_4gb  (cpu-pinned)     132     39     59    317    338
pinned_8gb  (cpu-pinned)     125     39     58    316    345
pinned_12gb (cpu-pinned)     112     39     53    335    348
  → pool size: 微弱波动 (~10-20%), 无单调趋势

conn_1  (cpu, pageable)      312    155    202    324    927
conn_8  (cpu, pageable)      309    154    203    327    925
conn_32 (cpu, pageable)      317    157    207    340    950
  → MAX_CONNECTIONS: 无差异
```

---

## Analysis

### 1. Memory Placement — THE Dominant Knob

**Pageable → Pinned: 2-4x speedup**

| Query | Pageable (ms) | Pinned (ms) | Speedup | 原因 |
|-------|--------------|-------------|---------|------|
| q1 (scan+agg) | 307 | 128 | 2.4x | Large lineitem scan, dominated by transfer |
| q6 (filter+agg) | 159 | 38 | 4.2x | Simple filter, transfer overhead is majority |
| q3 (join) | 201 | 53 | 3.8x | Join tables need fast transfer |
| q9 (complex) | 318 | 330 | ~1x | GPU compute dominates, transfer is small fraction |
| q12 (join+agg) | 906 | 352 | 2.6x | Large join benefits from fast transfer |

**解释:** Pinned memory 允许 GPU 通过 DMA 直接访问 host memory，绕过 OS page table 翻译。对于 memory-bound 查询 (q1, q6, q3)，数据传输是瓶颈，pinned memory 的效果最明显。对于 compute-bound 查询 (q9)，GPU 计算时间占主导，传输加速效果被稀释。

### 2. Operator Fusion — 无可测量差异

SF1 数据太小（查询时间 0-4ms），fusion 的 kernel launch saving（~10-100μs per kernel）被测量噪声掩盖。SF10 gpu-storage OOM 无法测试。

**推论:** 在当前 cuDF 实现下，operator fusion 的效果可能已经被 cuDF 内部的 kernel 合并优化所吸收。需要更大规模或更多 operator chain 的查询来验证。

### 3. Pinned Pool Size — 无单调影响

1GB 到 12GB 的 pinned pool size 变化没有产生一致的性能差异。可能的原因：
- Maximus 的 pinned pool 主要用于 cuDF 内部的临时分配，不是 H2D 传输的瓶颈
- Arrow 表在 CPU 上不在 pinned memory 中分配（除非 `storage=cpu-pinned`）

### 4. CUDA Runtime Parameters — 完全无影响

`CUDA_DEVICE_MAX_CONNECTIONS` 和 `CUDA_AUTO_BOOST` 在数据库 workload 下无效：
- **MAX_CONNECTIONS:** GPU 数据库是单 pipeline 执行，没有并发 kernel launch 竞争
- **AUTO_BOOST:** 当 clock 未被 lock 时，GPU 已经在自动 boost 到最高频率

---

## Conclusions

1. **Memory placement 是唯一有实际意义的 CUDA 软件 knob**，其他参数在 GPU 数据库 workload 下无可测量影响
2. **Pinned memory 的 2-4x 加速**证实了 PCIe 传输带宽是 CPU-storage 模式下的核心瓶颈
3. 这直接激发了 Category D 实验的动机：
   - **数据压缩** (D1): 既然 PCIe 传输是瓶颈，压缩数据可以减少传输量
   - **数据格式** (D2): Parquet 比 CSV 更紧凑，可能减少 I/O + 传输时间

---

## Result Files

| File | Description |
|------|-------------|
| `results/cuda_knob_sweep/knob_sweep_summary.csv` | SF1 全部 14 configs, 4 queries |
| `results/cuda_knob_sweep/knob_sweep_sf10_summary.csv` | SF10 全部 12 configs, 5 queries |
| `scripts/run_cuda_knob_sweep.py` | SF1 实验脚本 |
| `scripts/run_cuda_knob_sweep_sf10.py` | SF10 实验脚本 |

---

## Relationship to Other Categories

| Category | Focus | Key Knobs | Affects Power? | Affects Latency? |
|----------|-------|-----------|---------------|-----------------|
| **A** | GPU Hardware | Power limit, SM clock | Yes | Yes |
| **B** | Memory Clock + CPU Freq | Memory clock, CPU frequency | Yes | Yes |
| **C (this)** | CUDA Software | Memory placement, fusion, CUDA env vars | **No** | **Yes (memory only)** |
| **D** | Data Path Optimization | Compression, file format (CSV/Parquet) | **No** | **Yes** |
