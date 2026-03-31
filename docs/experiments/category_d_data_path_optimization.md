# Category D: Data Path Optimization — Compression & Format

## Overview

**目标:** 测量数据路径优化（压缩传输 + 数据格式选择）对 GPU 数据库查询延迟和能耗的影响。Category C 的结论表明 PCIe 传输是 CPU-storage 模式的核心瓶颈，本类实验探索两种减少传输开销的方法。

**动机来源:** Category C 发现 pinned memory 可带来 2-4x 加速，说明 CPU→GPU 数据传输是性能瓶颈。两个自然的优化方向：
1. **D1 — 压缩传输:** 在传输前压缩数据，减少 PCIe 传输量
2. **D2 — 数据格式:** 使用更紧凑的列式格式（Parquet），减少 I/O + 解析 + 传输开销

**实验日期:** 2026-03-30（计划）

---

## Sub-Experiment D1: GPU Transfer Compression

### Hypothesis

PCIe 4.0 x16 带宽 ~20 GB/s。如果能在 CPU 侧压缩数据后再传输：
- 压缩比 2-5x → 传输数据量减少 2-5x
- nvcomp GPU 解压速度 ~100 GB/s (LZ4) → 远大于 PCIe 带宽
- CPU 压缩速度 ~3-5 GB/s (LZ4) → 主要瓶颈

**净效果预测:**
- LZ4: 最佳平衡（CPU compress ~3 GB/s, 压缩比 ~2-3x）→ 预期正收益
- Snappy: 类似 LZ4，略低压缩比
- ZSTD: CPU compress ~0.5 GB/s → 可能反而变慢（CPU 压缩成为新瓶颈）

### Implementation Approach

**需要修改 Maximus 源代码。** 改动隔离在传输层：

```
原始路径 (storage=cpu):
  Arrow Table (CPU) → cudaMemcpy → Device Buffer (GPU)

压缩路径 (storage=cpu, compression=lz4):
  Arrow Table (CPU) → Arrow LZ4 compress (CPU) → cudaMemcpy (compressed)
    → nvcomp LZ4 decompress (GPU) → Device Buffer (GPU)
```

**修改文件列表:**

| File | Change | Purpose |
|------|--------|---------|
| `src/maximus/CMakeLists.txt` | Link nvcomp | 构建依赖 |
| `src/maximus/config.{hpp,cpp}` | Add `TransferCompression` enum | 配置: none/lz4/snappy/zstd |
| `src/maximus/context.hpp` | Add `transfer_compression` field | 运行时配置 |
| `src/maximus/gpu/compression/transfer_compressor.{hpp,cpp}` | **New** | CPU compress (Arrow) + GPU decompress (nvcomp) |
| `src/maximus/gpu/gtable/cuda/cuda_context.{hpp,cpp}` | Add compressed H2D path | 集成到现有传输 |
| `benchmarks/maxbench.cpp` | Add `--transfer_compression` flag | CLI 接口 |

**关键依赖:**
- nvcomp 5.1.0: `/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/`
  - Headers: `include/nvcomp/{lz4,snappy,zstd}.h`
  - Library: `lib64/libnvcomp.so.5`
- Arrow compression codecs: `arrow::util::Codec` (LZ4_FRAME, SNAPPY, ZSTD) — 已验证可用

**新环境变量:** `MAXIMUS_TRANSFER_COMPRESSION` = `none` | `lz4` | `snappy` | `zstd`
**新 CLI 参数:** `--transfer_compression` (same values)

### Experiment Matrix

| Dimension | Values |
|-----------|--------|
| Codecs | none (baseline), lz4, snappy, zstd |
| Storage | cpu, cpu-pinned |
| Scale Factors | 1, 2, 10 |
| Queries | q1, q6, q3, q9, q12 (TPC-H) |
| Reps | 20 per config |
| Metrics | min_ms, avg_ms, nvidia-smi power sampling |

**总配置数:** 4 codecs × 2 storage × 3 SF = 24 configs × 5 queries = 120 measurements

### Expected Results

| Codec | 压缩比 | CPU 压缩速度 | GPU 解压速度 | 预测 net 效果 |
|-------|--------|-------------|-------------|--------------|
| none | 1x | N/A | N/A | baseline |
| LZ4 | 2-3x | ~3 GB/s | ~100 GB/s | **正收益 (20-50% faster)** |
| Snappy | 1.5-2.5x | ~4 GB/s | ~80 GB/s | 微弱正收益 |
| ZSTD | 3-5x | ~0.5 GB/s | ~30 GB/s | **负收益 (CPU 瓶颈)** |

### Result Files (planned)

| File | Description |
|------|-------------|
| `results/compression_experiment/compression_summary.csv` | 全量结果 |
| `scripts/run_compression_experiment.py` | 实验脚本 |

### Implementation Plan

详细实现步骤见: `docs/superpowers/plans/2026-03-30-experiment2-compression.md` (7 Tasks)

---

## Sub-Experiment D2: Data Format Impact (CSV vs Parquet)

### Hypothesis

不修改引擎代码，仅通过改变输入数据格式来影响性能：

**CSV 特点:**
- 文本格式，需要解析（string → typed columns）
- 无压缩，文件大
- 行式存储，需要全部读取

**Parquet 特点:**
- 二进制列式格式，解析开销小
- 内置压缩（Snappy/ZSTD/LZ4），文件小 3-7x
- 列裁剪（只读需要的列）
- cuDF 原生支持 Parquet 读取 + GPU 端解压

**预期数据大小 (TPC-H SF1):**

| Format | Estimated Size | vs CSV |
|--------|---------------|--------|
| CSV | ~1.1 GB | 1x |
| Parquet (uncompressed) | ~300-400 MB | ~3x smaller |
| Parquet (Snappy) | ~200-250 MB | ~5x smaller |
| Parquet (ZSTD) | ~150-200 MB | ~6-7x smaller |
| Parquet (LZ4) | ~220-270 MB | ~4-5x smaller |

### Implementation Approach

**不需要修改任何引擎代码。** Maximus 已支持 Parquet (`database_catalogue.cpp` 自动检测文件扩展名)，Sirius 通过 DuckDB 支持所有格式。

**步骤:**

1. **数据生成:** 将现有 CSV 数据转换为 4 种 Parquet 变体
   - 工具: PyArrow (`pyarrow.parquet.write_table()` with compression parameter)
   - 输出: `tests/tpch/parquet-{codec}-{sf}/` (codec ∈ {none, snappy, zstd, lz4})

2. **Maximus 实验:** 直接将 maxbench `--path` 指向 Parquet 目录
   - maxbench 自动检测 `.parquet` 文件并使用 cuDF Parquet reader

3. **Sirius 实验:** 创建 DuckDB 数据库从不同格式源加载
   - 注意: DuckDB 在导入时将数据转为内部格式，因此源格式只影响加载时间
   - Sirius 的 GPU query latency 在不同源格式间应该相同

4. **CPU 基线:** DuckDB (无 GPU) 在各格式上的表现
   - DuckDB 对 Parquet 有原生优化（谓词下推、列裁剪）

### Experiment Matrix

#### D2a: Maximus GPU

| Dimension | Values |
|-----------|--------|
| Formats | csv, parquet-none, parquet-snappy, parquet-zstd, parquet-lz4 |
| Storage | gpu, cpu |
| Scale Factors | 1, 2 (SF10 gpu-storage OOM) |
| Queries | q1, q6, q3, q9, q12 |
| Reps | 20 |
| Metrics | load_ms (数据加载时间), min_ms (查询时间), nvidia-smi power |

**总配置数:** 5 formats × 2 storage × 2 SF = 20 configs × 5 queries = 100 measurements

#### D2b: Sirius GPU

| Dimension | Values |
|-----------|--------|
| Formats | csv, parquet-none, parquet-snappy, parquet-zstd, parquet-lz4 |
| Scale Factors | 1, 2 |
| Queries | q1, q3, q6, q12 |
| Reps | 20 |

**注意:** Sirius 通过 DuckDB 导入数据到内部表，然后 `gpu_processing()` 查询。源格式只影响导入阶段。

#### D2c: CPU Baseline (DuckDB, no GPU)

| Dimension | Values |
|-----------|--------|
| Formats | csv, parquet-none, parquet-snappy, parquet-zstd, parquet-lz4 |
| Scale Factors | 1, 2 |
| Queries | q1, q3, q6, q12 |
| Reps | 10 |

### Expected Results

**GPU-resident (storage=gpu) 场景:**
- **加载时间:** Parquet << CSV (Parquet 二进制列式 → 更少解析; 压缩 → 更少 I/O)
- **查询时间:** 所有格式相同（数据在 GPU 内存中已经是 columnar format）
- **总能耗:** Parquet 更低（减少加载阶段的 GPU idle power 消耗）

**CPU-storage (storage=cpu) 场景:**
- **查询时间:** Parquet 可能更快（cuDF Parquet reader 直接在 GPU 上解压 + 列裁剪）
- **CSV 劣势:** 需要全量 CPU 解析 → 大量 CPU→GPU 传输

**CPU 基线:**
- **DuckDB 对 Parquet 原生优化:** 支持谓词下推、列裁剪 → Parquet 明显快于 CSV
- **CSV 需要完整解析:** DuckDB 的 CSV reader 虽然优化了，但仍然比 Parquet 慢

### Key Files

| File | Description |
|------|-------------|
| `scripts/generate_parquet_data.py` | CSV → Parquet 转换 (4 codecs × all SF) |
| `scripts/run_format_experiment.py` | Maximus GPU 格式实验 |
| `scripts/run_format_experiment_sirius.py` | Sirius GPU 格式实验 |
| `scripts/run_format_experiment_cpu.py` | DuckDB CPU 基线 |
| `scripts/setup_sirius_parquet.py` | 创建 Sirius DuckDB 数据库 |
| `scripts/run_format_energy.py` | 统一能耗测量 |
| `scripts/plot_format_results.py` | 结果分析 + 可视化 |
| `results/format_experiment/maximus_format_summary.csv` | Maximus 结果 |
| `results/format_experiment/sirius_format_summary.csv` | Sirius 结果 |
| `results/format_experiment/cpu_format_summary.csv` | CPU 基线结果 |

### Implementation Plan

详细实现步骤见: `docs/superpowers/plans/2026-03-30-experiment3-data-format.md` (6 Tasks)

---

## Execution Order

建议先做 **D2 (数据格式)**，后做 **D1 (压缩传输)**：

1. **D2 先行:** 不需要修改任何引擎代码，只需生成数据 + 跑实验
   - 可以快速获得 Parquet vs CSV 的 baseline 数据
   - 验证 cuDF 的 Parquet reader 是否比 CSV reader 更快

2. **D1 后续:** 需要修改 Maximus 源码 + 链接 nvcomp
   - D2 的结果可以作为 D1 效果的参照
   - 如果 Parquet (已有压缩) 效果显著，D1 的压缩传输可能边际收益更小

---

## Relationship to Other Categories

```
Category A: GPU Hardware Knobs          ─── 影响 Power + Latency
  (power limit, SM clock)                   ↓ 决定 "硬件天花板"
                                            │
Category B: Memory/CPU Clock            ─── 影响 Power + Latency
  (memory clock, CPU freq)                  ↓ 决定 "带宽天花板"
                                            │
Category C: CUDA Software Knobs         ─── 仅影响 Latency
  (memory placement, fusion, env vars)      ↓ 发现 "传输是瓶颈"
                                            │
Category D: Data Path Optimization      ─── 仅影响 Latency (through I/O + transfer)
  D1: Transfer compression (nvcomp)         → 减少 PCIe 传输量
  D2: Data format (CSV vs Parquet)          → 减少 I/O + 解析 + 传输
```

**核心逻辑链:**
- A/B → 确定了硬件级的 energy-latency Pareto front
- C → 发现软件层唯一有效 knob 是 memory placement，说明 **传输是瓶颈**
- D → 既然传输是瓶颈，通过 **压缩** 和 **格式优化** 来减少传输量

---

## Metrics Definition

所有 Category D 实验统一使用以下指标：

| Metric | Unit | Measurement Method |
|--------|------|-------------------|
| `load_ms` | milliseconds | maxbench 数据加载阶段计时 |
| `min_ms` | milliseconds | 查询执行时间 (min of N reps, excludes loading) |
| `avg_ms` | milliseconds | 查询执行时间 (mean of N reps) |
| `avg_power_w` | watts | nvidia-smi 50ms 采样, steady-state average |
| `energy_j` | joules | avg_power_w × min_ms / 1000 |
| `cpu_energy_j` | joules | Intel RAPL package energy during query |
| `compression_ratio` | dimensionless | original_size / compressed_size |
| `file_size_mb` | megabytes | 输入数据文件总大小 |
