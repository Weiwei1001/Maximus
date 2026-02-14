# Maximus GPU Edition - Complete Guide

[English](#english) | [中文](#中文)

---

## English

### Quick Start (One Command)

```bash
bash <(curl -s https://raw.githubusercontent.com/Weiwei1001/Maximus/main/scripts/deploy_gpu.sh)
```

After deployment:

```bash
cd ~/Maximus/build
source setup_env.sh

# Test with single query
./benchmarks/maxbench --benchmark=tpch --queries=q1 \
    --device=gpu --storage_device=cpu --engines=maximus --n_reps=1 \
    --path=../tests/tpch/csv-0.01

# Full benchmark (all 22 TPC-H queries)
./benchmarks/maxbench --benchmark=tpch \
    --queries=q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22 \
    --device=gpu --storage_device=cpu --engines=maximus --n_reps=3 \
    --path=../tests/tpch/csv-0.01
```

### What's New in This Release

- ✅ Full GPU support with cuDF 24.12
- ✅ All 22 TPC-H queries pass on NVIDIA A100
- ✅ 5-50x speedup vs CPU depending on query
- ✅ One-command deployment script
- ✅ Comprehensive testing suite
- ✅ Detailed installation guide

### System Requirements

- NVIDIA GPU with CUDA 12.0+
- Ubuntu 20.04+ or CentOS 8+
- 16GB+ RAM, 50GB+ disk space
- Modern C++ compiler (gcc/clang 11+)

### Performance

**Average query execution time (after warmup):**

| Query | Time (ms) | Query | Time (ms) |
|-------|-----------|-------|-----------|
| q1 | 2 | q12 | 4 |
| q2 | 4 | q13 | 4 |
| q3 | 1 | q14 | 4 |
| q4 | 1 | q15 | 4 |
| q5 | 2 | q16 | 8 |
| q6 | 0 | q17 | 4 |
| q7 | 3 | q18 | 4 |
| q8 | 3 | q19 | 5 |
| q9 | 3 | q20 | 6 |
| q10 | 7 | q21 | 10 |
| q11 | 3 | q22 | 5 |

### Installation Files

- **INSTALL_GPU.md** - Detailed step-by-step installation guide
- **scripts/deploy_gpu.sh** - One-command deployment script
- **scripts/test_gpu.sh** - Comprehensive testing suite
- **BENCHMARKS.md** - Detailed performance results and analysis

### Directory Structure

```
Maximus/
├── README_GPU.md              # This file
├── INSTALL_GPU.md             # Detailed installation guide
├── BENCHMARKS.md              # Performance benchmarks
├── scripts/
│   ├── deploy_gpu.sh          # One-command deployment
│   ├── test_gpu.sh            # Testing suite
│   ├── build_arrow.sh         # Arrow build script
│   └── build_taskflow.sh      # Taskflow build script
├── src/                        # Source code (GPU-enabled)
├── tests/
│   └── tpch/
│       ├── csv-0.01/          # TPC-H test data (1%)
│       └── csv/               # TPC-H full data
└── benchmarks/
    └── maxbench               # TPC-H benchmark executable
```

### Troubleshooting

**CUDA not found:**
```bash
export PATH=/usr/local/cuda/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH
```

**Build errors:**
```bash
# Check dependencies
cmake --version  # Need 3.17+
ninja --version  # Need 1.10+
nvcc --version   # Need 12.0+

# Clean and rebuild
cd ~/Maximus/build
rm -rf *
cmake ... (repeat cmake command)
make clean && make -j 8
```

**Library path issues:**
```bash
# Set library path before running
export LD_LIBRARY_PATH="/arrow_install/lib:$CONDA_PREFIX/lib:$(pwd)/lib:$LD_LIBRARY_PATH"
```

### Next Steps

1. Read **INSTALL_GPU.md** for detailed setup instructions
2. Run **deploy_gpu.sh** for one-command deployment
3. Use **test_gpu.sh** to verify the installation
4. Check **BENCHMARKS.md** for performance expectations

### Support

For issues or questions:
- Check the troubleshooting section above
- Review INSTALL_GPU.md for detailed steps
- Check benchmark logs in `results.csv`

---

## 中文

### 快速开始（一键命令）

```bash
bash <(curl -s https://raw.githubusercontent.com/Weiwei1001/Maximus/main/scripts/deploy_gpu.sh)
```

部署完成后：

```bash
cd ~/Maximus/build
source setup_env.sh

# 单查询测试
./benchmarks/maxbench --benchmark=tpch --queries=q1 \
    --device=gpu --storage_device=cpu --engines=maximus --n_reps=1 \
    --path=../tests/tpch/csv-0.01

# 完整基准测试（全部22个TPC-H查询）
./benchmarks/maxbench --benchmark=tpch \
    --queries=q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22 \
    --device=gpu --storage_device=cpu --engines=maximus --n_reps=3 \
    --path=../tests/tpch/csv-0.01
```

### 本次发布的新功能

- ✅ 完整的 cuDF 24.12 GPU 支持
- ✅ 全部 22 个 TPC-H 查询在 NVIDIA A100 上通过
- ✅ 相比 CPU 快 5-50 倍（取决于查询类型）
- ✅ 一键部署脚本
- ✅ 完整的测试套件
- ✅ 详细的安装指南

### 系统要求

- NVIDIA GPU，支持 CUDA 12.0+
- Ubuntu 20.04+ 或 CentOS 8+
- 16GB+ 内存，50GB+ 磁盘空间
- C++ 编译器（gcc/clang 11+）

### 性能指标

**平均查询执行时间（预热后）：**

| 查询 | 时间 (ms) | 查询 | 时间 (ms) |
|------|-----------|------|-----------|
| q1 | 2 | q12 | 4 |
| q2 | 4 | q13 | 4 |
| q3 | 1 | q14 | 4 |
| q4 | 1 | q15 | 4 |
| q5 | 2 | q16 | 8 |
| q6 | 0 | q17 | 4 |
| q7 | 3 | q18 | 4 |
| q8 | 3 | q19 | 5 |
| q9 | 3 | q20 | 6 |
| q10 | 7 | q21 | 10 |
| q11 | 3 | q22 | 5 |

### 关键文件

- **INSTALL_GPU.md** - 详细的逐步安装指南
- **scripts/deploy_gpu.sh** - 一键部署脚本
- **scripts/test_gpu.sh** - 完整的测试套件
- **BENCHMARKS.md** - 详细的性能测试结果

### 目录结构

```
Maximus/
├── README_GPU.md              # 本文件
├── INSTALL_GPU.md             # 详细安装指南
├── BENCHMARKS.md              # 性能基准测试
├── scripts/
│   ├── deploy_gpu.sh          # 一键部署脚本
│   ├── test_gpu.sh            # 测试套件
│   ├── build_arrow.sh         # Arrow 编译脚本
│   └── build_taskflow.sh      # Taskflow 编译脚本
├── src/                        # 源代码（GPU 启用）
├── tests/
│   └── tpch/
│       ├── csv-0.01/          # TPC-H 测试数据（1%）
│       └── csv/               # TPC-H 完整数据
└── benchmarks/
    └── maxbench               # TPC-H 基准测试程序
```

### 故障排除

**找不到 CUDA：**
```bash
export PATH=/usr/local/cuda/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH
```

**编译错误：**
```bash
# 检查依赖版本
cmake --version  # 需要 3.17+
ninja --version  # 需要 1.10+
nvcc --version   # 需要 12.0+

# 清理并重新编译
cd ~/Maximus/build
rm -rf *
cmake ... (重复 cmake 命令)
make clean && make -j 8
```

**库路径问题：**
```bash
# 运行前设置库路径
export LD_LIBRARY_PATH="/arrow_install/lib:$CONDA_PREFIX/lib:$(pwd)/lib:$LD_LIBRARY_PATH"
```

### 后续步骤

1. 阅读 **INSTALL_GPU.md** 获取详细的设置说明
2. 运行 **deploy_gpu.sh** 一键部署
3. 使用 **test_gpu.sh** 验证安装
4. 查看 **BENCHMARKS.md** 了解性能期望

### 获得帮助

遇到问题或有疑问：
- 查看上面的故障排除部分
- 参考 INSTALL_GPU.md 获取详细步骤
- 查看 `results.csv` 中的基准测试日志

---

## Git 仓库设置

有关如何在 GitHub 上设置仓库的说明，请参见 [GITHUB_SETUP.md](/GITHUB_SETUP.md)

## 许可证

请参见原始仓库的许可证信息。

## 致谢

GPU 支持实现基于：
- Apache Arrow 17.0.0
- cuDF 24.12
- Taskflow v3.11.0
- NVIDIA CUDA 12.8
