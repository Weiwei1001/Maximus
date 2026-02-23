# New Machine Setup Guide

Complete guide to set up Maximus + Sirius benchmarking from a bare metal machine.

## Prerequisites

- Ubuntu 22.04 or 24.04
- NVIDIA GPU with CUDA support (recommended: >= 24GB VRAM, 32GB optimal)
- Root access
- RAM: >= 128GB (TPC-H SF=20 loading requires ~145GB)
- Disk: >= 300GB (test data + compilation)

## Step 1: System Dependencies

```bash
apt-get update && apt-get install -y \
    build-essential cmake git ninja-build \
    libssl-dev libnuma-dev libconfig++-dev \
    python3 python3-pip wget curl
```

## Step 2: CUDA Toolkit

Install CUDA >= 12.x if not already present:

```bash
# Check if CUDA is installed
nvcc --version

# If not installed, download from:
# https://developer.nvidia.com/cuda-downloads
# Follow the instructions for your Ubuntu version
```

Verify:
```bash
nvcc --version   # should show >= 12.x
nvidia-smi       # should show your GPU
```

## Step 3: Python Dependencies

```bash
pip install cmake duckdb
# cmake >= 3.30.4 is required for Sirius
cmake --version  # verify >= 3.30.4
```

## Step 4: Clone Maximus

```bash
cd /workspace
git clone https://github.com/Weiwei1001/Maximus.git
cd Maximus
```

## Step 5: Install Maximus Dependencies

### 5a: Apache Arrow

```bash
bash scripts/build_arrow.sh
```

If you encounter issues, see [Installation_issues.md](./Installation_issues.md).

### 5b: Taskflow

```bash
bash scripts/build_taskflow.sh
```

### 5c: cuDF (GPU backend)

The easiest way is via pip:

```bash
pip install cudf-cu12 libcudf-cu12
```

For building from source, see [scripts/build_cudf.md](./scripts/build_cudf.md).

## Step 6: Build Maximus

```bash
cd /workspace/Maximus

# Configure with GPU support (auto-detects pip-installed cuDF)
bash scripts/configure_with_gpu_pip_cudf.sh

# Build
cd build && make -j$(nproc)
cd ..

# Verify
ls -lh build/benchmarks/maxbench
# Should exist and be ~400-500KB
```

## Step 7: Prepare Test Data

### 7a: TPC-H (generated via DuckDB)

```bash
python3 -c "
import duckdb, os
for sf in [1, 2, 10, 20]:
    out = f'tests/tpch/csv-{sf}'
    if os.path.exists(out):
        print(f'SF={sf} already exists, skipping')
        continue
    os.makedirs(out, exist_ok=True)
    print(f'Generating TPC-H SF={sf}...')
    conn = duckdb.connect()
    conn.execute('INSTALL tpch; LOAD tpch;')
    conn.execute(f'CALL dbgen(sf={sf})')
    for t in ['lineitem','orders','customer','part','partsupp','supplier','nation','region']:
        conn.execute(f\"COPY {t} TO '{out}/{t}.csv' (HEADER, DELIMITER ',')\")
    conn.close()
    print(f'Done: SF={sf}')
"
```

Approximate sizes: SF=1 ~1GB, SF=2 ~2GB, SF=10 ~12GB, SF=20 ~23GB.

### 7b: H2O Group-By Benchmark

Generate using the H2O benchmark data generator:

```bash
# Install R data generator or use Python
pip install datatable

python3 -c "
import os
# Option 1: Download pre-generated data
# https://github.com/h2oai/db-benchmark

# Option 2: Generate with DuckDB
import duckdb
for size in ['1gb', '2gb', '3gb', '4gb']:
    out = f'tests/h2o/csv-{size}'
    if os.path.exists(out):
        print(f'{size} already exists, skipping')
        continue
    os.makedirs(out, exist_ok=True)

    # Map size to row count
    nrows = {'1gb': 10_000_000, '2gb': 20_000_000, '3gb': 30_000_000, '4gb': 40_000_000}[size]
    k = 100  # number of groups

    conn = duckdb.connect()
    conn.execute(f'''
        CREATE TABLE groupby AS
        SELECT
            'id' || (i % {k})::VARCHAR AS id1,
            'id' || (i % {k})::VARCHAR AS id2,
            'id' || (i % {k*10})::VARCHAR AS id3,
            (i % {k})::INTEGER AS id4,
            (i % {k})::INTEGER AS id5,
            (i % {k*10})::INTEGER AS id6,
            (random() * 100)::INTEGER AS v1,
            (random() * 100)::INTEGER AS v2,
            random() * 100 AS v3
        FROM generate_series(1, {nrows}) t(i)
    ''')
    conn.execute(f\"COPY groupby TO '{out}/groupby.csv' (HEADER, DELIMITER ',')\")
    conn.close()
    print(f'Done: {size} ({nrows} rows)')
"
```

### 7c: ClickBench

```bash
# Download the ClickBench hits dataset
mkdir -p tests/clickbench

# SF=1: standard ClickBench (~100M rows, ~14GB CSV)
wget -O tests/clickbench/csv-1/t.csv \
    https://datasets.clickhouse.com/hits_compatible/hits.csv.gz | gunzip

# SF=2: duplicate to create 2x
mkdir -p tests/clickbench/csv-2
cat tests/clickbench/csv-1/t.csv > tests/clickbench/csv-2/t.csv
tail -n +2 tests/clickbench/csv-1/t.csv >> tests/clickbench/csv-2/t.csv
```

**Note:** ClickBench data is large. Ensure sufficient disk space.

## Step 8: Setup Sirius (DuckDB GPU Extension)

```bash
cd /workspace/Maximus
bash benchmarks/scripts/setup_sirius.sh --install-dir /workspace
```

This script automatically:
1. Checks prerequisites (CMake >= 3.30.4, CUDA, ninja)
2. Clones Sirius from https://github.com/sirius-db/sirius.git
3. Installs dependencies (libcudf, abseil, libconfig++, libnuma)
4. Builds Sirius
5. Generates DuckDB benchmark databases from CSV data
6. Generates GPU query SQL files (`call gpu_processing(...)`)

Verify:
```bash
ls -lh /workspace/sirius/build/release/duckdb    # Sirius binary
ls /workspace/tpch_duckdb/*.duckdb                # TPC-H databases
ls /workspace/h2o_duckdb/*.duckdb                 # H2O databases
ls /workspace/click_duckdb/*.duckdb               # ClickBench databases
ls /workspace/tpch_sql/queries/1/*.sql            # GPU query SQL files
```

## Step 9: Set Runtime Environment

```bash
# Required for pip-installed cuDF libraries
export LD_LIBRARY_PATH=/usr/local/lib/python3.12/dist-packages/nvidia/libnvcomp/lib64:/usr/local/lib/python3.12/dist-packages/libkvikio/lib64:$LD_LIBRARY_PATH

# Optional: add to .bashrc for persistence
echo 'export LD_LIBRARY_PATH=/usr/local/lib/python3.12/dist-packages/nvidia/libnvcomp/lib64:/usr/local/lib/python3.12/dist-packages/libkvikio/lib64:$LD_LIBRARY_PATH' >> ~/.bashrc
```

## Step 10: Run Benchmarks

**Important:** Run Maximus and Sirius sequentially, never simultaneously. GPU memory contention causes false failures.

### 10a: Maximus Benchmark

```bash
cd /workspace/Maximus

# Run all benchmarks (TPC-H, H2O, ClickBench)
python3 benchmarks/scripts/run_maximus_benchmark.py

# Or run specific benchmarks
python3 benchmarks/scripts/run_maximus_benchmark.py tpch
python3 benchmarks/scripts/run_maximus_benchmark.py h2o clickbench
```

Methodology: data preloaded to GPU, 3 reps per query, min time reported.

### 10b: Sirius Benchmark

```bash
cd /workspace/Maximus

# Run all benchmarks
python3 benchmarks/scripts/run_sirius_benchmark.py

# Or specific benchmarks with custom settings
python3 benchmarks/scripts/run_sirius_benchmark.py tpch --n-passes 3 --batch-size 10
```

Methodology: 3 passes (separate DuckDB process each), 10 queries per batch (avoids OOM), 3rd pass timing recorded.

### 10c: Compare Results

```bash
python3 benchmarks/scripts/compare_results.py \
    --sirius benchmark_results/sirius_benchmark.csv \
    --maximus benchmark_results/maximus_benchmark.csv
```

## Quick Verification (Smoke Test)

After setup, run a quick smoke test to verify everything works:

```bash
# Maximus: single TPC-H query on SF=1
cd /workspace/Maximus
LD_LIBRARY_PATH=/usr/local/lib/python3.12/dist-packages/nvidia/libnvcomp/lib64:/usr/local/lib/python3.12/dist-packages/libkvikio/lib64:$LD_LIBRARY_PATH \
build/benchmarks/maxbench --benchmark tpch -q q1 -d gpu -r 1 --n_reps_storage 1 \
    --path tests/tpch/csv-1 -s cpu --engines maximus

# Sirius: single TPC-H query on SF=1
echo '.timer on
call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT count(*) FROM lineitem;");
' | /workspace/sirius/build/release/duckdb /workspace/tpch_duckdb/tpch_sf1.duckdb
```

## Important Notes

| Item | Details |
|------|---------|
| GPU VRAM | 32GB recommended. TPC-H SF=20 uses ~26GB on GPU |
| RAM | SF=10 needs ~75GB RAM for CSV loading, SF=20 needs ~145GB |
| Sirius GPU memory leak | Sirius leaks GPU memory over consecutive queries. Use batch size of 10 and separate processes per pass |
| Sirius FALLBACK | If a query takes > 60s or GPU memory is exhausted, Sirius falls back to CPU. Check output for "fallback" |
| cuDF GPU limitations | `minute()`, `STRLEN()`, `REGEXP_REPLACE()` not supported on GPU (ClickBench q18, q27, q28, q42) |
| Maximus unsupported | H2O q8 not implemented; ClickBench q18, q27, q28, q42 unsupported on GPU |

## Estimated Maximum Scale Factors (32GB VRAM)

| Benchmark | Max SF (all queries pass) | Bottleneck |
|-----------|--------------------------|------------|
| TPC-H | Maximus: ~SF 10-15, Sirius: ~SF 30-40 | Complex JOINs with large intermediate results |
| H2O | ~10-15gb | Full GROUP BY (q10) memory explosion |
| ClickBench | ~SF 3-5 | High-cardinality GROUP BY queries |

## Troubleshooting

**Build fails with cuDF not found:**
```bash
# Check cuDF installation
python3 -c "import cudf; print(cudf.__version__)"
# Re-run configure script
bash scripts/configure_with_gpu_pip_cudf.sh
```

**GPU out of memory:**
```bash
# Check GPU memory
nvidia-smi
# Kill any leftover processes
pkill -9 maxbench; pkill -9 duckdb
# Wait for memory to free
sleep 5 && nvidia-smi
```

**Sirius build fails (CMake version):**
```bash
# Upgrade CMake via pip
pip install --upgrade cmake
cmake --version  # needs >= 3.30.4
```

**maxbench segfaults on large SF:**
- Ensure sufficient RAM (SF=20 needs ~145GB)
- Try smaller SF first to verify GPU works
- Check `dmesg` for OOM killer messages
