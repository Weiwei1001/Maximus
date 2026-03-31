# Experiment 2: GPU Data Transfer Compression

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add nvcomp-based compression/decompression to Maximus's CPU→GPU data transfer path, measuring latency and energy impact across compression algorithms.

**Architecture:** When `storage_device=cpu`, Arrow column buffers are compressed on CPU (LZ4/Snappy/ZSTD), transferred as compressed data over PCIe, then decompressed on GPU using nvcomp. This reduces PCIe transfer volume at the cost of CPU compress + GPU decompress time. The modification is isolated to the transfer layer in `cuda_context.cpp` and a new `compression_utils` module.

**Tech Stack:** nvcomp 5.1.0 (GPU decompression), Arrow codecs (CPU compression), CUDA streams, RMM

**Hypothesis:** PCIe 4.0 x16 bandwidth ~20 GB/s. nvcomp LZ4 GPU decompression ~100+ GB/s. For typical columnar data with 2-5x compression ratio, the compressed transfer should be faster because:
- Transfer time: reduced by compression_ratio (2-5x less data)
- GPU decompress: ~100 GB/s >> PCIe bandwidth
- CPU compress: LZ4 ~3-5 GB/s (main bottleneck, but still net positive for large transfers)

---

## File Structure

| File | Action | Purpose |
|------|--------|---------|
| `src/maximus/gpu/compression/transfer_compressor.hpp` | Create | Compression interface for H2D transfers |
| `src/maximus/gpu/compression/transfer_compressor.cpp` | Create | nvcomp LZ4/Snappy/ZSTD CPU compress + GPU decompress |
| `src/maximus/gpu/gtable/cuda/cuda_context.cpp` | Modify | Add compressed H2D transfer path |
| `src/maximus/gpu/gtable/cuda/cuda_context.hpp` | Modify | Add compressed transfer method |
| `src/maximus/context.hpp` | Modify | Add compression config to MaximusContext |
| `src/maximus/config.hpp` | Modify | Add MAXIMUS_TRANSFER_COMPRESSION env var |
| `src/maximus/config.cpp` | Modify | Parse compression config |
| `src/maximus/CMakeLists.txt` | Modify | Link nvcomp |
| `benchmarks/maxbench.cpp` | Modify | Add --transfer_compression CLI flag |
| `scripts/run_compression_experiment.py` | Create | Experiment runner: sweep codecs × queries × scale factors |

---

### Task 1: Link nvcomp to Maximus build

**Files:**
- Modify: `/home/xzw/Maximus/src/maximus/CMakeLists.txt`

- [ ] **Step 1: Add nvcomp include and library paths**

In `/home/xzw/Maximus/src/maximus/CMakeLists.txt`, add after the existing cudf/rmm link section:

```cmake
# nvcomp for GPU compression/decompression
set(NVCOMP_ROOT "${Python3_SITEARCH}/nvidia/libnvcomp")
target_include_directories(maximus_lib PUBLIC "${NVCOMP_ROOT}/include")
target_link_directories(maximus_lib PUBLIC "${NVCOMP_ROOT}/lib64")
target_link_libraries(maximus_lib PUBLIC nvcomp)
```

- [ ] **Step 2: Verify build compiles**

```bash
cd /home/xzw/Maximus && ninja -C build -j$(nproc)
```

Expected: Build succeeds with nvcomp linked.

- [ ] **Step 3: Commit**

```bash
git add src/maximus/CMakeLists.txt
git commit -m "build: link nvcomp for GPU transfer compression"
```

---

### Task 2: Add compression configuration

**Files:**
- Modify: `/home/xzw/Maximus/src/maximus/config.hpp`
- Modify: `/home/xzw/Maximus/src/maximus/config.cpp`
- Modify: `/home/xzw/Maximus/src/maximus/context.hpp`

- [ ] **Step 1: Add compression enum and config to config.hpp**

Add after existing config declarations in `/home/xzw/Maximus/src/maximus/config.hpp`:

```cpp
enum class TransferCompression {
    NONE,   // No compression (default, current behavior)
    LZ4,    // Fast compression, moderate ratio
    SNAPPY, // Balanced
    ZSTD,   // High ratio, slower
};

// Parse from env var MAXIMUS_TRANSFER_COMPRESSION (none/lz4/snappy/zstd)
TransferCompression parse_transfer_compression();
```

- [ ] **Step 2: Implement parsing in config.cpp**

Add to `/home/xzw/Maximus/src/maximus/config.cpp`:

```cpp
TransferCompression parse_transfer_compression() {
    const char* env = std::getenv("MAXIMUS_TRANSFER_COMPRESSION");
    if (!env) return TransferCompression::NONE;
    std::string val(env);
    if (val == "lz4") return TransferCompression::LZ4;
    if (val == "snappy") return TransferCompression::SNAPPY;
    if (val == "zstd") return TransferCompression::ZSTD;
    return TransferCompression::NONE;
}
```

- [ ] **Step 3: Add to MaximusContext**

In `/home/xzw/Maximus/src/maximus/context.hpp`, add to the MaximusContext class:

```cpp
TransferCompression transfer_compression = TransferCompression::NONE;
```

Initialize it in the constructor in `context.cpp`:

```cpp
transfer_compression = parse_transfer_compression();
```

- [ ] **Step 4: Build and verify**

```bash
ninja -C build -j$(nproc)
```

- [ ] **Step 5: Commit**

```bash
git add src/maximus/config.hpp src/maximus/config.cpp src/maximus/context.hpp src/maximus/context.cpp
git commit -m "feat: add transfer compression configuration (MAXIMUS_TRANSFER_COMPRESSION env var)"
```

---

### Task 3: Implement TransferCompressor

**Files:**
- Create: `/home/xzw/Maximus/src/maximus/gpu/compression/transfer_compressor.hpp`
- Create: `/home/xzw/Maximus/src/maximus/gpu/compression/transfer_compressor.cpp`

- [ ] **Step 1: Create the header**

Create `/home/xzw/Maximus/src/maximus/gpu/compression/transfer_compressor.hpp`:

```cpp
#pragma once

#include <memory>
#include <cstdint>
#include <rmm/device_buffer.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <arrow/buffer.h>
#include "maximus/config.hpp"

namespace maximus {
namespace gpu {

// Compresses host buffer on CPU, transfers compressed data to GPU,
// decompresses on GPU using nvcomp. Returns device buffer with original data.
//
// Flow: host_buf → CPU compress → cudaMemcpy(compressed) → GPU decompress → device_buf
class TransferCompressor {
public:
    // Compress host buffer and transfer to GPU with decompression.
    // Returns device buffer containing the original (decompressed) data.
    static rmm::device_buffer compress_and_transfer(
        const uint8_t* host_data,
        int64_t nbytes,
        TransferCompression codec,
        rmm::cuda_stream_view stream);

    // CPU-side compression using Arrow codecs.
    // Returns compressed buffer and its size.
    static std::shared_ptr<arrow::Buffer> compress_on_cpu(
        const uint8_t* data,
        int64_t nbytes,
        TransferCompression codec);

    // GPU-side decompression using nvcomp.
    // Input: device buffer with compressed data. Output: device buffer with decompressed data.
    static rmm::device_buffer decompress_on_gpu(
        const rmm::device_buffer& compressed_device_buf,
        int64_t original_size,
        TransferCompression codec,
        rmm::cuda_stream_view stream);
};

}  // namespace gpu
}  // namespace maximus
```

- [ ] **Step 2: Create the implementation**

Create `/home/xzw/Maximus/src/maximus/gpu/compression/transfer_compressor.cpp`:

```cpp
#include "transfer_compressor.hpp"

#include <arrow/util/compression.h>
#include <nvcomp/lz4.h>
#include <nvcomp/snappy.h>
#include <nvcomp/zstd.h>
#include <rmm/device_buffer.hpp>
#include <cuda_runtime.h>
#include <stdexcept>
#include <vector>

namespace maximus {
namespace gpu {

namespace {

arrow::Compression::type to_arrow_codec(TransferCompression codec) {
    switch (codec) {
        case TransferCompression::LZ4: return arrow::Compression::LZ4_FRAME;
        case TransferCompression::SNAPPY: return arrow::Compression::SNAPPY;
        case TransferCompression::ZSTD: return arrow::Compression::ZSTD;
        default: throw std::runtime_error("Unsupported compression codec");
    }
}

}  // namespace

std::shared_ptr<arrow::Buffer> TransferCompressor::compress_on_cpu(
        const uint8_t* data, int64_t nbytes, TransferCompression codec) {
    auto arrow_codec = arrow::util::Codec::Create(to_arrow_codec(codec)).ValueOrDie();
    int64_t max_compressed = arrow_codec->MaxCompressedLen(nbytes, data);
    auto buf = arrow::AllocateBuffer(max_compressed).ValueOrDie();
    int64_t actual_size = arrow_codec->Compress(
        nbytes, data, max_compressed, buf->mutable_data()).ValueOrDie();
    return arrow::SliceBuffer(std::move(buf), 0, actual_size);
}

rmm::device_buffer TransferCompressor::decompress_on_gpu(
        const rmm::device_buffer& compressed_device_buf,
        int64_t original_size,
        TransferCompression codec,
        rmm::cuda_stream_view stream) {

    const size_t comp_size = compressed_device_buf.size();
    const void* comp_data = compressed_device_buf.data();

    // Allocate output buffer for decompressed data
    rmm::device_buffer output(original_size, stream);

    // Use nvcomp batched API (single chunk = batch of 1)
    const size_t num_chunks = 1;
    const void* const* device_compressed_ptrs;
    size_t* device_compressed_sizes;
    void* const* device_uncompressed_ptrs;
    size_t* device_uncompressed_sizes;
    nvcompStatus_t* device_statuses;

    // Allocate device arrays for batch API
    rmm::device_buffer d_comp_ptrs(sizeof(void*), stream);
    rmm::device_buffer d_comp_sizes(sizeof(size_t), stream);
    rmm::device_buffer d_uncomp_ptrs(sizeof(void*), stream);
    rmm::device_buffer d_uncomp_sizes(sizeof(size_t), stream);
    rmm::device_buffer d_statuses(sizeof(nvcompStatus_t), stream);

    // Copy pointers and sizes to device
    const void* h_comp_ptr = comp_data;
    size_t h_comp_size = comp_size;
    void* h_uncomp_ptr = output.data();
    size_t h_uncomp_size = static_cast<size_t>(original_size);

    cudaMemcpyAsync(d_comp_ptrs.data(), &h_comp_ptr, sizeof(void*),
                    cudaMemcpyHostToDevice, stream.value());
    cudaMemcpyAsync(d_comp_sizes.data(), &h_comp_size, sizeof(size_t),
                    cudaMemcpyHostToDevice, stream.value());
    cudaMemcpyAsync(d_uncomp_ptrs.data(), &h_uncomp_ptr, sizeof(void*),
                    cudaMemcpyHostToDevice, stream.value());
    cudaMemcpyAsync(d_uncomp_sizes.data(), &h_uncomp_size, sizeof(size_t),
                    cudaMemcpyHostToDevice, stream.value());

    // Get temp size and allocate
    size_t temp_size = 0;

    switch (codec) {
        case TransferCompression::LZ4: {
            nvcompBatchedLZ4DecompressGetTempSize(
                num_chunks, h_uncomp_size, &temp_size);
            rmm::device_buffer temp(temp_size, stream);
            nvcompBatchedLZ4DecompressAsync(
                static_cast<const void* const*>(d_comp_ptrs.data()),
                static_cast<const size_t*>(d_comp_sizes.data()),
                static_cast<const size_t*>(d_uncomp_sizes.data()),
                nullptr,  // actual_uncompressed_sizes (optional)
                num_chunks,
                temp.data(), temp_size,
                static_cast<void* const*>(d_uncomp_ptrs.data()),
                static_cast<nvcompStatus_t*>(d_statuses.data()),
                stream.value());
            break;
        }
        case TransferCompression::SNAPPY: {
            nvcompBatchedSnappyDecompressGetTempSize(
                num_chunks, h_uncomp_size, &temp_size);
            rmm::device_buffer temp(temp_size, stream);
            nvcompBatchedSnappyDecompressAsync(
                static_cast<const void* const*>(d_comp_ptrs.data()),
                static_cast<const size_t*>(d_comp_sizes.data()),
                static_cast<const size_t*>(d_uncomp_sizes.data()),
                nullptr,
                num_chunks,
                temp.data(), temp_size,
                static_cast<void* const*>(d_uncomp_ptrs.data()),
                static_cast<nvcompStatus_t*>(d_statuses.data()),
                stream.value());
            break;
        }
        case TransferCompression::ZSTD: {
            nvcompBatchedZstdDecompressGetTempSize(
                num_chunks, h_uncomp_size, &temp_size);
            rmm::device_buffer temp(temp_size, stream);
            nvcompBatchedZstdDecompressAsync(
                static_cast<const void* const*>(d_comp_ptrs.data()),
                static_cast<const size_t*>(d_comp_sizes.data()),
                static_cast<const size_t*>(d_uncomp_sizes.data()),
                nullptr,
                num_chunks,
                temp.data(), temp_size,
                static_cast<void* const*>(d_uncomp_ptrs.data()),
                static_cast<nvcompStatus_t*>(d_statuses.data()),
                stream.value());
            break;
        }
        default:
            throw std::runtime_error("Unsupported decompression codec");
    }

    stream.synchronize();
    return output;
}

rmm::device_buffer TransferCompressor::compress_and_transfer(
        const uint8_t* host_data, int64_t nbytes,
        TransferCompression codec, rmm::cuda_stream_view stream) {

    if (codec == TransferCompression::NONE || nbytes == 0) {
        // No compression: direct transfer
        return rmm::device_buffer(host_data, nbytes, stream);
    }

    // Step 1: Compress on CPU
    auto compressed = compress_on_cpu(host_data, nbytes, codec);

    // Step 2: Transfer compressed data to GPU
    rmm::device_buffer compressed_device(
        compressed->data(), compressed->size(), stream);

    // Step 3: Decompress on GPU
    return decompress_on_gpu(compressed_device, nbytes, codec, stream);
}

}  // namespace gpu
}  // namespace maximus
```

- [ ] **Step 3: Add to CMakeLists.txt**

Add the new source file to the maximus_lib sources in `/home/xzw/Maximus/src/maximus/CMakeLists.txt`:

```cmake
gpu/compression/transfer_compressor.cpp
```

- [ ] **Step 4: Build and verify**

```bash
ninja -C build -j$(nproc)
```

- [ ] **Step 5: Commit**

```bash
git add src/maximus/gpu/compression/
git commit -m "feat: add TransferCompressor with nvcomp GPU decompression"
```

---

### Task 4: Integrate compressed transfer into data loading

**Files:**
- Modify: `/home/xzw/Maximus/src/maximus/gpu/gtable/cuda/cuda_context.hpp`
- Modify: `/home/xzw/Maximus/src/maximus/gpu/gtable/cuda/cuda_context.cpp`
- Modify: `/home/xzw/Maximus/src/maximus/utils/utils.cpp`

- [ ] **Step 1: Add compressed H2D method to cuda_context.hpp**

Add method declaration to `MaximusCudaContext` class:

```cpp
arrow::Status memcpy_host_to_device_compressed(
    const std::shared_ptr<arrow::Buffer>& host_buf,
    int64_t nbytes, int64_t offset,
    TransferCompression codec,
    std::shared_ptr<GBuffer>& device_buf);
```

- [ ] **Step 2: Implement in cuda_context.cpp**

```cpp
arrow::Status MaximusCudaContext::memcpy_host_to_device_compressed(
    const std::shared_ptr<arrow::Buffer>& host_buf,
    int64_t nbytes, int64_t offset,
    TransferCompression codec,
    std::shared_ptr<GBuffer>& device_buf) {

    if (codec == TransferCompression::NONE) {
        return memcpy_host_to_device(host_buf, nbytes, offset, device_buf);
    }

    auto result = gpu::TransferCompressor::compress_and_transfer(
        host_buf->data() + offset, nbytes, codec, rmm::cuda_stream_default);

    device_buf = std::make_shared<GBuffer>(std::move(result));
    return arrow::Status::OK();
}
```

- [ ] **Step 3: Wire into read_table() for cpu storage mode**

In `/home/xzw/Maximus/src/maximus/utils/utils.cpp`, modify the CPU→GPU transfer path. The key change is in the `read_table()` function where `storage_device == DeviceType::CPU` but execution device is GPU. This is where Arrow tables on CPU get transferred to GPU — the transfer happens later in the table_source_operator when it copies data.

Note: The actual integration point depends on how Maximus currently handles the cpu→gpu transfer during query execution. The transfer happens inside the GPU operators when they receive CPU-side data. We need to identify the exact function that copies Arrow buffers to GPU and add the compression path there.

- [ ] **Step 4: Build and verify**

```bash
ninja -C build -j$(nproc)
```

- [ ] **Step 5: Quick smoke test**

```bash
MAXIMUS_TRANSFER_COMPRESSION=lz4 ./build/benchmarks/maxbench \
    --benchmark tpch -q q1 -d gpu -r 3 -s cpu \
    --path tests/tpch/csv-1 --engines maximus
```

Expected: Query completes successfully with same result as without compression.

- [ ] **Step 6: Commit**

```bash
git add src/maximus/gpu/gtable/cuda/cuda_context.hpp src/maximus/gpu/gtable/cuda/cuda_context.cpp src/maximus/utils/utils.cpp
git commit -m "feat: integrate compressed H2D transfer into data loading path"
```

---

### Task 5: Add CLI flag to maxbench

**Files:**
- Modify: `/home/xzw/Maximus/benchmarks/maxbench.cpp`

- [ ] **Step 1: Add --transfer_compression option**

Add CLI option parsing in maxbench.cpp alongside existing options:

```cpp
std::string compression_str = "none";
// In argument parsing section:
app.add_option("--transfer_compression", compression_str,
    "Compression for CPU->GPU transfer: none, lz4, snappy, zstd")
    ->default_val("none");

// After parsing, set on context:
if (compression_str == "lz4") ctx->transfer_compression = TransferCompression::LZ4;
else if (compression_str == "snappy") ctx->transfer_compression = TransferCompression::SNAPPY;
else if (compression_str == "zstd") ctx->transfer_compression = TransferCompression::ZSTD;
else ctx->transfer_compression = TransferCompression::NONE;
```

- [ ] **Step 2: Build and verify**

```bash
ninja -C build -j$(nproc)
```

- [ ] **Step 3: Test with CLI flag**

```bash
./build/benchmarks/maxbench --benchmark tpch -q q1,q6 -d gpu -r 5 \
    -s cpu --transfer_compression lz4 --path tests/tpch/csv-1 --engines maximus
```

- [ ] **Step 4: Commit**

```bash
git add benchmarks/maxbench.cpp
git commit -m "feat: add --transfer_compression CLI flag to maxbench"
```

---

### Task 6: Create experiment runner script

**Files:**
- Create: `/home/xzw/gpu_db/scripts/run_compression_experiment.py`

- [ ] **Step 1: Write the experiment script**

```python
#!/usr/bin/env python3
"""
Experiment 2: Measure latency and energy impact of compressed CPU→GPU transfers.

Matrix:
  - Codecs: none, lz4, snappy, zstd
  - Scale factors: 1, 2, 10
  - Queries: q1 (scan+agg), q6 (filter), q3 (join), q9 (complex), q12 (join+agg)
  - Storage: cpu, cpu-pinned (compression only helps when data is on CPU)
  - Reps: 20

Metrics: min_ms, avg_ms per query. Energy via nvidia-smi sampling.
"""
from __future__ import annotations
import csv, os, re, subprocess, sys, time
from pathlib import Path

MAXIMUS_DIR = Path("/home/xzw/Maximus")
MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
RESULTS_DIR = Path("/home/xzw/gpu_db/results/compression_experiment")
LD_EXTRA = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
]

CODECS = ["none", "lz4", "snappy", "zstd"]
STORAGE_MODES = ["cpu", "cpu-pinned"]
SCALE_FACTORS = [1, 2, 10]
QUERIES = ["q1", "q6", "q3", "q9", "q12"]
N_REPS = 20


def get_env(codec="none"):
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    env["MAXIMUS_TRANSFER_COMPRESSION"] = codec
    return env


def parse_timings(output):
    result = {}
    current = None
    for line in output.split("\n"):
        qm = re.match(r"\s*QUERY (\w+)", line.strip())
        if qm: current = qm.group(1)
        tm = re.match(r"- MAXIMUS TIMINGS \[ms\]:\s*(.*)", line.strip())
        if tm and current:
            ts = tm.group(1).strip().rstrip(",")
            result[current] = [int(t) for t in ts.split(",") if t.strip()]
    for line in output.split("\n"):
        if line.startswith("gpu,maximus,"):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                q = parts[2]
                if q not in result:
                    result[q] = [int(t) for t in parts[3:] if t.strip()]
    return result


def run(sf, storage, codec, queries):
    data_path = MAXIMUS_DIR / "tests" / "tpch" / f"csv-{sf}"
    if not data_path.exists():
        return {q: (-1, -1, "no_data") for q in queries}
    cmd = [
        str(MAXBENCH), "--benchmark", "tpch",
        "-q", ",".join(queries), "-d", "gpu",
        "-r", str(N_REPS), "--n_reps_storage", "1",
        "--path", str(data_path), "-s", storage,
        "--transfer_compression", codec,
        "--engines", "maximus",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=600, env=get_env(codec))
        timings = parse_timings(proc.stdout + (proc.stderr or ""))
    except Exception:
        return {q: (-1, -1, "error") for q in queries}

    out = {}
    for q in queries:
        if q in timings and timings[q]:
            t = timings[q]
            out[q] = (min(t), round(sum(t)/len(t), 1), "ok")
        else:
            out[q] = (-1, -1, "missing")
    return out


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    total = len(CODECS) * len(STORAGE_MODES) * len(SCALE_FACTORS)
    i = 0
    for sf in SCALE_FACTORS:
        for storage in STORAGE_MODES:
            for codec in CODECS:
                i += 1
                print(f"[{i}/{total}] sf={sf} storage={storage} codec={codec}")
                results = run(sf, storage, codec, QUERIES)
                for q, (mn, avg, st) in results.items():
                    rows.append({
                        "sf": sf, "storage": storage, "codec": codec,
                        "query": q, "min_ms": mn, "avg_ms": avg, "status": st,
                    })
                    print(f"  {q}: min={mn}ms avg={avg}ms {st}")

    out = RESULTS_DIR / "compression_summary.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["sf","storage","codec","query","min_ms","avg_ms","status"])
        w.writeheader()
        w.writerows(rows)
    print(f"\nResults: {out}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the experiment**

```bash
python /home/xzw/gpu_db/scripts/run_compression_experiment.py
```

- [ ] **Step 3: Commit results**

```bash
git add scripts/run_compression_experiment.py results/compression_experiment/
git commit -m "data: compression experiment results"
```

---

### Task 7: Add energy measurement

**Files:**
- Modify: `/home/xzw/gpu_db/scripts/run_compression_experiment.py`

- [ ] **Step 1: Add nvidia-smi power sampling**

Extend the experiment script to spawn `nvidia-smi` in background during each benchmark run, sampling at 50ms. Calculate energy = avg_power × latency for each configuration.

Follow the pattern from `/home/xzw/gpu_db/benchmarks/scripts/run_maximus_metrics.py` for power measurement methodology.

- [ ] **Step 2: Re-run with energy measurement**

- [ ] **Step 3: Commit**

---

## Expected Results Matrix

| Codec | Compression Ratio | CPU Compress Speed | GPU Decompress Speed | Net H2D Improvement |
|-------|-------------------|-------------------|---------------------|---------------------|
| none | 1x | N/A | N/A | baseline |
| LZ4 | 2-3x | ~3 GB/s | ~100 GB/s | likely positive |
| Snappy | 1.5-2.5x | ~4 GB/s | ~80 GB/s | marginal |
| ZSTD | 3-5x | ~0.5 GB/s | ~30 GB/s | negative (CPU bottleneck) |

**Prediction:** LZ4 will show the best latency improvement for cpu-storage mode because it has the best balance of compression ratio and CPU compression speed. ZSTD will be worse than no compression due to slow CPU-side compression. The benefit increases with data size (SF10 > SF1).
