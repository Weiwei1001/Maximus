#pragma once
#include <arrow/acero/exec_plan.h>
#include <arrow/compute/exec.h>

#include <maximus/config.hpp>
#include <maximus/memory_pool.hpp>
#include <maximus/proxy_memory_pool.hpp>
#include <thread>

#ifdef MAXIMUS_WITH_CUDA
#include <cudf/detail/utilities/stream_pool.hpp>
#include <rmm/cuda_stream_view.hpp>
// RMM 24.12+: rmm/mr/device/pool_memory_resource.hpp
// RMM older:   rmm/mr/pool_memory_resource.hpp
#if __has_include(<rmm/mr/device/pool_memory_resource.hpp>)
#include <rmm/mr/device/pool_memory_resource.hpp>
#else
#include <rmm/mr/pool_memory_resource.hpp>
#endif
#if __has_include(<rmm/mr/device/managed_memory_resource.hpp>)
#include <rmm/mr/device/managed_memory_resource.hpp>
#else
#include <rmm/mr/managed_memory_resource.hpp>
#endif
#endif

// Forward declaration
namespace maximus::gpu {
class MaximusGContext;
}

// Forward declaration
namespace maximus {

class MaximusContext {
public:
    MaximusContext();
    ~MaximusContext();

    void set_memory_pool(std::unique_ptr<MemoryPool> &&pool);

    arrow::MemoryPool *get_memory_pool();
    arrow::MemoryPool *get_pinned_memory_pool();
    arrow::MemoryPool *get_pinned_memory_pool_if_available();

    arrow::compute::ExecContext *get_exec_context();

    arrow::acero::QueryOptions get_query_options();

    arrow::io::IOContext *get_io_context();

    std::shared_ptr<maximus::gpu::MaximusGContext> &get_gpu_context();

    std::shared_ptr<arrow::acero::ExecPlan> get_mini_exec_plan();

    std::unique_ptr<ProxyMemoryPool> proxy_pool;
    std::unique_ptr<arrow::MemoryPool> default_pool;
    std::unique_ptr<arrow::MemoryPool> pinned_pool;

    void barrier() const;

    std::unique_ptr<arrow::compute::ExecContext> exec_context;

    std::unique_ptr<arrow::io::IOContext> io_context;

    bool fusing_enabled = get_operators_fusion();

    int n_outer_threads = -1;
    int n_inner_threads = -1;

    int32_t csv_batch_size = -1;

    std::size_t max_pinned_pool_size = -1;

    TransferCompression transfer_compression = TransferCompression::NONE;

    bool tables_initially_pinned = false;

    bool tables_initially_as_single_chunk = false;

#ifdef MAXIMUS_WITH_CUDA

    bool use_separate_copy_streams = false;

    std::vector<rmm::cuda_stream_view> stream_vector;
    cudf::detail::cuda_stream_pool *stream_pool;
    rmm::cuda_stream_view h2d_stream;
    rmm::cuda_stream_view d2h_stream;

    // Use managed memory (cudaMallocManaged) which:
    // - Allows GPU memory to overflow into CPU memory via CUDA Unified Memory
    // - On GH200 with NVLink-C2C + ATS, this uses hardware address translation
    //   with minimal performance penalty for hot data
    // - No fixed capacity limit — can use full system memory (GPU + CPU)
    // - Eliminates OOM for queries with large intermediate results
    // Wrap in pool for allocation performance.
    rmm::mr::managed_memory_resource managed_mr;
    rmm::mr::pool_memory_resource<rmm::mr::managed_memory_resource> pool_mr{
        &managed_mr,
        rmm::percent_of_free_device_memory(50),   // initial pool from GPU memory
        std::size_t{400} * 1024 * 1024 * 1024};   // max 400GB (GPU+CPU combined)

    void wait_h2d_copy() const;
    void wait_d2h_copy() const;

    rmm::cuda_stream_view get_h2d_stream();
    rmm::cuda_stream_view get_d2h_stream();

    std::vector<std::shared_ptr<arrow::Table>> tables_pending_copy;
#endif

    std::shared_ptr<maximus::gpu::MaximusGContext> gcontext = nullptr;
};

using Context = std::shared_ptr<MaximusContext>;

Context make_context();
}  // namespace maximus
