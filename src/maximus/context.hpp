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

    bool tables_initially_pinned = false;

    bool tables_initially_as_single_chunk = false;

#ifdef MAXIMUS_WITH_CUDA

    bool use_separate_copy_streams = false;

    std::vector<rmm::cuda_stream_view> stream_vector;
    cudf::detail::cuda_stream_pool *stream_pool;
    rmm::cuda_stream_view h2d_stream;
    rmm::cuda_stream_view d2h_stream;

    rmm::mr::cuda_memory_resource cuda_mr;

    // RMM GPU memory pool configuration:
    // - Initial size: 50% of free device memory
    // - Maximum size: 90% of free device memory (leaves headroom for CUDA runtime)
    // - The pool grows on demand up to the maximum; "maximum pool size exceeded"
    //   errors mean the dataset + query intermediates exceed available GPU memory.
    rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource> pool_mr{
        &cuda_mr,
        rmm::percent_of_free_device_memory(50),   // initial
        rmm::percent_of_free_device_memory(90)};  // maximum

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
