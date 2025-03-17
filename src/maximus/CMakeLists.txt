set(maximus_src_files
        # types
        types/types.cpp
        types/expression.cpp
        types/aggregate.cpp
        types/node_type.cpp
        types/physical_operator_type.cpp
        types/schema.cpp
        types/table.cpp
        types/table_batch.cpp
        types/device_table_ptr.cpp

        # utils
        utils/utils.cpp
        utils/arrow_helpers.cpp
        utils/json_helpers.cpp

        # config
        config.cpp

        # io
        io/file.cpp
        io/csv.cpp
        io/parquet.cpp

        # operators
        operators/abstract_operator.hpp
        operators/abstract_table_source_operator.cpp
        operators/abstract_filter_operator.cpp
        operators/abstract_project_operator.cpp
        operators/abstract_hash_join_operator.cpp
        operators/abstract_order_by_operator.cpp
        operators/abstract_group_by_operator.cpp
        operators/abstract_limit_operator.cpp
        operators/abstract_distinct_operator.cpp
        operators/abstract_fused_operator.cpp
        operators/abstract_local_broadcast_operator.cpp
        operators/engine.cpp

        # operators/native
        operators/native/limit_operator.cpp
        operators/native/hash_join_operator.cpp
        operators/native/table_sink_operator.cpp
        operators/native/random_table_source_operator.cpp
        operators/native/table_source_operator.cpp
        operators/native/local_broadcast_operator.cpp

        # operators/acero
        operators/acero/dummy_node.cpp
        operators/acero/filter_operator.cpp
        operators/acero/project_operator.cpp
        operators/acero/hash_join_operator.cpp
        operators/acero/order_by_operator.cpp
        operators/acero/group_by_operator.cpp
        operators/acero/limit_operator.cpp
        operators/acero/distinct_operator.cpp
        operators/acero/fused_operator.cpp
        operators/acero/properties.cpp
        operators/acero/interop.cpp

        # dag
        dag/query_node.cpp
        dag/query_plan.cpp

        # frontend
        frontend/query_plan_api.cpp
        frontend/expressions.cpp

        # execution
        exec/pipeline.cpp
        exec/executor.cpp

        # src
        context.cpp
        database.cpp
        database_catalogue.cpp

        # profiler
        profiler/profiler.cpp

        # sql
        sql/parser.cpp

        # tpch
        tpch/tpch_queries.cpp

        # clickbench
        clickbench/clickbench_queries.cpp

        # h2o
        h2o/h2o_queries.cpp
)

set(maximus_src_files_cuda
    # gpu table
    gpu/gtable/cuda/cuda_buffer.cu
    gpu/gtable/cuda/cuda_buffer.cpp
    gpu/gtable/cuda/cuda_context.cpp
    gpu/gtable/cuda/cuda_scalar.cpp
    gpu/gtable/gcolumn.cpp
    gpu/gtable/gtable.cpp

    # cudf
    gpu/cudf/table_reader.cpp
    gpu/cudf/table_writer.cpp
    gpu/cudf/cudf_types.cpp
    gpu/cudf/cudf_expr.cpp
    gpu/cudf/csv.cpp
    gpu/cudf/parquet.cpp

    # cudf operators
    operators/gpu/cudf/group_by_operator.cpp
    operators/gpu/cudf/order_by_operator.cpp
    operators/gpu/cudf/hash_join_operator.cpp
    operators/gpu/cudf/project_operator.cpp
    operators/gpu/cudf/filter_operator.cpp
    operators/gpu/cudf/order_by_operator.cpp
    operators/gpu/cudf/distinct_operator.cpp
    operators/gpu/cudf/limit_operator.cpp
    # operators/gpu/cudf/fused_operator.cpp
    operators/gpu/cudf/local_broadcast_operator.cpp
    operators/gpu/cudf/table_source_operator.cpp

    types/cuda_event.cpp
)

if (MAXIMUS_WITH_CUDA STREQUAL "ON")
    list(APPEND maximus_src_files ${maximus_src_files_cuda})
endif()

add_library(maximus ${maximus_src_files})

if (MAXIMUS_WITH_CUDA STREQUAL "ON")
    target_compile_definitions(maximus PUBLIC MAXIMUS_WITH_CUDA)
endif()

if (MAXIMUS_WITH_PROFILING STREQUAL "ON")
    target_compile_definitions(maximus PUBLIC MAXIMUS_WITH_PROFILING)
endif()

# message("MAXIMUS_EXTERNAL_INCLUDE_DIRS: ${MAXIMUS_EXTERNAL_INCLUDE_DIRS}")
# message("MAXIMUS_EXTERNAL_LIBS: ${MAXIMUS_EXTERNAL_LIBS}")

target_include_directories(maximus PUBLIC $<BUILD_INTERFACE:${maximus_SOURCE_DIR}/src> ${MAXIMUS_EXTERNAL_INCLUDE_DIRS})

target_compile_features(maximus PUBLIC cxx_std_17)

target_link_libraries(maximus PUBLIC ${MAXIMUS_EXTERNAL_LIBS})
