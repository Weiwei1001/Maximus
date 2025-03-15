#include <maximus/profiler/profiler.hpp>
#include <maximus/utils/utils.hpp>
#include <string>

#ifdef MAXIMUS_WITH_CUDA
#include <maximus/gpu/cudf/csv.hpp>
#include <maximus/gpu/cudf/parquet.hpp>
#endif

namespace maximus {

bool ends_with(const std::string& full_string, const std::string& ending) {
    if (ending.size() > full_string.size()) return false;

    return full_string.compare(full_string.size() - ending.size(), ending.size(), ending) == 0;
}

bool contains(const std::string& full_string, const std::string& substring) {
    return full_string.find(substring) != std::string::npos;
}

DeviceTablePtr read_table(std::shared_ptr<MaximusContext>& ctx,
                          std::string path,
                          const std::shared_ptr<Schema>& schema,
                          const std::vector<std::string>& include_columns,
                          const DeviceType& storage_device) {
    TablePtr cpu_table;
    GTablePtr gpu_table;
    if (ends_with(path, ".csv")) {
        if (storage_device == DeviceType::CPU) {
            auto status = Table::from_csv(ctx, path, schema, include_columns, cpu_table);
            check_status(status);
        }
        if (storage_device == DeviceType::GPU) {
#ifdef MAXIMUS_WITH_CUDA
            auto status = gpu::read_csv_cudf(
                ctx->get_gpu_context(), path, schema, include_columns, gpu_table);
            check_status(status);
#else
            throw std::runtime_error("Maximus must be built with the GPU support.");
#endif
        }
    } else if (ends_with(path, ".parquet")) {
        if (storage_device == DeviceType::CPU) {
            auto status = Table::from_parquet(ctx, path, schema, include_columns, cpu_table);
            check_status(status);
        }
        if (storage_device == DeviceType::GPU) {
#ifdef MAXIMUS_WITH_CUDA
            auto status = gpu::read_parquet_cudf(
                ctx->get_gpu_context(), path, schema, include_columns, gpu_table);
            check_status(status);
#else
            throw std::runtime_error("Maximus must be built with the GPU support.");
#endif
        }
    } else {
        throw std::runtime_error("Unsupported file format: " + path);
    }

    if (storage_device == DeviceType::GPU) {
        assert(gpu_table);
        return DeviceTablePtr(std::move(gpu_table));
    }

    if (storage_device == DeviceType::CPU) {
        assert(cpu_table);
        return DeviceTablePtr(std::move(cpu_table));
    }

    throw std::runtime_error("Unsupported storage device: " +
                             device_type_to_string(storage_device));
}
}  // namespace maximus
