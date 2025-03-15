#pragma once

#include <cudf/io/csv.hpp>
#include <maximus/gpu/cuda_api.hpp>

namespace maximus {

namespace gpu {

// if the schema is given, it will be used to set the column names and types
Status read_csv_cudf(std::shared_ptr<MaximusGContext>& ctx,
                     const std::string& path,
                     const std::shared_ptr<Schema>& schema,
                     const std::vector<std::string>& include_columns,
                     std::shared_ptr<GTable>& table);

Status write_csv_cudf(const std::string& path, std::shared_ptr<GTable>& table);

}  // namespace gpu

}  // namespace maximus