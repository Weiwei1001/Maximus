#pragma once

#include <cudf/table/table.hpp>
#include <maximus/gpu/cudf/cudf_types.hpp>
#include <maximus/gpu/gtable/cuda/cuda_buffer.hpp>
#include <maximus/gpu/gtable/gtable.hpp>

namespace maximus {
namespace gpu {

/**
 * To convert a cudf table to a GTable
 */

std::shared_ptr<GTable> cudf_to_gtable(std::shared_ptr<Schema> schema,
                                       std::shared_ptr<cudf::table> tab,
                                       std::shared_ptr<MaximusGContext> &ctx);

}  // namespace gpu
}  // namespace maximus