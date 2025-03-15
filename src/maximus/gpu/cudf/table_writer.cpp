#include <maximus/gpu/cudf/table_writer.hpp>

namespace maximus {
namespace gpu {

std::shared_ptr<GColumn> cudf_to_gcolumn(std::unique_ptr<cudf::column> &col,
                                         std::shared_ptr<MaximusGContext> &ctx) {
    // Convert cudf type to maximus type
    std::shared_ptr<DataType> type = to_maximus_type(col->type());
    assert(type);
    int64_t null_count = col->null_count(), sz = col->size();

    // Transfer cudf column to GColumn
    cudf::column::contents data     = col->release();
    rmm::device_buffer data_dev_buf = std::move(*(data.data));
    rmm::device_buffer null_dev_buf = std::move(*(data.null_mask));
    int64_t null_sz = null_dev_buf.size(), data_sz = data_dev_buf.size();
    std::shared_ptr<CudaBuffer> data_buf = std::make_shared<CudaBuffer>(
        std::make_shared<rmm::device_buffer>(std::move(data_dev_buf)), data_sz);
    std::shared_ptr<CudaBuffer> null_buf = nullptr;

    // Transfer cudf null buffer to GColumn
    if (null_count > 0) {
        null_buf = std::make_shared<CudaBuffer>(
            std::make_shared<rmm::device_buffer>(std::move(null_dev_buf)), null_sz);
    }

    // Transfer children
    std::vector<std::shared_ptr<GColumn>> children;
    std::vector<std::unique_ptr<cudf::column>> cudf_children = std::move(data.children);
    for (auto &child : cudf_children) {
        children.push_back(cudf_to_gcolumn(child, ctx));
    }

    // Return GColumn
    return std::make_shared<GColumn>(sz, null_count, type, null_buf, data_buf, ctx, children);
}

std::shared_ptr<GTable> cudf_to_gtable(std::shared_ptr<Schema> schema,
                                       std::shared_ptr<cudf::table> tab,
                                       std::shared_ptr<MaximusGContext> &ctx) {
    // Transfer cudf table to GTable
    std::vector<std::unique_ptr<cudf::column>> cols = tab->release();
    std::vector<std::shared_ptr<GColumn>> gcols(cols.size());
    assert(schema->size() == cols.size());
    for (int i = 0; i < cols.size(); i++) {
        assert(maximus::to_maximus_type(schema->get_schema()->field(i)->type())->id() ==
               to_maximus_type(cols[i]->type())->id());
        gcols[i] = cudf_to_gcolumn(cols[i], ctx);
    }
    return std::make_shared<GTable>(schema, gcols, ctx);
}

}  // namespace gpu
}  // namespace maximus
