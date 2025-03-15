#include <maximus/gpu/cudf/table_reader.hpp>

namespace maximus {

namespace gpu {

std::string type_id_to_string(cudf::type_id t) {
    switch (t) {
        case cudf::type_id::EMPTY:
            return "EMPTY";
        case cudf::type_id::INT8:
            return "INT8";
        case cudf::type_id::INT16:
            return "INT16";
        case cudf::type_id::INT32:
            return "INT32";
        case cudf::type_id::INT64:
            return "INT64";
        case cudf::type_id::UINT8:
            return "UINT8";
        case cudf::type_id::UINT16:
            return "UINT16";
        case cudf::type_id::UINT32:
            return "UINT32";
        case cudf::type_id::UINT64:
            return "UINT64";
        case cudf::type_id::FLOAT32:
            return "FLOAT32";
        case cudf::type_id::FLOAT64:
            return "FLOAT64";
        case cudf::type_id::BOOL8:
            return "BOOL8";
        case cudf::type_id::TIMESTAMP_DAYS:
            return "TIMESTAMP_DAYS";
        case cudf::type_id::TIMESTAMP_SECONDS:
            return "TIMESTAMP_SECONDS";
        case cudf::type_id::TIMESTAMP_MILLISECONDS:
            return "TIMESTAMP_MILLISECONDS";
        case cudf::type_id::TIMESTAMP_MICROSECONDS:
            return "TIMESTAMP_MICROSECONDS";
        case cudf::type_id::TIMESTAMP_NANOSECONDS:
            return "TIMESTAMP_NANOSECONDS";
        case cudf::type_id::DURATION_DAYS:
            return "DURATION_DAYS";
        case cudf::type_id::DURATION_SECONDS:
            return "DURATION_SECONDS";
        case cudf::type_id::DURATION_MILLISECONDS:
            return "DURATION_MILLISECONDS";
        case cudf::type_id::DURATION_MICROSECONDS:
            return "DURATION_MICROSECONDS";
        case cudf::type_id::DURATION_NANOSECONDS:
            return "DURATION_NANOSECONDS";
        case cudf::type_id::DICTIONARY32:
            return "DICTIONARY32";
        case cudf::type_id::STRING:
            return "STRING";
        case cudf::type_id::LIST:
            return "LIST";
        case cudf::type_id::DECIMAL32:
            return "DECIMAL32";
        case cudf::type_id::DECIMAL64:
            return "DECIMAL64";
        case cudf::type_id::DECIMAL128:
            return "DECIMAL128";
        case cudf::type_id::STRUCT:
            return "STRUCT";
        case cudf::type_id::NUM_TYPE_IDS:
            return "NUM_TYPE_IDS";
        default:
            return "UNKNOWN_TYPE_ID";
    }
}

std::unique_ptr<cudf::column> gcolumn_to_cudf(std::shared_ptr<GColumn> col) {
    // Convert maximus type to cudf type
    cudf::data_type type = to_cudf_type(col->get_data_type());
    assert(type != cudf::data_type(cudf::type_id::EMPTY));

    // Convert GColumn to cudf column
    int32_t size                   = col->get_length();
    std::shared_ptr<GBuffer> gdata = col->get_data_buffer();
    std::shared_ptr<rmm::device_buffer> data_device_buffer =
        std::move(std::static_pointer_cast<CudaBuffer>(gdata)->get_buffer());
    int32_t null_count                                     = col->get_null_count();
    std::shared_ptr<rmm::device_buffer> null_device_buffer = nullptr;

    // Convert GColumn null buffer to cudf null buffer
    if (null_count > 0) {
        std::shared_ptr<GBuffer> gnull = col->get_null_buffer();
        assert(gnull);
        null_device_buffer = std::move(std::static_pointer_cast<CudaBuffer>(gnull)->get_buffer());
    }

    // convert children
    std::vector<std::shared_ptr<GColumn>> children           = col->release_children();
    std::vector<std::unique_ptr<cudf::column>> cudf_children = {};
    for (auto &child : children) {
        std::unique_ptr<cudf::column> child_column = gcolumn_to_cudf(std::move(child));
        cudf_children.push_back(std::move(child_column));
    }

    // std::cout << type_id_to_string(type.id()) << std::endl;;
    assert(size == 0 || data_device_buffer);

    // return cudf::column
    if (null_count == 0) {
        return std::make_unique<cudf::column>(
            type,
            size,
            std::move(*data_device_buffer),
            std::move(rmm::device_buffer{0, rmm::cuda_stream_default}),
            null_count,
            std::move(cudf_children));
    }

    return std::make_unique<cudf::column>(type,
                                          size,
                                          std::move(*data_device_buffer),
                                          std::move(*null_device_buffer),
                                          null_count,
                                          std::move(cudf_children));
}

std::shared_ptr<cudf::table> gtable_to_cudf(std::shared_ptr<GTable> tab) {
    // Convert GTable to cudf table
    std::vector<std::unique_ptr<cudf::column>> columns;
    std::vector<std::shared_ptr<GColumn>> gcols = tab->get_table();
    for (int i = 0; i < gcols.size(); i++) {
        columns.emplace_back(std::move(gcolumn_to_cudf(gcols[i])));
    }
    return std::make_shared<cudf::table>(std::move(columns));
}

cudf::column_view gcolumn_to_cudf_view(std::shared_ptr<GColumn> col) {
    // Convert maximus type to cudf type
    cudf::data_type type = to_cudf_type(col->get_data_type());
    assert(type != cudf::data_type(cudf::type_id::EMPTY));

    // Convert GColumn to cudf column view
    int32_t size                      = col->get_length();
    std::shared_ptr<GBuffer> gdata    = col->get_data_buffer();
    std::shared_ptr<CudaBuffer> cdata = std::static_pointer_cast<CudaBuffer>(gdata);
    assert(cdata);
    uint8_t *data      = cdata->data<uint8_t>();
    int32_t null_count = col->get_null_count();
    uint8_t *null      = nullptr;

    // Convert GColumn null buffer to cudf null buffer
    if (null_count > 0) {
        std::shared_ptr<GBuffer> gnull = col->get_null_buffer();
        assert(gnull);
        std::shared_ptr<CudaBuffer> cnull = std::static_pointer_cast<CudaBuffer>(gnull);
        assert(cnull);
        null = cnull->data<uint8_t>();
    }

    // convert children
    std::vector<std::shared_ptr<GColumn>> children = col->get_children();
    std::vector<cudf::column_view> cudf_children   = {};
    for (auto &child : children) {
        cudf_children.push_back(gcolumn_to_cudf_view(child));
    }

    // return cudf::column_view
    return cudf::column_view(
        type, size, (const void *) data, (uint32_t const *) null, null_count, 0, cudf_children);
}

std::shared_ptr<cudf::table_view> gtable_to_cudf_view(std::shared_ptr<GTable> tab) {
    // Convert GTable to cudf table view
    std::vector<cudf::column_view> columns;
    std::vector<std::shared_ptr<GColumn>> gcols = tab->get_table();
    for (int i = 0; i < gcols.size(); i++) {
        columns.push_back(gcolumn_to_cudf_view(gcols[i]));
    }
    return std::make_shared<cudf::table_view>(columns);
}

}  // namespace gpu
}  // namespace maximus
