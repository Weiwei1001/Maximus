#include <maximus/gpu/cudf/parquet.hpp>
#include <maximus/profiler/profiler.hpp>
#include <maximus/utils/utils.hpp>
#include <typeinfo>

namespace maximus::gpu {

Status read_parquet_cudf(std::shared_ptr<MaximusGContext>& ctx,
                         const std::string& path,
                         const std::shared_ptr<Schema>& schema,
                         const std::vector<std::string>& include_columns,
                         std::shared_ptr<GTable>& table) {
    PE("IO");
    PE("read_parquet_cudf");
    // auto start_time = std::chrono::high_resolution_clock::now();
    assert(ends_with(path, ".parquet") && "read_parquet(...) only supports parquet files");
    assert(!ctx && "ctx cannot be nullptr");
    assert(typeid(*ctx) == typeid(MaximusCudaContext) && "ctx must be a MaximusCudaContext object");

    cudf::io::source_info source = cudf::io::source_info(path);

    cudf::io::parquet_reader_options_builder options =
        cudf::io::parquet_reader_options::builder(source);

    if (!include_columns.empty()) {
        options.columns(include_columns);
    }

    if (schema) {
        std::vector<cudf::io::reader_column_schema> column_types;
        std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> schema_types =
            schema->column_types();
        std::transform(
            schema_types.begin(),
            schema_types.end(),
            std::back_inserter(column_types),
            [](const std::pair<std::string, std::shared_ptr<arrow::DataType>>& schema_type) {
                return schema_type.second->id() == arrow::Type::STRING
                           ? cudf::io::reader_column_schema(0).add_child(
                                 cudf::io::reader_column_schema(0).set_type_length(4))
                           : cudf::io::reader_column_schema(0).set_type_length(
                                 schema_type.second->byte_width());
            });
        options.set_column_schema(column_types);
    }

    cudf::io::table_with_metadata result = cudf::io::read_parquet(options);

    table = cudf_to_gtable(schema, std::move(result.tbl), ctx);

    // auto end_time = std::chrono::high_resolution_clock::now();

    //auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // std::cout << "GPU: read_parquet = " << duration << std::endl;
    PL("read_parquet_cudf");
    PL("IO");
    return Status::OK();
}

Status write_parquet_cudf(const std::string& path,
                          std::shared_ptr<GTable>& table,
                          std::unique_ptr<std::vector<uint8_t>>& result) {
    PE("IO");
    PE("write_parquet_cudf");
    // auto start_time = std::chrono::high_resolution_clock::now();
    assert(ends_with(path, ".parquet") && "write_parquet(...) only supports parquet files");

    cudf::io::sink_info sink = cudf::io::sink_info(path);

    std::shared_ptr<cudf::table_view> cudf_table = gtable_to_cudf_view(table);

    cudf::io::parquet_writer_options_builder options =
        cudf::io::parquet_writer_options::builder(sink, *cudf_table);

    result = cudf::io::write_parquet(options);

    PL("write_parquet_cudf");
    PL("IO");
    return Status::OK();
}

}  // namespace maximus::gpu
