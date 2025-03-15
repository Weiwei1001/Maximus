#include <parquet/arrow/reader.h>

#include <maximus/io/file.hpp>
#include <maximus/io/parquet.hpp>

namespace maximus {
Status streaming_reader_parquet(const std::shared_ptr<MaximusContext> &ctx,
                                const std::string &path,
                                const std::shared_ptr<Schema> &schema,
                                const std::vector<std::string> &include_columns,
                                std::shared_ptr<arrow::RecordBatchReader> &reader) {
    assert(ends_with(path, ".parquet") && "streaming_reader_parquet only supports parquet files");

    assert(ctx);
    auto pool = ctx->get_memory_pool();
    assert(pool);

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    auto status = reader_builder.OpenFile(path, /*memory_map=*/false, reader_properties);
    if (!status.ok()) {
        return Status(ErrorCode::ArrowError, status.message());
    }
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    auto maybe_arrow_reader = reader_builder.Build();
    if (!maybe_arrow_reader.ok()) {
        return Status(ErrorCode::ArrowError, maybe_arrow_reader.status().message());
    }
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader =
        std::move(maybe_arrow_reader.ValueOrDie());

    status = arrow_reader->GetRecordBatchReader(&reader);
    if (!status.ok()) {
        return Status(ErrorCode::ArrowError, status.message());
    }
    return Status::OK();
}

Status read_parquet(const std::shared_ptr<MaximusContext> &ctx,
                    const std::string &path,
                    const std::shared_ptr<Schema> &schema,
                    const std::vector<std::string> &include_columns,
                    TablePtr &tableOut) {
    assert(ends_with(path, ".parquet") && "read_parquet(...) only supports parquet files");

    assert(ctx && "The context must not be null in Table::from_parquet.");
    auto pool = ctx->get_memory_pool();
    assert(pool && "The memory pool must not be null in Table::from_parquet.");

    std::shared_ptr<arrow::io::RandomAccessFile> input;
    auto status = input_file(ctx, path, input);
    if (!arrow_status(status).ok()) {
        return status;
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto pq_status = parquet::arrow::OpenFile(input, pool, &reader);
    if (!pq_status.ok()) {
        return Status(ErrorCode::ArrowError, pq_status.message());
    }

    std::shared_ptr<arrow::Table> full_table;
    pq_status = reader->ReadTable(&full_table);
    if (!pq_status.ok()) {
        return Status(ErrorCode::ArrowError, pq_status.message());
    }

    tableOut = std::make_shared<Table>(ctx, full_table);
    return Status::OK();
}

Status read_parquet(const std::shared_ptr<MaximusContext> &ctx,
                    const std::string &path,
                    TablePtr &tableOut) {
    return read_parquet(ctx, path, nullptr, {}, tableOut);
}
}  // namespace maximus