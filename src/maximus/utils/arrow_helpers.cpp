#include <iostream>
#include <maximus/error_handling.hpp>
#include <maximus/utils/arrow_helpers.hpp>
#include <random>

namespace maximus {

std::shared_ptr<arrow::RecordBatch> arrow_clone(
    const std::shared_ptr<arrow::RecordBatch> &input_batch, arrow::MemoryPool *memory_pool) {
    std::vector<std::shared_ptr<arrow::Array>> cloned_arrays;
    cloned_arrays.reserve(input_batch->num_columns());

    for (int i = 0; i < input_batch->num_columns(); ++i) {
        auto column       = input_batch->column(i);
        auto &column_data = column->data();

        std::vector<std::shared_ptr<arrow::Buffer>> copied_buffers;
        copied_buffers.reserve(column_data->buffers.size());

        for (auto &buffer : column_data->buffers) {
            if (buffer) {
                auto buffer_result = buffer->CopySlice(0, buffer->size(), memory_pool);
                if (!buffer_result.ok()) {
                    check_status(buffer_result.status());
                }
                copied_buffers.push_back(buffer_result.ValueOrDie());
            } else {
                copied_buffers.emplace_back(nullptr);
            }
        }

        auto new_array_data =
            arrow::ArrayData::Make(column->type(), column->length(), std::move(copied_buffers));
        cloned_arrays.emplace_back(arrow::MakeArray(new_array_data));
    }

    return arrow::RecordBatch::Make(
        input_batch->schema(), input_batch->num_rows(), std::move(cloned_arrays));
}

std::shared_ptr<arrow::Table> arrow_clone(const std::shared_ptr<arrow::Table> &source_table,
                                          arrow::MemoryPool *memory_pool) {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> cloned_columns;
    cloned_columns.reserve(source_table->num_columns());

    for (int col_idx = 0; col_idx < source_table->num_columns(); ++col_idx) {
        auto chunked_array = source_table->column(col_idx);
        std::vector<std::shared_ptr<arrow::Array>> cloned_chunks;

        for (const auto &chunk : chunked_array->chunks()) {
            auto &chunk_data = chunk->data();
            std::vector<std::shared_ptr<arrow::Buffer>> copied_buffers;
            copied_buffers.reserve(chunk_data->buffers.size());

            for (const auto &buffer : chunk_data->buffers) {
                if (buffer) {
                    auto buffer_copy = buffer->CopySlice(0, buffer->size(), memory_pool);
                    if (!buffer_copy.ok()) {
                        check_status(buffer_copy.status());
                    }
                    copied_buffers.push_back(buffer_copy.ValueOrDie());
                } else {
                    copied_buffers.emplace_back(nullptr);
                }
            }

            auto new_array_data =
                arrow::ArrayData::Make(chunk->type(), chunk->length(), std::move(copied_buffers));
            cloned_chunks.push_back(arrow::MakeArray(new_array_data));
        }

        auto new_chunked_array =
            std::make_shared<arrow::ChunkedArray>(std::move(cloned_chunks), chunked_array->type());
        cloned_columns.emplace_back(std::move(new_chunked_array));
    }

    return arrow::Table::Make(source_table->schema(), std::move(cloned_columns));
}

int max_num_of_chunks(const std::shared_ptr<arrow::Table> &table) {
    int max_chunks = 0;

    // Iterate over each column in the table
    for (int i = 0; i < table->num_columns(); ++i) {
        // Get the chunked array for the column
        std::shared_ptr<arrow::ChunkedArray> chunked_array = table->column(i);

        // Get the number of chunks in this chunked array
        int num_chunks = chunked_array->num_chunks();

        max_chunks = std::max(max_chunks, num_chunks);
    }

    return max_chunks;
}

std::shared_ptr<arrow::Table> concatenate_chunks(std::shared_ptr<arrow::Table> table,
                                                 arrow::MemoryPool *pool) {
    assert(pool);

    int num_chunks = max_num_of_chunks(table);

    // if there is only one chunk, and the pinned memory usage is enforced, copy the table to pinned memory
    if (num_chunks == 1) {
        auto new_table = arrow_clone(table, pool);
        return std::move(new_table);
    }

    // otherwise, combine chunks will anyway perform a full copy, using the provided memory pool
    auto maybe_batch = table->CombineChunksToBatch(pool);
    if (!maybe_batch.ok()) {
        check_status(maybe_batch.status());
    }
    auto batch       = std::move(maybe_batch).ValueOrDie();
    auto maybe_table = arrow::Table::FromRecordBatches({std::move(batch)});
    if (!maybe_table.ok()) {
        check_status(maybe_table.status());
    }
    return std::move(maybe_table).ValueOrDie();
}

std::shared_ptr<arrow::Table> arrow_clone_to_single_chunk(
    const std::shared_ptr<arrow::Table> &table, arrow::MemoryPool *pool) {
    return concatenate_chunks(table, pool);
}

std::shared_ptr<arrow::RecordBatch> to_record_batch(std::shared_ptr<arrow::Table> table,
                                                    arrow::MemoryPool *pool) {
    auto table_reader  = arrow::TableBatchReader(table);
    auto maybe_batches = table_reader.ToRecordBatches();

    if (!maybe_batches.ok()) {
        check_status(maybe_batches.status());
    }

    auto batches = maybe_batches.ValueOrDie();

    if (batches.size() == 0) {
        return nullptr;
    }

    if (batches.size() == 1) {
        return std::move(batches[0]);
    }

    assert(pool);
    auto maybe_batch = table->CombineChunksToBatch(pool);
    if (!maybe_batch.ok()) {
        check_status(maybe_batch.status());
    }
    auto batch = std::move(maybe_batch.ValueOrDie());
    return std::move(batch);
}

std::shared_ptr<arrow::RecordBatch> to_record_batch(
    std::vector<std::shared_ptr<arrow::RecordBatch>> &batches, arrow::MemoryPool *pool) {
    if (batches.size() == 0) {
        return nullptr;
    }
    if (batches.size() == 1) {
        return std::move(batches[0]);
    }

    auto maybe_table = arrow::Table::FromRecordBatches(batches);
    if (!maybe_table.ok()) {
        check_status(maybe_table.status());
    }

    auto table = std::move(maybe_table.ValueOrDie());

    assert(pool);

    auto maybe_batch = table->CombineChunksToBatch(pool);
    if (!maybe_batch.ok()) {
        check_status(maybe_batch.status());
    }
    auto batch = std::move(maybe_batch.ValueOrDie());
    return std::move(batch);
}


// Helper function template for numeric types
template<typename TYPE>
std::string to_string_numeric(const std::shared_ptr<arrow::Array> &array, int index) {
    auto casted_array = std::static_pointer_cast<arrow::NumericArray<TYPE>>(array);
    return casted_array->IsNull(index) ? "" : std::to_string(casted_array->Value(index));
}

// Type mapping using a function pointer approach
using ToStringFunc = std::function<std::string(const std::shared_ptr<arrow::Array> &, int)>;

std::unordered_map<arrow::Type::type, ToStringFunc> create_to_string_map() {
    return {{arrow::Type::UINT8, to_string_numeric<arrow::UInt8Type>},
            {arrow::Type::INT8, to_string_numeric<arrow::Int8Type>},
            {arrow::Type::UINT16, to_string_numeric<arrow::UInt16Type>},
            {arrow::Type::INT16, to_string_numeric<arrow::Int16Type>},
            {arrow::Type::UINT32, to_string_numeric<arrow::UInt32Type>},
            {arrow::Type::INT32, to_string_numeric<arrow::Int32Type>},
            {arrow::Type::UINT64, to_string_numeric<arrow::UInt64Type>},
            {arrow::Type::INT64, to_string_numeric<arrow::Int64Type>},
            {arrow::Type::HALF_FLOAT, to_string_numeric<arrow::HalfFloatType>},
            {arrow::Type::FLOAT, to_string_numeric<arrow::FloatType>},
            {arrow::Type::DOUBLE, to_string_numeric<arrow::DoubleType>},
            {arrow::Type::DATE32, to_string_numeric<arrow::Date32Type>},
            {arrow::Type::DATE64, to_string_numeric<arrow::Date64Type>},
            {arrow::Type::TIMESTAMP, to_string_numeric<arrow::TimestampType>},
            {arrow::Type::TIME32, to_string_numeric<arrow::Time32Type>},
            {arrow::Type::TIME64, to_string_numeric<arrow::Time64Type>},
            {arrow::Type::STRING, [](const std::shared_ptr<arrow::Array> &array, int index) {
                 return std::static_pointer_cast<arrow::StringArray>(array)->GetString(index);
             }}};
}

std::string array_string(const std::shared_ptr<arrow::Array> &array, int64_t index) {
    static const auto to_string_map = create_to_string_map();

    if (!array) return "NA";

    auto it = to_string_map.find(array->type()->id());
    if (it != to_string_map.end()) {
        return it->second(array, index);
    }

    return "NA";  // Default case for unsupported types
}

std::shared_ptr<arrow::RecordBatch> generate_batch(const arrow::FieldVector &fields,
                                                   int64_t size,
                                                   int seed,
                                                   arrow::MemoryPool *memory_pool) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> float_dist(-1.0f, 1.0f);
    std::uniform_real_distribution<double> double_dist(-1.0, 1.0);
    std::uniform_int_distribution<int> char_dist('a', 'z');
    std::uniform_int_distribution<int> length_dist(5, 20);

    std::uniform_int_distribution<int> binary_length_dist(
        10, 50);  // Random binary length between 10 and 50// Random string length between 5 and 20

    for (const auto &field : fields) {
        auto type = field->type()->id();
        std::shared_ptr<arrow::Array> array;
        switch (type) {
            case arrow::Type::BOOL: {
                arrow::BooleanBuilder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    bool value = rng() % 2;
                    check_status(builder.Append(value));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::UINT8: {
                arrow::UInt8Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(rng() % 256));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::INT8: {
                arrow::Int8Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(rng() % 256 - 128));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::UINT16: {
                arrow::UInt16Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(rng() % 65536));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::INT16: {
                arrow::Int16Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(rng() % 65536 - 32768));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::UINT32: {
                arrow::UInt32Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(rng()));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::INT32: {
                arrow::Int32Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(static_cast<int32_t>(rng())));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::UINT64: {
                arrow::UInt64Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(static_cast<uint64_t>(rng())));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::INT64: {
                arrow::Int64Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(static_cast<int64_t>(rng())));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::HALF_FLOAT: {
                arrow::HalfFloatBuilder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(float_dist(rng)));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::FLOAT: {
                arrow::FloatBuilder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(float_dist(rng)));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::DOUBLE: {
                arrow::DoubleBuilder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(double_dist(rng)));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::DATE32: {
                arrow::Date32Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(rng() % (365 * 50)));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::DATE64: {
                arrow::Date64Builder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(rng() % (365LL * 50 * 24 * 60 * 60 * 1000)));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::TIMESTAMP: {
                arrow::TimestampBuilder builder(field->type(), memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    check_status(builder.Append(rng()));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::STRING: {
                arrow::StringBuilder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    int length = length_dist(rng);  // Random string length between 5 and 20
                    std::string value;
                    value.reserve(length);
                    for (int j = 0; j < length; ++j) {
                        value.push_back(static_cast<char>(char_dist(rng)));  // Random char
                    }
                    check_status(builder.Append(value));  // Append as a string
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::FIXED_SIZE_BINARY: {
                int64_t byte_size =
                    field->type()->byte_width();  // Get the fixed size of the binary field
                arrow::FixedSizeBinaryBuilder builder(field->type(), memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    std::string value(byte_size, ' ');
                    for (int j = 0; j < byte_size; ++j) {
                        value[j] = static_cast<char>(rng() % 256);  // Random byte between 0 and 255
                    }
                    check_status(builder.Append(value));
                }
                check_status(builder.Finish(&array));
                break;
            }
            case arrow::Type::BINARY: {
                arrow::BinaryBuilder builder(memory_pool);
                for (int64_t i = 0; i < size; ++i) {
                    int length =
                        binary_length_dist(rng);  // Random binary length between 10 and 50 bytes
                    std::string value;
                    value.resize(length);  // Resize string to the random length
                    for (int j = 0; j < length; ++j) {
                        value[j] = static_cast<char>(rng() % 256);  // Random byte between 0 and 255
                    }
                    check_status(builder.Append(value));
                }
                check_status(builder.Finish(&array));
                break;
            }
            default:
                throw std::runtime_error("Type not yet implemented");
        }
        arrays.push_back(array);
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), size, arrays);
}

}  // namespace maximus
