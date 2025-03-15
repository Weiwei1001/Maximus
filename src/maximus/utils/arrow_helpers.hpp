#pragma once

#include <arrow/api.h>

namespace maximus {
std::shared_ptr<arrow::RecordBatch> arrow_clone(const std::shared_ptr<arrow::RecordBatch>& rb,
                                                arrow::MemoryPool* pool);

std::shared_ptr<arrow::Table> arrow_clone(const std::shared_ptr<arrow::Table>& table,
                                          arrow::MemoryPool* pool);

std::shared_ptr<arrow::Table> arrow_clone_to_single_chunk(
    const std::shared_ptr<arrow::Table>& table, arrow::MemoryPool* pool);

std::shared_ptr<arrow::RecordBatch> to_record_batch(std::shared_ptr<arrow::Table> table,
                                                    arrow::MemoryPool* pool);

std::shared_ptr<arrow::RecordBatch> to_record_batch(
    std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, arrow::MemoryPool* pool);

// converts an arrow::Array value at given index to string
std::string array_string(const std::shared_ptr<arrow::Array>& array, int64_t index);

std::shared_ptr<arrow::RecordBatch> generate_batch(
    const arrow::FieldVector& fields,
    int64_t size,
    int seed,
    arrow::MemoryPool* memory_pool = arrow::default_memory_pool());


}  // namespace maximus
