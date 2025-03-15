#include <iostream>
#include <maximus/gpu/gtable/gtable.hpp>

namespace maximus {

namespace gpu {

GTable::GTable(std::shared_ptr<Schema> schema,
               std::vector<std::shared_ptr<GColumn>> &cols,
               const std::shared_ptr<MaximusGContext> &ctx)
        : schema_(std::move(schema)), ctx_(ctx), cols_(std::move(cols)) {
}

const std::shared_ptr<MaximusGContext> &GTable::get_context() const {
    return ctx_;
}

std::shared_ptr<Schema> &GTable::get_schema() {
    return schema_;
}

std::vector<std::shared_ptr<GColumn>> &GTable::get_table() {
    return cols_;
}

int GTable::get_num_columns() {
    return cols_.size();
}

int64_t GTable::get_num_rows() {
    return cols_[0]->get_length();
}

std::shared_ptr<GTable> GTable::clone() {
    std::vector<std::shared_ptr<GColumn>> cloned_cols;
    std::shared_ptr<Schema> cloned_schema = std::make_shared<Schema>(*schema_);
    for (auto &col : cols_) {
        assert(col != nullptr);
        cloned_cols.push_back(col->clone());
    }
    return std::make_shared<GTable>(cloned_schema, cloned_cols, ctx_);
}

std::shared_ptr<GTable> GTable::select_columns(const std::vector<std::string> &column_names) const {
    assert(schema_);
    std::vector<int> indices;
    indices.reserve(column_names.size());
    for (const auto &name : column_names) {
        auto maybe_index = schema_->get_schema()->GetFieldIndex(name);
        if (maybe_index == -1) {
            // Handle error: column name not found
            throw std::runtime_error("Column name " + name + " not found in table schema.");
        }
        indices.push_back(maybe_index);
    }

    std::vector<std::shared_ptr<GColumn>> selected_columns;
    std::vector<std::shared_ptr<arrow::Field>> selected_fields;

    for (int index : indices) {
        selected_columns.push_back(cols_[index]);
        selected_fields.push_back(schema_->get_schema()->field(index));
    }

    auto selected_schema = std::make_shared<Schema>(selected_fields);
    return std::make_shared<GTable>(selected_schema, selected_columns, ctx_);
}

arrow::Status GTable::Make(std::shared_ptr<arrow::RecordBatch> &host_batch,
                           std::shared_ptr<GTable> &device_table,
                           std::shared_ptr<MaximusGContext> &device_ctx) {
    arrow::ArrayVector array_vectors = host_batch->columns();
    int num_columns                  = host_batch->num_columns();
    std::vector<std::shared_ptr<GColumn>> columns(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        // Transfer every column from CPU to GPU
        arrow::Status status = GColumn::Make(array_vectors[i], columns[i], device_ctx);
        if (!status.ok()) return status;
    }

    // Construct Gtable on device
    device_table = std::make_shared<GTable>(
        std::make_shared<Schema>(host_batch->schema()), columns, device_ctx);
    return arrow::Status::OK();
}

arrow::Status GTable::Compose(std::shared_ptr<GTable> device_table,
                              std::shared_ptr<arrow::RecordBatch> &host_batch,
                              arrow::MemoryPool *pool) {
    arrow::FieldVector fields = device_table->schema_->get_schema()->fields();
    int num_fields            = fields.size();
    int num_rows              = device_table->cols_[0]->get_length();

    // Transfer every column from GPU to CPU
    arrow::ArrayVector arrays(num_fields);
    for (int i = 0; i < num_fields; ++i) {
        arrow::Status status = GColumn::Compose(device_table->cols_[i], num_rows, arrays[i], pool);
        if (!status.ok()) return status;
    }

    // construct recordbatch on CPU
    host_batch =
        arrow::RecordBatch::Make(device_table->schema_->get_schema(), num_rows, std::move(arrays));
    return arrow::Status::OK();
}
}  // namespace gpu
}  // namespace maximus
