#pragma once

#include <maximus/gpu/gtable/gcolumn.hpp>
#include <maximus/types/schema.hpp>

namespace maximus {

namespace gpu {

class GTable {
public:
    GTable() = default;

    GTable(std::shared_ptr<Schema> schema,
           std::vector<std::shared_ptr<GColumn>> &cols,
           const std::shared_ptr<MaximusGContext> &ctx);

    /**
     * To get the device context
     */
    const std::shared_ptr<MaximusGContext> &get_context() const;

    /**
     * To get the schema
     */
    std::shared_ptr<Schema> &get_schema();

    /**
     * To get the table columns
     */
    std::vector<std::shared_ptr<GColumn>> &get_table();

    /**
     * To get the number of columns
     */
    int get_num_columns();

    /**
     * To get the number of rows
     */
    int64_t get_num_rows();

    /**
     * To clone a GTable
     */
    std::shared_ptr<GTable> clone();

    std::shared_ptr<GTable> select_columns(const std::vector<std::string> &include_columns) const;

    /**
     * To transfer a CPU recordbatch to a GPU GTable
     */
    static arrow::Status Make(std::shared_ptr<arrow::RecordBatch> &host_batch,
                              std::shared_ptr<GTable> &device_table,
                              std::shared_ptr<MaximusGContext> &device_ctx);

    /**
     * To transfer a GPU GTable to a CPU recordbatch
     */
    static arrow::Status Compose(std::shared_ptr<GTable> device_table,
                                 std::shared_ptr<arrow::RecordBatch> &host_batch,
                                 arrow::MemoryPool *pool = arrow::default_memory_pool());

private:
    std::shared_ptr<Schema> schema_;
    std::vector<std::shared_ptr<GColumn>> cols_;
    const std::shared_ptr<MaximusGContext> ctx_;
};

}  // namespace gpu
}  // namespace maximus
