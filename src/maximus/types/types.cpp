#include <maximus/types/types.hpp>
#include <unordered_set>


namespace maximus {
// Define a set of supported types for quick lookup
const std::unordered_set<arrow::Type::type> supported_types = {arrow::Type::BOOL,
                                                               arrow::Type::UINT8,
                                                               arrow::Type::INT8,
                                                               arrow::Type::UINT16,
                                                               arrow::Type::INT16,
                                                               arrow::Type::UINT32,
                                                               arrow::Type::INT32,
                                                               arrow::Type::UINT64,
                                                               arrow::Type::INT64,
                                                               arrow::Type::HALF_FLOAT,
                                                               arrow::Type::FLOAT,
                                                               arrow::Type::DOUBLE,
                                                               arrow::Type::FIXED_SIZE_BINARY,
                                                               arrow::Type::BINARY,
                                                               arrow::Type::STRING,
                                                               arrow::Type::DATE32,
                                                               arrow::Type::DATE64,
                                                               arrow::Type::TIMESTAMP,
                                                               arrow::Type::TIME32,
                                                               arrow::Type::TIME64};

Status are_types_supported(const std::shared_ptr<arrow::Schema> &schema) {
    for (const auto &field : schema->fields()) {
        auto type_id = field->type()->id();

        if (supported_types.count(type_id)) {
            continue;
        }

        return {ErrorCode::MaximusError,
                "Unsupported type found in the schema: " + field->type()->ToString()};
    }
    return Status::OK();
}

std::shared_ptr<DataType> to_maximus_type(std::shared_ptr<arrow::DataType> type) {
    return type;
}

std::shared_ptr<arrow::DataType> to_arrow_type(std::shared_ptr<DataType> type) {
    return type;
}

}  // namespace maximus
