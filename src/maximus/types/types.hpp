#pragma once
#include <arrow/type.h>

#include <maximus/error_handling.hpp>

namespace maximus {
// we want to define maximus::DataType to be the same (alias) as the arrow:DataType
using DataType            = arrow::DataType;
using Type                = arrow::Type;
using DataTypeLayout      = arrow::DataTypeLayout;
using FixedSizeBinaryType = arrow::FixedSizeBinaryType;
using TimeUnit            = arrow::TimeUnit;
using TimestampType       = arrow::TimestampType;
using DurationType        = arrow::DurationType;
using DecimalType         = arrow::DecimalType;
using FixedSizeBinaryType = arrow::FixedSizeBinaryType;

Status are_types_supported(const std::shared_ptr<arrow::Schema> &schema);

std::shared_ptr<DataType> to_maximus_type(std::shared_ptr<arrow::DataType> type);
std::shared_ptr<arrow::DataType> to_arrow_type(std::shared_ptr<DataType> type);

}  // namespace maximus
