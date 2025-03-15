#pragma once

#include <cstdint>
#include <string>

namespace maximus {
enum class PhysicalOperatorType : uint8_t {
    FILTER,
    PROJECT,
    HASH_JOIN,
    ORDER_BY,
    GROUP_BY,
    RANDOM_TABLE_SOURCE,
    TABLE_SOURCE,
    TABLE_SINK,
    LIMIT,
    DISTINCT,
    FUSED,
    LOCAL_BROADCAST,
    UNDEFINED
};

std::string physical_operator_to_string(PhysicalOperatorType type);
}  // namespace maximus
