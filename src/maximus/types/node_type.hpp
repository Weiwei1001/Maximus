#pragma once

#include <cstdint>
#include <maximus/types/physical_operator_type.hpp>
#include <string>

namespace maximus {
enum class NodeType : uint8_t {
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
    QUERY_PLAN_ROOT,
    UNDEFINED,
};

std::string node_type_to_string(NodeType type);
PhysicalOperatorType node_type_to_operator_type(NodeType type);

}  // namespace maximus
