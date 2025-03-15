#include <maximus/types/node_type.hpp>

std::string maximus::node_type_to_string(NodeType type) {
    switch (type) {
        case NodeType::FILTER:
            return "FILTER";
        case NodeType::PROJECT:
            return "PROJECT";
        case NodeType::HASH_JOIN:
            return "HASH JOIN";
        case NodeType::ORDER_BY:
            return "ORDER BY";
        case NodeType::GROUP_BY:
            return "GROUP BY";
        case NodeType::RANDOM_TABLE_SOURCE:
            return "RANDOM TABLE SOURCE";
        case NodeType::TABLE_SOURCE:
            return "TABLE SOURCE";
        case NodeType::TABLE_SINK:
            return "TABLE SINK";
        case NodeType::LIMIT:
            return "LIMIT";
        case NodeType::DISTINCT:
            return "DISTINCT";
        case NodeType::FUSED:
            return "FUSED";
        case NodeType::LOCAL_BROADCAST:
            return "LOCAL BROADCAST";
        case NodeType::QUERY_PLAN_ROOT:
            return "QUERY PLAN ROOT";
        default:
            return "NOT SUPPORTED";
    }
}

maximus::PhysicalOperatorType maximus::node_type_to_operator_type(NodeType type) {
    switch (type) {
        case NodeType::FILTER:
            return PhysicalOperatorType::FILTER;
        case NodeType::PROJECT:
            return PhysicalOperatorType::PROJECT;
        case NodeType::HASH_JOIN:
            return PhysicalOperatorType::HASH_JOIN;
        case NodeType::ORDER_BY:
            return PhysicalOperatorType::ORDER_BY;
        case NodeType::GROUP_BY:
            return PhysicalOperatorType::GROUP_BY;
        case NodeType::RANDOM_TABLE_SOURCE:
            return PhysicalOperatorType::RANDOM_TABLE_SOURCE;
        case NodeType::TABLE_SOURCE:
            return PhysicalOperatorType::TABLE_SOURCE;
        case NodeType::TABLE_SINK:
            return PhysicalOperatorType::TABLE_SINK;
        case NodeType::LIMIT:
            return PhysicalOperatorType::LIMIT;
        case NodeType::DISTINCT:
            return PhysicalOperatorType::DISTINCT;
        case NodeType::FUSED:
            return PhysicalOperatorType::FUSED;
        case NodeType::QUERY_PLAN_ROOT:
            return PhysicalOperatorType::UNDEFINED;
        default:
            return PhysicalOperatorType::UNDEFINED;
    }
}
