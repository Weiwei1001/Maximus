#pragma once
#include <maximus/io/csv.hpp>
#include <maximus/types/aggregate.hpp>
#include <maximus/types/device_table_ptr.hpp>
#include <maximus/types/expression.hpp>
#include <maximus/types/node_type.hpp>
#include <maximus/types/schema.hpp>
#include <sstream>

namespace maximus {

class NodeProperties {
public:
    virtual ~NodeProperties() = default;

    [[nodiscard]] virtual std::string to_string() const { return "NodeProperties"; }
};

class LocalBroadcastProperties : public NodeProperties {
public:
    explicit LocalBroadcastProperties() = default;

    LocalBroadcastProperties(int num_output_ports): num_output_ports(num_output_ports) {}

    int num_output_ports = 0;

    bool should_replicate = true;

    [[nodiscard]] std::string to_string() const override { return "LocalBroadcastProperties"; }
};

class TableSinkProperties : public NodeProperties {
public:
    explicit TableSinkProperties() = default;

    [[nodiscard]] std::string to_string() const override { return "TableSinkProperties"; }
};

class RandomTableSourceProperties : public NodeProperties {
public:
    explicit RandomTableSourceProperties(std::shared_ptr<Schema> output_schema,
                                         std::size_t output_batch_size,
                                         std::size_t total_num_rows,
                                         int seed)
            : output_schema(std::move(output_schema))
            , output_batch_size(output_batch_size)
            , total_num_rows(total_num_rows)
            , seed(seed) {}

    std::shared_ptr<Schema> output_schema;
    std::size_t output_batch_size = 100;
    std::size_t total_num_rows    = 1000;
    int seed                      = 0;

    [[nodiscard]] std::string to_string() const override {
        std::stringstream ss;
        ss << "RandomTableSourceProperties {"
           << "output_batch_size: " << output_batch_size << ", total_num_rows: " << total_num_rows
           << ", seed: " << seed << "}";
        return ss.str();
    }
};

class TableSourceProperties : public NodeProperties {
public:
    explicit TableSourceProperties(std::string path,
                                   std::shared_ptr<Schema> schema           = nullptr,
                                   std::vector<std::string> include_columns = {})
            : path(path), schema(std::move(schema)), include_columns(std::move(include_columns)) {
        assert(this->path != "");
    }

    explicit TableSourceProperties(DeviceTablePtr _table,
                                   std::vector<std::string> include_columns = {})
            : table(std::move(_table)), include_columns(std::move(include_columns)) {
        assert(table);
        assert(table.is_table() || table.is_gtable());
        if (table.is_table()) {
            assert(table.as_table());
            schema = table.as_table()->get_schema();
        }
        if (table.is_gtable()) {
#ifdef MAXIMUS_WITH_CUDA
            assert(table.as_gtable());
            schema = table.as_gtable()->get_schema();
#else
            throw std::runtime_error("Maximus must be built with the CUDA support to use GTable");
#endif
        }
        assert(schema && schema->size() > 0);
    }

    // if the table is not given to this operator
    // then it will be read from the filesystem's path
    std::string path                         = "";
    std::shared_ptr<Schema> schema           = nullptr;
    std::vector<std::string> include_columns = {};

    // if the table is already provided to this operator
    // then it will be used directly
    DeviceTablePtr table;

    // the output batch size
    int64_t output_batch_size = -1;

    [[nodiscard]] std::string to_string() const override {
        std::stringstream ss;
        ss << "TableSourceProperties {"
           << "path: " << path << ", include_columns: [";
        for (const auto& field : include_columns) {
            ss << field << ", ";
        }
        ss << "],\nSchema = " << (schema ? schema->to_string() : "nullptr") << "}";
        return ss.str();
    }
};

class FilterProperties : public NodeProperties {
public:
    explicit FilterProperties(std::shared_ptr<Expression> filter_expression)
            : filter_expression(std::move(filter_expression)) {}

    std::shared_ptr<Expression> filter_expression;

    [[nodiscard]] std::string to_string() const override {
        return "FilterProperties { filter_expression: " + filter_expression->to_string() + " }";
    }
};

class ProjectProperties : public NodeProperties {
public:
    ProjectProperties(std::vector<std::shared_ptr<Expression>> project_expressions,
                      std::vector<std::string> column_names = {})
            : project_expressions(std::move(project_expressions))
            , column_names(std::move(column_names)) {}

    std::vector<std::shared_ptr<Expression>> project_expressions;
    std::vector<std::string> column_names;

    [[nodiscard]] std::string to_string() const override {
        std::stringstream ss;
        ss << "ProjectProperties {"
           << "project_expressions: [";
        for (const auto& expr : project_expressions) {
            ss << expr->to_string() << ", ";
        }
        ss << "], column_names: [";
        for (const auto& name : column_names) {
            ss << name << ", ";
        }
        ss << "]}";
        return ss.str();
    }
};

enum class JoinType {
    LEFT_SEMI,
    RIGHT_SEMI,
    LEFT_ANTI,
    RIGHT_ANTI,
    INNER,
    LEFT_OUTER,
    RIGHT_OUTER,
    FULL_OUTER,
    CROSS_JOIN
};

class JoinProperties : public NodeProperties {
public:
    JoinProperties() = default;

    explicit JoinProperties(
        JoinType join_type,
        std::vector<arrow::FieldRef> left_keys,
        std::vector<arrow::FieldRef> right_keys,
        std::shared_ptr<Expression> filter = std::make_shared<Expression>(
            std::make_shared<arrow::compute::Expression>(arrow::compute::literal(true))),
        std::string left_output_suffix  = "",
        std::string right_output_suffix = "")
            : join_type(join_type)
            , left_keys(std::move(left_keys))
            , right_keys(std::move(right_keys))
            , left_suffix(std::move(left_output_suffix))
            , right_suffix(std::move(right_output_suffix))
            , filter(std::move(filter)) {}

    JoinType join_type = JoinType::INNER;
    std::vector<arrow::FieldRef> left_keys;
    std::vector<arrow::FieldRef> right_keys;
    std::string left_suffix            = "";
    std::string right_suffix           = "";
    std::shared_ptr<Expression> filter = std::make_shared<Expression>(
        std::make_shared<arrow::compute::Expression>(arrow::compute::literal(true)));

    [[nodiscard]] std::string to_string() const override {
        std::stringstream ss;
        ss << "JoinProperties {"
           << "join_type: " << static_cast<int>(join_type) << ", left_keys: [";
        for (const auto& key : left_keys) {
            ss << key.ToString() << ", ";
        }
        ss << "], right_keys: [";
        for (const auto& key : right_keys) {
            ss << key.ToString() << ", ";
        }
        ss << "], left_suffix: " << left_suffix << ", right_suffix: " << right_suffix
           << ", filter: " << filter->to_string() << "}";
        return ss.str();
    }
};

enum class SortOrder { ASCENDING, DESCENDING };

enum class NullOrder { FIRST, LAST };

struct SortKey {
    SortKey(arrow::FieldRef field, SortOrder order = SortOrder::ASCENDING)
            : field(std::move(field)), order(order) {}
    arrow::FieldRef field;
    SortOrder order;
};

class OrderByProperties : public NodeProperties {
public:
    OrderByProperties(std::vector<SortKey> sort_keys, NullOrder null_order = NullOrder::FIRST)
            : sort_keys(std::move(sort_keys)), null_order(std::move(null_order)) {}

    std::vector<SortKey> sort_keys;
    NullOrder null_order;

    std::string to_string() const override {
        std::stringstream ss;
        ss << "OrderByProperties {"
           << "sort_keys: [";
        for (const auto& key : sort_keys) {
            ss << key.field.ToString() << " " << static_cast<int>(key.order) << ", ";
        }
        ss << "], null_order: " << static_cast<int>(null_order) << "}";
        return ss.str();
    }
};

class GroupByProperties : public NodeProperties {
public:
    explicit GroupByProperties(std::vector<arrow::FieldRef> group_keys,
                               std::vector<std::shared_ptr<Aggregate>> aggregates)
            : group_keys(std::move(group_keys)), aggregates(std::move(aggregates)) {}

    explicit GroupByProperties(std::vector<std::string> group_keys_string,
                               std::vector<std::shared_ptr<Aggregate>> aggregates)
            : aggregates(std::move(aggregates)) {
        std::vector<arrow::FieldRef> temp_group_keys;
        for (auto& gk : group_keys_string) {
            temp_group_keys.push_back(arrow::FieldRef(gk));
        }
        group_keys = temp_group_keys;
    }

    std::vector<arrow::FieldRef> group_keys;
    std::vector<std::shared_ptr<Aggregate>> aggregates;

    std::string to_string() const override {
        std::stringstream ss;
        ss << "GroupByProperties {"
           << "group_keys: [";
        for (const auto& key : group_keys) {
            ss << key.ToString() << ", ";
        }
        ss << "], aggregate_expressions: [";
        for (const auto& aggr : aggregates) {
            ss << aggr->to_string() << ", ";
        }
        ss << "]}";
        return ss.str();
    }
};

class LimitProperties : public NodeProperties {
public:
    LimitProperties(int64_t limit, int64_t offset): limit(limit), offset(offset) {}

    int64_t limit  = 0;
    int64_t offset = 0;

    std::string to_string() const override {
        std::stringstream ss;
        ss << "LimitProperties {"
           << "  limit: " << limit << "\n  offset: " << offset << "\n}";
        return ss.str();
    }
};

class DistinctProperties : public NodeProperties {
public:
    DistinctProperties(std::vector<arrow::FieldRef> distinct_keys = {})
            : distinct_keys(std::move(distinct_keys)) {}

    std::vector<arrow::FieldRef> distinct_keys;

    std::string to_string() const override {
        std::stringstream ss;
        ss << "DistinctProperties {"
           << "distinct_keys: [";
        for (const auto& key : distinct_keys) {
            ss << key.ToString() << ", ";
        }
        ss << "]}";
        return ss.str();
    }
};

class FusedProperties : public NodeProperties {
public:
    FusedProperties(std::vector<std::shared_ptr<NodeProperties>> properties,
                    std::vector<NodeType> node_types)
            : properties(std::move(properties)), node_types(std::move(node_types)) {}

    std::vector<std::shared_ptr<NodeProperties>> properties;
    std::vector<NodeType> node_types;

    std::string to_string() const override {
        std::stringstream ss;
        ss << "FusedProperties (\n";
        for (std::size_t i = 0; i < node_types.size(); ++i) {
            ss << node_type_to_string(node_types[i]);
            if (i < node_types.size() - 1) {
                ss << " + ";
            }
        }
        ss << ")\n";
        for (std::size_t i = 0; i < properties.size(); ++i) {
            ss << "  - " << node_type_to_string(node_types[i]) << ": " << properties[i]->to_string()
               << "\n";
        }
        return ss.str();
    }
};
}  // namespace maximus
