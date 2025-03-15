#include <arrow/vendored/pcg/pcg_random.hpp>
#include <maximus/frontend/expressions.hpp>
#include <maximus/frontend/query_plan_api.hpp>
#include <maximus/operators/native/table_source_operator.hpp>

namespace maximus {

std::shared_ptr<QueryNode> filter(const std::shared_ptr<QueryNode> &input_node,
                                  const std::shared_ptr<Expression> &filter_expr,
                                  DeviceType device) {
    auto ctx               = input_node->get_context();
    auto filter_properties = std::make_shared<FilterProperties>(filter_expr);
    EngineType engine      = EngineType::ACERO;
    switch (device) {
        case DeviceType::GPU:
            engine = EngineType::CUDF;
            break;
        default:
            break;
    }

    auto filter_node =
        std::make_shared<QueryNode>(engine, NodeType::FILTER, std::move(filter_properties), ctx);
    filter_node->add_input(input_node);
    return filter_node;
}

std::shared_ptr<QueryNode> distinct(const std::shared_ptr<QueryNode> &input_node,
                                    const std::vector<std::string> &column_names,
                                    DeviceType device) {
    auto ctx = input_node->get_context();
    std::vector<arrow::FieldRef> fields;
    fields.reserve(column_names.size());
    for (const auto &column_name : column_names) {
        fields.emplace_back(column_name);
    }
    auto distinct_properties = std::make_shared<DistinctProperties>(fields);
    EngineType engine_type   = EngineType::ACERO;
    engine_type              = device == DeviceType::GPU ? EngineType::CUDF : EngineType::ACERO;
    auto distinct_node       = std::make_shared<QueryNode>(
        engine_type, NodeType::DISTINCT, std::move(distinct_properties), ctx);
    distinct_node->add_input(input_node);
    return distinct_node;
}

std::shared_ptr<QueryNode> project(const std::shared_ptr<QueryNode> &input_node,
                                   const std::vector<std::shared_ptr<Expression>> exprs,
                                   std::vector<std::string> column_names,
                                   DeviceType device) {
    auto ctx                = input_node->get_context();
    auto project_properties = std::make_shared<ProjectProperties>(exprs, column_names);
    EngineType engine       = EngineType::ACERO;
    switch (device) {
        case DeviceType::GPU:
            engine = EngineType::CUDF;
            break;
        default:
            break;
    }
    auto project_node =
        std::make_shared<QueryNode>(engine, NodeType::PROJECT, std::move(project_properties), ctx);
    project_node->add_input(input_node);
    return project_node;
}

std::shared_ptr<QueryNode> rename(const std::shared_ptr<QueryNode> &input_node,
                                  const std::vector<std::string> &old_column_names,
                                  const std::vector<std::string> &new_column_names,
                                  DeviceType device) {
    return project(input_node, exprs(old_column_names), new_column_names, device);
}

std::shared_ptr<QueryNode> project(const std::shared_ptr<QueryNode> &input_node,
                                   std::vector<std::string> column_names,
                                   DeviceType device) {
    auto expressions = exprs(column_names);
    expressions.reserve(column_names.size());
    auto ctx = input_node->get_context();
    auto project_properties =
        std::make_shared<ProjectProperties>(std::move(expressions), std::move(column_names));
    EngineType engine = device == DeviceType::GPU ? EngineType::CUDF : EngineType::ACERO;
    auto project_node =
        std::make_shared<QueryNode>(engine, NodeType::PROJECT, std::move(project_properties), ctx);
    project_node->add_input(input_node);
    return project_node;
}

std::shared_ptr<QueryNode> table_source(std::shared_ptr<Database> &db,
                                        const std::string &table_name,
                                        const std::shared_ptr<Schema> schema,
                                        const std::vector<std::string> &column_names,
                                        DeviceType device) {
    auto ctx          = db->get_context();
    auto db_catalogue = db->get_catalogue();

    std::shared_ptr<TableSourceProperties> source_properties;

    EngineType engine_type = device == DeviceType::CPU ? EngineType::NATIVE : EngineType::CUDF;

    // bool as_single_chunk = device == DeviceType::GPU;
    auto device_table = db->get_table(table_name);
    if (device_table && !device_table.empty()) {
        // the engine_type here depends on where the table is stored (CPU or GPU)
        engine_type       = device_table.on_cpu() ? EngineType::NATIVE : EngineType::CUDF;
        source_properties = std::make_shared<TableSourceProperties>(device_table, column_names);
    } else {
        // otherwise, the operator will load it from the file
        // the engine_type here depends on the device argument of this table_source function
        engine_type       = device == DeviceType::GPU ? EngineType::CUDF : EngineType::NATIVE;
        source_properties = std::make_shared<TableSourceProperties>(
            db_catalogue->table_path(table_name), schema, column_names);
    }

    assert(source_properties);

    auto source_node = std::make_shared<QueryNode>(
        engine_type, NodeType::TABLE_SOURCE, std::move(source_properties), ctx);

    return std::move(source_node);
}

std::shared_ptr<QueryNode> group_by(const std::shared_ptr<QueryNode> &input_node,
                                    const std::vector<std::string> &group_by_keys,
                                    const std::vector<std::shared_ptr<Aggregate>> &aggregates,
                                    DeviceType device) {
    auto ctx = input_node->get_context();
    std::vector<arrow::FieldRef> keys(group_by_keys.size());
    std::transform(group_by_keys.begin(),
                   group_by_keys.end(),
                   keys.begin(),
                   [](const std::string &column_name) {
                       return arrow::FieldRef(column_name);
                   });
    auto group_by_properties = std::make_shared<GroupByProperties>(keys, aggregates);

    EngineType engine_type = EngineType::ACERO;
    engine_type            = device == DeviceType::GPU ? EngineType::CUDF : EngineType::ACERO;
    auto group_by_node     = std::make_shared<QueryNode>(
        engine_type, NodeType::GROUP_BY, std::move(group_by_properties), ctx);
    group_by_node->add_input(input_node);
    return group_by_node;
}
std::shared_ptr<QueryNode> order_by(const std::shared_ptr<QueryNode> &input_node,
                                    const std::vector<SortKey> &sort_keys,
                                    DeviceType device) {
    auto ctx = input_node->get_context();

    auto order_by_properties = std::make_shared<OrderByProperties>(sort_keys);
    EngineType engine_type   = EngineType::ACERO;
    engine_type              = device == DeviceType::GPU ? EngineType::CUDF : EngineType::ACERO;
    auto order_by_node       = std::make_shared<QueryNode>(
        engine_type, NodeType::ORDER_BY, std::move(order_by_properties), ctx);
    order_by_node->add_input(input_node);
    return order_by_node;
}
std::shared_ptr<QueryNode> join(const JoinType &join_type,
                                const std::shared_ptr<QueryNode> &left_node,
                                const std::shared_ptr<QueryNode> &right_node,
                                const std::vector<std::string> &left_keys,
                                const std::vector<std::string> &right_keys,
                                const std::string &left_suffix,
                                const std::string &right_suffix,
                                DeviceType device) {
    auto ctx = left_node->get_context();

    // Convert column names to FieldRef
    std::vector<arrow::FieldRef> left_keys_ref(left_keys.size());
    std::transform(left_keys.begin(),
                   left_keys.end(),
                   left_keys_ref.begin(),
                   [](const std::string &column_name) {
                       return arrow::FieldRef(column_name);
                   });

    // Convert column names to FieldRef
    std::vector<arrow::FieldRef> right_keys_ref(right_keys.size());
    std::transform(right_keys.begin(),
                   right_keys.end(),
                   right_keys_ref.begin(),
                   [](const std::string &column_name) {
                       return arrow::FieldRef(column_name);
                   });

    auto join_properties = std::make_shared<JoinProperties>(
        join_type,
        std::move(left_keys_ref),
        std::move(right_keys_ref),
        std::make_shared<Expression>(
            std::make_shared<arrow::compute::Expression>(arrow::compute::literal(true))),
        left_suffix,
        right_suffix);

    EngineType engine_type = EngineType::ACERO;
    engine_type            = device == DeviceType::GPU ? EngineType::CUDF : EngineType::ACERO;

    auto join_node = std::make_shared<QueryNode>(
        engine_type, NodeType::HASH_JOIN, std::move(join_properties), ctx);

    join_node->add_input(left_node);
    join_node->add_input(right_node);

    return join_node;
}

std::shared_ptr<QueryNode> inner_join(const std::shared_ptr<QueryNode> &left_node,
                                      const std::shared_ptr<QueryNode> &right_node,
                                      const std::vector<std::string> &left_keys,
                                      const std::vector<std::string> &right_keys,
                                      const std::string &left_suffix,
                                      const std::string &right_suffix,
                                      DeviceType device) {
    return join(JoinType::INNER,
                left_node,
                right_node,
                left_keys,
                right_keys,
                left_suffix,
                right_suffix,
                device);
}

std::shared_ptr<QueryNode> cross_join(const std::shared_ptr<QueryNode> &left_node,
                                      const std::shared_ptr<QueryNode> &right_node,
                                      const std::vector<std::string> &left_columns,
                                      const std::vector<std::string> &right_columns,
                                      const std::string &left_suffix,
                                      const std::string &right_suffix,
                                      DeviceType device) {
    // a workaround since acero does not support cross join
    // we add a column of 1s to both tables and inner-join on that column
    // then we remove this column
    if (device == DeviceType::CPU) {
        auto left_fields  = left_columns;
        auto right_fields = right_columns;

        auto left_expressions  = exprs(left_fields);
        auto right_expressions = exprs(right_fields);

        // now we are adding a temporary column to both input nodes
        std::string temp_column_name = "temporary_column";
        left_fields.push_back(temp_column_name);
        right_fields.push_back(temp_column_name);

        // the new temporary column will hold the value 1
        left_expressions.push_back(expr(int32_literal(1)));
        right_expressions.push_back(expr(int32_literal(1)));

        // now we project the input nodes to include the temporary column
        auto left_project  = project(left_node, left_expressions, left_fields, device);
        auto right_project = project(right_node, right_expressions, right_fields, device);

        // now we join the two tables on the temporary column
        auto joined = inner_join(left_project,
                                 right_project,
                                 {temp_column_name},
                                 {temp_column_name},
                                 left_suffix,
                                 right_suffix,
                                 device);

        // keep in mind that the temporary columns might now have suffixes added to them
        auto left_temp_column  = temp_column_name + left_suffix;
        auto right_temp_column = temp_column_name + right_suffix;

        // the output schema is simply the concatenation of the input schemas
        std::vector<std::string> output_fields;
        output_fields.reserve(left_columns.size() + right_columns.size());

        // exclude the temporary columns from the output schema
        for (const auto &field_name : left_columns) {
            if (field_name != left_temp_column) {
                output_fields.push_back(field_name);
            }
        }
        for (const auto &field_name : right_columns) {
            if (field_name != right_temp_column) {
                output_fields.push_back(field_name);
            }
        }

        auto output_expressions = exprs(output_fields);

        // do the final projection to remove the temporary columns
        return project(joined, output_expressions, output_fields, device);
    }

    if (device == DeviceType::GPU) {
        return join(
            JoinType::CROSS_JOIN, left_node, right_node, {}, {}, left_suffix, right_suffix, device);
    }

    throw std::runtime_error("Cross join is not supported on the specified device.");
}


std::shared_ptr<QueryNode> left_semi_join(const std::shared_ptr<QueryNode> &left_node,
                                          const std::shared_ptr<QueryNode> &right_node,
                                          const std::vector<std::string> &left_keys,
                                          const std::vector<std::string> &right_keys,
                                          const std::string &left_suffix,
                                          const std::string &right_suffix,
                                          DeviceType device) {
    return join(JoinType::LEFT_SEMI,
                left_node,
                right_node,
                left_keys,
                right_keys,
                left_suffix,
                right_suffix,
                device);
}

std::shared_ptr<QueryNode> left_anti_join(const std::shared_ptr<QueryNode> &left_node,
                                          const std::shared_ptr<QueryNode> &right_node,
                                          const std::vector<std::string> &left_keys,
                                          const std::vector<std::string> &right_keys,
                                          const std::string &left_suffix,
                                          const std::string &right_suffix,
                                          DeviceType device) {
    return join(JoinType::LEFT_ANTI,
                left_node,
                right_node,
                left_keys,
                right_keys,
                left_suffix,
                right_suffix,
                device);
}

std::shared_ptr<QueryNode> left_outer_join(const std::shared_ptr<QueryNode> &left_node,
                                           const std::shared_ptr<QueryNode> &right_node,
                                           const std::vector<std::string> &left_keys,
                                           const std::vector<std::string> &right_keys,
                                           const std::string &left_suffix,
                                           const std::string &right_suffix,
                                           DeviceType device) {
    return join(JoinType::LEFT_OUTER,
                left_node,
                right_node,
                left_keys,
                right_keys,
                left_suffix,
                right_suffix,
                device);
}

std::shared_ptr<QueryNode> table_sink(const std::shared_ptr<QueryNode> &input_node) {
    auto ctx             = input_node->get_context();
    auto sink_properties = std::make_shared<TableSinkProperties>();
    auto engine_type     = EngineType::NATIVE;
    auto sink_node       = std::make_shared<QueryNode>(
        engine_type, NodeType::TABLE_SINK, std::move(sink_properties), ctx);
    sink_node->add_input(input_node);
    return sink_node;
}
std::shared_ptr<QueryNode> limit(const std::shared_ptr<QueryNode> &input_node,
                                 int64_t limit,
                                 int64_t offset,
                                 DeviceType device) {
    auto ctx              = input_node->get_context();
    auto limit_properties = std::make_shared<LimitProperties>(limit, offset);

    auto engine_type = EngineType::NATIVE;
    if (input_node->engine_type == EngineType::ACERO) {
        engine_type = EngineType::ACERO;
    }

    // we keep the limit engine the same as the input's engine.
    // However, if the input engine is ACERO, the fusing has to be enabled
    // because Acero's FETCH node, which acts as a LIMIT node, requires the input
    // to a FETCH node to be ordered. However, when used in isolation, the FETCH
    // operator does not have information on the ordering of the input data.
    if (engine_type == EngineType::ACERO && !ctx->fusing_enabled) {
        engine_type = EngineType::NATIVE;
    }

    if (device == DeviceType::GPU) {
        engine_type = EngineType::CUDF;
    }

    auto limit_node =
        std::make_shared<QueryNode>(engine_type, NodeType::LIMIT, std::move(limit_properties), ctx);
    limit_node->add_input(input_node);
    return limit_node;
}
std::shared_ptr<QueryPlan> query_plan(const std::shared_ptr<QueryNode> &sink_node) {
    auto ctx                              = sink_node->get_context();
    std::shared_ptr<QueryPlan> query_plan = std::make_shared<QueryPlan>(ctx);

    query_plan->add_input(sink_node);
    return query_plan;
}
}  // namespace maximus
