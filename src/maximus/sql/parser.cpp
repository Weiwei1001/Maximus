#include <maximus/sql/parser.hpp>

#include <util/sqlhelper.h>

namespace maximus {

Status extract_join_keys(std::vector<std::vector<arrow::FieldRef>>& column_names,
                         std::unordered_map<std::string, int>& table_name_to_idx,
                         hsql::Expr* condition) {
    switch (condition->opType) {
        case hsql::OperatorType::kOpAnd: {
            hsql::Expr* left  = condition->expr;
            hsql::Expr* right = condition->expr2;
            assert(left);
            assert(right);

            check_status(
                extract_join_keys(column_names, table_name_to_idx, condition->expr));
            check_status(
                extract_join_keys(column_names, table_name_to_idx, condition->expr2));
            break;
        }
        case hsql::OperatorType::kOpEquals: {
            assert(condition->expr && condition->expr2);
            hsql::Expr* left  = condition->expr;
            hsql::Expr* right = condition->expr2;
            assert(left);
            assert(right);

            assert(left->type == hsql::kExprColumnRef);
            assert(right->type == hsql::kExprColumnRef);

            std::string table1_name  = left->table;
            std::string column1_name = left->name;

            std::string table2_name  = right->table;
            std::string column2_name = right->name;

            column_names[table_name_to_idx[table1_name]].push_back(column1_name);
            column_names[table_name_to_idx[table2_name]].push_back(column2_name);

            break;
        }
        default: {
            return Status(ErrorCode::MaximusError, "Unsupported join condition.");
        }
    }
    return Status::OK();
}

Status Parser::join_config_from_expr(std::string left_table_name,
                                     std::string right_table_name,
                                     hsql::JoinDefinition* join,
                                     std::shared_ptr<JoinProperties>& join_config) {
    assert(join);

    std::unordered_map<hsql::JoinType, maximus::JoinType> to_join_type = {
        {hsql::JoinType::kJoinInner, JoinType::INNER},
        {hsql::JoinType::kJoinLeft, JoinType::LEFT_SEMI},
        {hsql::JoinType::kJoinRight, JoinType::RIGHT_SEMI},
        {hsql::JoinType::kJoinNatural, JoinType::INNER},
        {hsql::JoinType::kJoinFull, JoinType::FULL_OUTER},
        {hsql::JoinType::kJoinCross, JoinType::CROSS_JOIN}};

    // assert(join->type);
    if (to_join_type.find(join->type) == to_join_type.end()) {
        return Status(ErrorCode::MaximusError, "Unsupported join type.");
    }

    JoinType join_type = to_join_type[join->type];

    assert(join->condition);

    hsql::Expr* condition = join->condition;

    assert(condition->opType);

    std::vector<std::vector<arrow::FieldRef>> column_names(2, std::vector<arrow::FieldRef>());

    std::unordered_map<std::string, int> table_name_to_idx = {{left_table_name, 0},
                                                              {right_table_name, 1}};

    check_status(extract_join_keys(column_names, table_name_to_idx, condition));

    join_config = std::make_shared<JoinProperties>(join_type, column_names[0], column_names[1]);

    std::cout << "join properties = " << join_config->to_string() << std::endl;

    return Status::OK();
}

Status Parser::qp_from_table_ref(const hsql::TableRef* table,
                                 const std::shared_ptr<DatabaseCatalogue>& db_catalogue,
                                 std::shared_ptr<MaximusContext>& ctx,
                                 std::shared_ptr<QueryNode>& query_plan) {
    assert(ctx);
    assert(table);

    /*
    if (table->name) {
        std::cout << "name field = " << table->name << std::endl;
    }
    */

    switch (table->type) {
        case hsql::TableRefType::kTableName: {
            assert(table->name);
            std::string table_path = db_catalogue->table_path(table->name);
            auto properties        = std::make_shared<TableSourceProperties>(table_path);
            std::cout << properties->to_string() << std::endl;
            query_plan = std::make_shared<QueryNode>(
                EngineType::NATIVE, NodeType::TABLE_SOURCE, std::move(properties), ctx);
            assert(query_plan);
            std::cout << query_plan->to_string() << std::endl;
            // std::cout << "table scan = " << query_plan->get_operator()->to_string() << std::endl;
            break;
        }

        case hsql::TableRefType::kTableJoin: {
            // std::cout << "table HashJoin node created" << std::endl;
            assert(table->join);
            assert(table->join->left);
            assert(table->join->right);
            assert(table->join->condition);

            std::shared_ptr<QueryNode> left;
            check_status(qp_from_table_ref(table->join->left, db_catalogue, ctx, left));
            std::shared_ptr<QueryNode> right;
            check_status(qp_from_table_ref(table->join->right, db_catalogue, ctx, right));
            std::shared_ptr<JoinProperties> join_config;
            std::string left_table_name  = table->join->left->name;
            std::string right_table_name = table->join->right->name;
            check_status(
                join_config_from_expr(left_table_name, right_table_name, table->join, join_config));

            query_plan = std::make_shared<QueryNode>(
                EngineType::ACERO, NodeType::HASH_JOIN, std::move(join_config), ctx);

            assert(left);
            assert(right);
            // std::cout << "Left = " << left->get_operator()->to_string() << std::endl;
            // std::cout << "Right = " << right->get_operator()->to_string() << std::endl;
            assert(query_plan);
            query_plan->add_input(std::move(left));
            query_plan->add_input(std::move(right));
            std::cout << query_plan->to_string() << std::endl;
            assert(query_plan->in_degree() == 2);
            break;
        }

        case hsql::TableRefType::kTableSelect: {
            qp_from_select(table->select, db_catalogue, ctx, query_plan);
            break;
        }

        case hsql::TableRefType::kTableCrossProduct: {
            return Status(ErrorCode::MaximusError, "CROSS JOIN is not supported yet.");
        }
    }

    assert(query_plan);

    // std::cout << "Finished qp_from_table_ref" << std::endl;

    return Status::OK();
}

Status Parser::qp_from_select(const hsql::SelectStatement* select,
                              const std::shared_ptr<DatabaseCatalogue>& db_catalogue,
                              std::shared_ptr<MaximusContext>& ctx,
                              std::shared_ptr<QueryNode>& query_plan) {
    hsql::printSelectStatementInfo(select, 0);

    std::vector<std::string> field_names;

    // handling the Fields
    for (hsql::Expr* expr : *select->selectList) {
        switch (expr->type) {
            case hsql::kExprStar:
                break;
            case hsql::kExprColumnRef:
                field_names.push_back(expr->name);
                break;
            default:
                return Status(ErrorCode::MaximusError,
                              "Unsupported expression in the specified SQL query.");
        }
    }

    // handling the Sources
    if (select->fromTable) {
        hsql::TableRef* table = select->fromTable;
        qp_from_table_ref(table, db_catalogue, ctx, query_plan);
        assert(query_plan);
    }

    if (select->whereClause) {
        return Status(ErrorCode::MaximusError, "WHERE clause is not supported yet.");
    }

    if (select->whereClause) {
        return Status(ErrorCode::MaximusError, "WHERE clause is not supported yet.");
    }

    if (select->groupBy) {
        return Status(ErrorCode::MaximusError, "GROUP BY clause is not supported yet.");
    }

    if (select->lockings) {
        return Status(ErrorCode::MaximusError, "LOCK clause is not supported yet.");
    }

    if (select->setOperations) {
        return Status(ErrorCode::MaximusError, "Set operations are not supported yet.");
    }

    if (select->order) {
        return Status(ErrorCode::MaximusError, "ORDER BY clause is not supported yet.");
    }

    if (select->limit && select->limit->limit) {
        return Status(ErrorCode::MaximusError, "LIMIT clause is not supported yet.");
    }

    if (select->limit && select->limit->offset) {
        return Status(ErrorCode::MaximusError, "OFFSET clause is not supported yet.");
    }

    return Status::OK();
}

Status Parser::query_plan_from_sql(const std::string& sql_query,
                                   const std::shared_ptr<DatabaseCatalogue>& db_catalogue,
                                   std::shared_ptr<MaximusContext>& ctx,
                                   std::shared_ptr<QueryPlan>& query_plan) {
    assert(ctx);
    assert(db_catalogue);
    assert(!query_plan);

    hsql::SQLParserResult result;
    hsql::SQLParser::parse(sql_query, &result);

    if (!result.isValid() || result.size() == 0) {
        return Status(ErrorCode::MaximusError, "Invalid SQL query");
    }

    assert(result.size() == 1);

    const hsql::SQLStatement* statement = result.getStatement(0);

    switch (statement->type()) {
        case hsql::kStmtSelect: {
            const auto* select = static_cast<const hsql::SelectStatement*>(statement);
            // hsql::printSelectStatementInfo(select, 0);

            std::shared_ptr<QueryNode> inner_qp;
            check_status(qp_from_select(select, db_catalogue, ctx, inner_qp));

            // std::cout << "Finished qp_from_select " << std::endl;

            assert(inner_qp);

            assert(ctx);

            // add the sink on top of the inner query plan
            auto sink_properties = std::make_shared<TableSinkProperties>();
            auto sink            = std::make_shared<QueryNode>(
                EngineType::NATIVE, NodeType::TABLE_SINK, sink_properties, ctx);
            sink->add_input(inner_qp);

            // add the query plan root on top of the sink
            query_plan = std::make_shared<QueryPlan>(ctx);
            assert(query_plan);
            query_plan->add_input(sink);

            assert(query_plan);
            assert(query_plan->in_degree() == 1);

            std::cout << "Full query plan = \n" << query_plan->to_string() << std::endl;
            // std::cout << "Inner query plan = \n" << inner_qp->to_string() << std::endl;

            break;
        }
        default: {
            return Status(ErrorCode::MaximusError, "Only SELECT statements are supported");
        }
    }

    return Status::OK();
}

arrow::compute::Expression Parser::parse_expression(const hsql::Expr* expr) {
    using arrow::compute::and_;
    using arrow::compute::call;
    using arrow::compute::equal;
    using arrow::compute::field_ref;
    using arrow::compute::greater;
    using arrow::compute::greater_equal;
    using arrow::compute::is_null;
    using arrow::compute::is_valid;
    using arrow::compute::less;
    using arrow::compute::less_equal;
    using arrow::compute::literal;
    using arrow::compute::not_;
    using arrow::compute::not_equal;
    using arrow::compute::or_;

    arrow::compute::Expression expression;

    // Handle different expression types
    switch (expr->type) {
        case hsql::kExprLiteralFloat:
            expression = literal(static_cast<double>(expr->fval));
            break;
        case hsql::kExprLiteralInt:
            expression = literal(static_cast<int64_t>(expr->ival));
            break;
        case hsql::kExprLiteralString:
            expression = literal(std::string(expr->name));
            break;
        case hsql::kExprColumnRef:
            expression = field_ref(expr->name);
            break;
        case hsql::kExprOperator:
            // Map operator type
            switch (expr->opType) {
                case hsql::kOpEquals:
                    expression = equal(parse_expression(expr->expr), parse_expression(expr->expr2));
                    break;
                case hsql::kOpNotEquals:
                    expression =
                        not_equal(parse_expression(expr->expr), parse_expression(expr->expr2));
                    break;
                case hsql::kOpLess:
                    expression = less(parse_expression(expr->expr), parse_expression(expr->expr2));
                    break;
                case hsql::kOpLessEq:
                    expression =
                        less_equal(parse_expression(expr->expr), parse_expression(expr->expr2));
                    break;
                case hsql::kOpGreater:
                    expression =
                        greater(parse_expression(expr->expr), parse_expression(expr->expr2));
                    break;
                case hsql::kOpGreaterEq:
                    expression =
                        greater_equal(parse_expression(expr->expr), parse_expression(expr->expr2));
                    break;
                case hsql::kOpAnd:
                    expression = and_(parse_expression(expr->expr), parse_expression(expr->expr2));
                    break;
                case hsql::kOpOr:
                    expression = or_(parse_expression(expr->expr), parse_expression(expr->expr2));
                    break;
                case hsql::kOpNot:
                    expression = not_(parse_expression(expr->expr));
                    break;
                case hsql::kOpIsNull:
                    expression = is_null(parse_expression(expr->expr));
                    break;
                case hsql::kOpExists:
                    expression = is_valid(parse_expression(expr->expr));
                    break;
                case hsql::kOpPlus:
                    expression =
                        call("add", {parse_expression(expr->expr), parse_expression(expr->expr2)});
                    break;
                case hsql::kOpMinus:
                    expression = call(
                        "subtract", {parse_expression(expr->expr), parse_expression(expr->expr2)});
                    break;
                case hsql::kOpAsterisk:
                    expression = call(
                        "multiply", {parse_expression(expr->expr), parse_expression(expr->expr2)});
                    break;

                // Handle other operators similarly
                // ...
                default:
                    throw std::runtime_error("Unsupported operator type");
            }
            break;

        default:
            throw std::runtime_error("Unsupported expression type");
    }

    return expression;
}

}  // namespace maximus

