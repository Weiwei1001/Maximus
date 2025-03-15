#pragma once
#include <SQLParser.h>

#include <iostream>
#include <maximus/context.hpp>
#include <maximus/dag/query_node.hpp>
#include <maximus/dag/query_plan.hpp>
#include <maximus/database.hpp>
#include <maximus/database_catalogue.hpp>
#include <maximus/operators/properties.hpp>
#include <memory>
#include <string>

namespace maximus {

class Parser {
public:
    static Status join_config_from_expr(std::string left_table_name,
                                        std::string right_table_name,
                                        hsql::JoinDefinition* join,
                                        std::shared_ptr<JoinProperties>& join_config);

    static Status qp_from_table_ref(const hsql::TableRef* table,
                                    const std::shared_ptr<DatabaseCatalogue>& db_catalogue,
                                    std::shared_ptr<MaximusContext>& ctx,
                                    std::shared_ptr<QueryNode>& query_plan);

    static Status qp_from_select(const hsql::SelectStatement* select,
                                 const std::shared_ptr<DatabaseCatalogue>& db_catalogue,
                                 std::shared_ptr<MaximusContext>& ctx,
                                 std::shared_ptr<QueryNode>& query_plan);

    static Status query_plan_from_sql(const std::string& sql_query,
                                      const std::shared_ptr<DatabaseCatalogue>& db_catalogue,
                                      std::shared_ptr<MaximusContext>& ctx,
                                      std::shared_ptr<QueryPlan>& query_plan);

    static arrow::compute::Expression parse_expression(const hsql::Expr* expr);
};
}  // namespace maximus

