#pragma once

#include <maximus/dag/query_node.hpp>
#include <maximus/database.hpp>
#include <maximus/types/device_table_ptr.hpp>
#include <maximus/types/expression.hpp>
#include <string>

namespace maximus {
namespace cp = ::arrow::compute;
namespace ac = ::arrow::acero;

std::shared_ptr<QueryNode> table_source(std::shared_ptr<Database>& db,
                                        const std::string& table_name,
                                        const std::shared_ptr<Schema> schema         = nullptr,
                                        const std::vector<std::string>& column_names = {},
                                        DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryNode> filter(const std::shared_ptr<QueryNode>& input_node,
                                  const std::shared_ptr<Expression>& filter_expr,
                                  DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryNode> distinct(const std::shared_ptr<QueryNode>& input_node,
                                    const std::vector<std::string>& column_names,
                                    DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryNode> project(const std::shared_ptr<QueryNode>& input_node,
                                   const std::vector<std::shared_ptr<Expression>> exprs,
                                   std::vector<std::string> column_names = {},
                                   DeviceType device                     = DeviceType::CPU);

std::shared_ptr<QueryNode> project(const std::shared_ptr<QueryNode>& input_node,
                                   std::vector<std::string> column_names,
                                   DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryNode> rename(const std::shared_ptr<QueryNode>& input_node,
                                  const std::vector<std::string>& old_column_names,
                                  const std::vector<std::string>& new_column_names,
                                  DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryNode> group_by(const std::shared_ptr<QueryNode>& input_node,
                                    const std::vector<std::string>& group_by_keys,
                                    const std::vector<std::shared_ptr<Aggregate>>& aggregates,
                                    DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryNode> order_by(const std::shared_ptr<QueryNode>& input_node,
                                    const std::vector<SortKey>& sort_keys,
                                    DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryNode> join(const JoinType& join_type,
                                const std::shared_ptr<QueryNode>& left_node,
                                const std::shared_ptr<QueryNode>& right_node,
                                const std::vector<std::string>& left_keys,
                                const std::vector<std::string>& right_keys,
                                const std::string& left_suffix  = "",
                                const std::string& right_suffix = "",
                                DeviceType device               = DeviceType::CPU);

std::shared_ptr<QueryNode> inner_join(const std::shared_ptr<QueryNode>& left_node,
                                      const std::shared_ptr<QueryNode>& right_node,
                                      const std::vector<std::string>& left_keys,
                                      const std::vector<std::string>& right_keys,
                                      const std::string& left_suffix  = "",
                                      const std::string& right_suffix = "",
                                      DeviceType device               = DeviceType::CPU);

std::shared_ptr<QueryNode> cross_join(const std::shared_ptr<QueryNode>& left_node,
                                      const std::shared_ptr<QueryNode>& right_node,
                                      const std::vector<std::string>& left_columns,
                                      const std::vector<std::string>& right_columns,
                                      const std::string& left_suffix  = "",
                                      const std::string& right_suffix = "",
                                      DeviceType device               = DeviceType::CPU);

std::shared_ptr<QueryNode> left_semi_join(const std::shared_ptr<QueryNode>& left_node,
                                          const std::shared_ptr<QueryNode>& right_node,
                                          const std::vector<std::string>& left_keys,
                                          const std::vector<std::string>& right_keys,
                                          const std::string& left_suffix  = "",
                                          const std::string& right_suffix = "",
                                          DeviceType device               = DeviceType::CPU);

std::shared_ptr<QueryNode> left_anti_join(const std::shared_ptr<QueryNode>& left_node,
                                          const std::shared_ptr<QueryNode>& right_node,
                                          const std::vector<std::string>& left_keys,
                                          const std::vector<std::string>& right_keys,
                                          const std::string& left_suffix  = "",
                                          const std::string& right_suffix = "",
                                          DeviceType device               = DeviceType::CPU);

std::shared_ptr<QueryNode> left_outer_join(const std::shared_ptr<QueryNode>& left_node,
                                           const std::shared_ptr<QueryNode>& right_node,
                                           const std::vector<std::string>& left_keys,
                                           const std::vector<std::string>& right_keys,
                                           const std::string& left_suffix  = "",
                                           const std::string& right_suffix = "",
                                           DeviceType device               = DeviceType::CPU);

std::shared_ptr<QueryNode> table_sink(const std::shared_ptr<QueryNode>& input_node);

std::shared_ptr<QueryNode> limit(const std::shared_ptr<QueryNode>& input_node,
                                 int64_t limit,
                                 int64_t offset    = 0,
                                 DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryPlan> query_plan(const std::shared_ptr<QueryNode>& sink_node);
}  // namespace maximus
