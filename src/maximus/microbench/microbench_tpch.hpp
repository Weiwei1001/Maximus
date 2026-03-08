#pragma once
#include <maximus/dag/query_plan.hpp>
#include <maximus/database.hpp>
#include <maximus/types/device_table_ptr.hpp>
#include <maximus/tpch/tpch_queries.hpp>

namespace maximus::microbench_tpch {

std::shared_ptr<QueryPlan> query_plan(const std::string& q,
                                       std::shared_ptr<Database>& db,
                                       DeviceType device = DeviceType::CPU);

std::vector<std::string> table_names();
std::vector<std::shared_ptr<Schema>> schemas();
}  // namespace maximus::microbench_tpch
