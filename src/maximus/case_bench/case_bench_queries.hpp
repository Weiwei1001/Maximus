#pragma once

// case_bench: a curated set of SQL queries where GPU database engines
// are NOT expected to deliver a significant energy / throughput advantage
// over CPU engines. Typical reasons: tiny input tables, very selective
// point lookups, short result projections, small-cardinality group-bys.
// Data is shared with TPC-H (reuses tests/tpch/csv-{sf}).

#include <maximus/dag/query_plan.hpp>
#include <maximus/database.hpp>
#include <maximus/tpch/tpch_queries.hpp>

namespace maximus::case_bench {

// case_bench reuses TPC-H tables and schemas, so table_names()/schemas()
// delegate to tpch.
inline std::vector<std::string> table_names() { return maximus::tpch::table_names(); }
inline std::vector<std::shared_ptr<Schema>> schemas() { return maximus::tpch::schemas(); }

std::shared_ptr<QueryPlan> q1(std::shared_ptr<Database>& db, DeviceType device = DeviceType::CPU);
std::shared_ptr<QueryPlan> q2(std::shared_ptr<Database>& db, DeviceType device = DeviceType::CPU);
std::shared_ptr<QueryPlan> q3(std::shared_ptr<Database>& db, DeviceType device = DeviceType::CPU);
std::shared_ptr<QueryPlan> q4(std::shared_ptr<Database>& db, DeviceType device = DeviceType::CPU);
std::shared_ptr<QueryPlan> q5(std::shared_ptr<Database>& db, DeviceType device = DeviceType::CPU);

std::shared_ptr<QueryPlan> query_plan(const std::string& q,
                                      std::shared_ptr<Database>& db,
                                      DeviceType device = DeviceType::CPU);

}  // namespace maximus::case_bench
