// case_bench queries: reuse TPC-H tables (region, nation, orders,
// customer) but with query shapes where GPU advantage is typically weak:
// tiny scans, point lookups, small-cardinality group-bys, narrow
// sort-limits. Used to measure the crossover region where CPU engines
// stay competitive on energy per query.
#include <maximus/case_bench/case_bench_queries.hpp>
#include <maximus/frontend/expressions.hpp>
#include <maximus/frontend/query_plan_api.hpp>
#include <maximus/types/expression.hpp>

namespace cp = arrow::compute;

namespace maximus::case_bench {

// q1: full scan of a tiny table (region = 5 rows). GPU kernel launch +
//     H2D transfer overhead dominates any compute savings.
//   SQL: SELECT r_regionkey, r_name, r_comment FROM region;
std::shared_ptr<QueryPlan> q1(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db,
                               "region",
                               tpch::schema("region"),
                               {"r_regionkey", "r_name", "r_comment"},
                               device);
    return query_plan(table_sink(source));
}

// q2: point lookup by primary key on nation (25 rows). Branch prediction
//     on CPU is ideal; GPU does a full scan + kernel launch for 1 match.
//   SQL: SELECT n_name FROM nation WHERE n_nationkey = 5;
std::shared_ptr<QueryPlan> q2(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db,
                               "nation",
                               tpch::schema("nation"),
                               {"n_nationkey", "n_name"},
                               device);
    auto filter_expr = expr(arrow_expr(cp::field_ref("n_nationkey"), "==",
                                       int32_literal(5)));
    auto filtered = filter(source, filter_expr, device);
    auto n_name   = Expression::from_field_ref("n_name");
    auto proj     = project(filtered, {n_name}, {"n_name"}, device);
    return query_plan(table_sink(proj));
}

// q3: small-cardinality group-by (nation → 5 region groups). Hashing 25
//     keys on GPU is dwarfed by fixed overhead.
//   SQL: SELECT n_regionkey, COUNT(*) FROM nation GROUP BY n_regionkey;
std::shared_ptr<QueryPlan> q3(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db,
                               "nation",
                               tpch::schema("nation"),
                               {"n_regionkey"},
                               device);
    auto count_opts = count_all();
    std::vector<std::shared_ptr<Aggregate>> aggs = {
        aggregate("hash_count", count_opts, "n_regionkey", "count_nations"),
    };
    auto gb = group_by(source, {"n_regionkey"}, aggs, device);
    return query_plan(table_sink(gb));
}

// q4: filter + global aggregate on a small table. Data volume too small
//     to amortize GPU.
//   SQL: SELECT n_regionkey, COUNT(*) FROM nation WHERE n_regionkey < 3
//        GROUP BY n_regionkey;
std::shared_ptr<QueryPlan> q4(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db,
                               "nation",
                               tpch::schema("nation"),
                               {"n_regionkey"},
                               device);
    auto fexpr  = expr(arrow_expr(cp::field_ref("n_regionkey"), "<",
                                  int32_literal(3)));
    auto filt   = filter(source, fexpr, device);
    auto count_opts = count_all();
    std::vector<std::shared_ptr<Aggregate>> aggs = {
        aggregate("hash_count", count_opts, "n_regionkey", "count_nations"),
    };
    auto gb = group_by(filt, {"n_regionkey"}, aggs, device);
    return query_plan(table_sink(gb));
}

// q5: narrow top-N on a larger table (orders). Sort + limit pulls tiny
//     output; CPU can use a partial heap-sort and avoids a full sort.
//   SQL: SELECT o_orderkey, o_orderdate FROM orders
//        ORDER BY o_orderdate ASC LIMIT 10;
std::shared_ptr<QueryPlan> q5(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db,
                               "orders",
                               tpch::schema("orders"),
                               {"o_orderkey", "o_orderdate"},
                               device);
    std::vector<SortKey> keys = {{"o_orderdate"}};
    auto sorted = order_by(source, keys, device);
    auto limited = limit(sorted, /*limit=*/10, /*offset=*/0, device);
    return query_plan(table_sink(limited));
}

std::shared_ptr<QueryPlan> query_plan(const std::string& q,
                                      std::shared_ptr<Database>& db,
                                      DeviceType device) {
    if (q == "q1") return q1(db, device);
    if (q == "q2") return q2(db, device);
    if (q == "q3") return q3(db, device);
    if (q == "q4") return q4(db, device);
    if (q == "q5") return q5(db, device);
    throw std::runtime_error("case_bench: unknown query '" + q + "'");
}

}  // namespace maximus::case_bench
