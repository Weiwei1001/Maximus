#include <maximus/frontend/expressions.hpp>
#include <maximus/frontend/query_plan_api.hpp>
#include <maximus/microbench/microbench_h2o.hpp>
#include <maximus/types/expression.hpp>

namespace maximus::microbench_h2o {

using maximus::h2o::schema;
namespace cp = ::arrow::compute;

std::vector<std::string> table_names() { return {"groupby"}; }
std::vector<std::shared_ptr<Schema>> schemas() {
    return {h2o::schema("groupby")};
}

// w1_001: SELECT COUNT(*) FROM groupby;
static std::shared_ptr<QueryPlan> w1_001(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "v1", "count")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_002: SELECT SUM(v1) FROM groupby;
static std::shared_ptr<QueryPlan> w1_002(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1", "sum_v1")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_003: SELECT SUM(v3) FROM groupby;
static std::shared_ptr<QueryPlan> w1_003(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v3"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v3", "sum_v3")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_004: SELECT SUM(v1 * v2) FROM groupby;
static std::shared_ptr<QueryPlan> w1_004(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1", "v2"}, device);
    auto proj_exprs = {expr(cp::call("multiply", {cp::field_ref("v1"), cp::field_ref("v2")}))};
    std::vector<std::string> proj_names = {"v1_times_v2"};
    auto proj = project(source, proj_exprs, proj_names, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1_times_v2", "sum_product")};
    auto gb = group_by(proj, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_005: SELECT SUM(v1 + v2), AVG(v3 * v3) FROM groupby;
static std::shared_ptr<QueryPlan> w1_005(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1", "v2", "v3"}, device);
    auto proj_exprs = {
        expr(cp::call("add", {cp::field_ref("v1"), cp::field_ref("v2")})),
        expr(cp::call("multiply", {cp::field_ref("v3"), cp::field_ref("v3")}))
    };
    std::vector<std::string> proj_names = {"v1_plus_v2", "v3_sq"};
    auto proj = project(source, proj_exprs, proj_names, device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "v1_plus_v2", "sum_v1_plus_v2"),
        aggregate("hash_mean", "v3_sq", "avg_v3_sq")
    };
    auto gb = group_by(proj, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_006: SELECT AVG(v1), AVG(v2), AVG(v3) FROM groupby;
static std::shared_ptr<QueryPlan> w1_006(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1", "v2", "v3"}, device);
    auto aggs = {
        aggregate("hash_mean", "v1", "avg_v1"),
        aggregate("hash_mean", "v2", "avg_v2"),
        aggregate("hash_mean", "v3", "avg_v3")
    };
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_007: SELECT MIN(v1), MAX(v1), MIN(v3), MAX(v3) FROM groupby;
static std::shared_ptr<QueryPlan> w1_007(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1", "v3"}, device);
    auto aggs = {
        aggregate("min", "v1", "min_v1"),
        aggregate("max", "v1", "max_v1"),
        aggregate("min", "v3", "min_v3"),
        aggregate("max", "v3", "max_v3")
    };
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_008: SELECT COUNT(*) FROM groupby WHERE id4 > 50;
static std::shared_ptr<QueryPlan> w2_008(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id4", "v1"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("id4"), ">", int32_literal(50))), device);
    auto aggs = {aggregate("hash_count", count_all(), "v1", "count")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_009: SELECT SUM(v1) FROM groupby WHERE v2 > 50;
static std::shared_ptr<QueryPlan> w2_009(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1", "v2"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("v2"), ">", int32_literal(50))), device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1", "sum_v1")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_010: SELECT AVG(v3) FROM groupby WHERE id1 = 'id001';
static std::shared_ptr<QueryPlan> w2_010(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "v3"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("id1"), "==", string_literal("id001"))), device);
    auto aggs = {aggregate("hash_mean", "v3", "avg_v3")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_011: SELECT SUM(v1) FROM groupby WHERE id4 BETWEEN 10 AND 30;
static std::shared_ptr<QueryPlan> w2_011(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id4", "v1"}, device);
    auto flt = filter(source, expr(arrow_between(cp::field_ref("id4"), int32_literal(10), int32_literal(30))), device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1", "sum_v1")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_012: SELECT COUNT(*) FROM groupby WHERE v1 > 0 AND v2 > 0;
static std::shared_ptr<QueryPlan> w2_012(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1", "v2"}, device);
    auto flt = filter(source, expr(arrow_all({
        arrow_expr(cp::field_ref("v1"), ">", int32_literal(0)),
        arrow_expr(cp::field_ref("v2"), ">", int32_literal(0))
    })), device);
    auto aggs = {aggregate("hash_count", count_all(), "v1", "count")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_013: SELECT MIN(v3), MAX(v3) FROM groupby WHERE id6 < 100;
static std::shared_ptr<QueryPlan> w2_013(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id6", "v3"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("id6"), "<", int32_literal(100))), device);
    auto aggs = {aggregate("min", "v3", "min_v3"), aggregate("max", "v3", "max_v3")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_014: SELECT SUM(v1 + v2) FROM groupby WHERE v3 > -100;
static std::shared_ptr<QueryPlan> w2_014(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1", "v2", "v3"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("v3"), ">", float64_literal(-100))), device);
    auto proj_exprs = {expr(cp::call("add", {cp::field_ref("v1"), cp::field_ref("v2")}))};
    std::vector<std::string> proj_names = {"v1_plus_v2"};
    auto proj = project(flt, proj_exprs, proj_names, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1_plus_v2", "sum")};
    auto gb = group_by(proj, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_016: SELECT id1, SUM(v1) FROM groupby GROUP BY id1;
static std::shared_ptr<QueryPlan> w3_016(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "v1"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1", "sum_v1")};
    auto gb = group_by(source, {"id1"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_017: SELECT id2, SUM(v1), AVG(v2) FROM groupby GROUP BY id2;
static std::shared_ptr<QueryPlan> w3_017(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id2", "v1", "v2"}, device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "v1", "sum_v1"),
        aggregate("hash_mean", "v2", "avg_v2")
    };
    auto gb = group_by(source, {"id2"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_018: SELECT id4, SUM(v1), SUM(v2) FROM groupby GROUP BY id4;
static std::shared_ptr<QueryPlan> w3_018(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id4", "v1", "v2"}, device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "v1", "sum_v1"),
        aggregate("hash_sum", sum_defaults(), "v2", "sum_v2")
    };
    auto gb = group_by(source, {"id4"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_019: SELECT id1, id2, SUM(v1) FROM groupby GROUP BY id1, id2;
static std::shared_ptr<QueryPlan> w3_019(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "id2", "v1"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1", "sum_v1")};
    auto gb = group_by(source, {"id1", "id2"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_020: SELECT id5, AVG(v3) FROM groupby GROUP BY id5;
static std::shared_ptr<QueryPlan> w3_020(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id5", "v3"}, device);
    auto aggs = {aggregate("hash_mean", "v3", "avg_v3")};
    auto gb = group_by(source, {"id5"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_021: SELECT id1, COUNT(*) FROM groupby GROUP BY id1;
static std::shared_ptr<QueryPlan> w3_021(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "id1", "count")};
    auto gb = group_by(source, {"id1"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_023: SELECT id1, COUNT(*), SUM(v1), AVG(v2), MIN(v3), MAX(v3) FROM groupby GROUP BY id1;
static std::shared_ptr<QueryPlan> w3_023(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "v1", "v2", "v3"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "id1", "count"),
        aggregate("hash_sum", sum_defaults(), "v1", "sum_v1"),
        aggregate("hash_mean", "v2", "avg_v2"),
        aggregate("min", "v3", "min_v3"),
        aggregate("max", "v3", "max_v3")
    };
    auto gb = group_by(source, {"id1"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_027: SELECT id3, SUM(v1) FROM groupby GROUP BY id3;
static std::shared_ptr<QueryPlan> w4_027(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id3", "v1"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1", "sum_v1")};
    auto gb = group_by(source, {"id3"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_028: SELECT id6, SUM(v1), SUM(v2) FROM groupby GROUP BY id6;
static std::shared_ptr<QueryPlan> w4_028(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id6", "v1", "v2"}, device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "v1", "sum_v1"),
        aggregate("hash_sum", sum_defaults(), "v2", "sum_v2")
    };
    auto gb = group_by(source, {"id6"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_029: SELECT id6, MIN(v3), MAX(v3) FROM groupby GROUP BY id6;
static std::shared_ptr<QueryPlan> w4_029(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id6", "v3"}, device);
    auto aggs = {aggregate("min", "v3", "min_v3"), aggregate("max", "v3", "max_v3")};
    auto gb = group_by(source, {"id6"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_030: SELECT id3, COUNT(*) FROM groupby GROUP BY id3;
static std::shared_ptr<QueryPlan> w4_030(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id3"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "id3", "count")};
    auto gb = group_by(source, {"id3"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_031: SELECT id1, id3, SUM(v1) FROM groupby GROUP BY id1, id3;
static std::shared_ptr<QueryPlan> w4_031(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "id3", "v1"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1", "sum_v1")};
    auto gb = group_by(source, {"id1", "id3"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_032: SELECT id4, id6, SUM(v1) FROM groupby GROUP BY id4, id6;
static std::shared_ptr<QueryPlan> w4_032(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id4", "id6", "v1"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "v1", "sum_v1")};
    auto gb = group_by(source, {"id4", "id6"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_033: SELECT id3, COUNT(*), AVG(v1), AVG(v2), AVG(v3) FROM groupby GROUP BY id3;
static std::shared_ptr<QueryPlan> w4_033(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id3", "v1", "v2", "v3"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "id3", "count"),
        aggregate("hash_mean", "v1", "avg_v1"),
        aggregate("hash_mean", "v2", "avg_v2"),
        aggregate("hash_mean", "v3", "avg_v3")
    };
    auto gb = group_by(source, {"id3"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w6_015: SELECT id1, v3 FROM groupby WHERE id4 > 50 ORDER BY v3 DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_015(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "id4", "v3"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("id4"), ">", int32_literal(50))), device);
    auto proj_node = project(flt, {"id1", "v3"}, device);
    std::vector<SortKey> sort_keys = {{"v3", SortOrder::DESCENDING}};
    auto ob = order_by(proj_node, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_022: SELECT id1, v3 FROM groupby ORDER BY v3 DESC LIMIT 10;
static std::shared_ptr<QueryPlan> w6_022(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "v3"}, device);
    std::vector<SortKey> sort_keys = {{"v3", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 10, 0, device);
    return query_plan(table_sink(lim));
}

// w6_024: SELECT id1, id2, v1 FROM groupby ORDER BY v1 DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_024(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "id2", "v1"}, device);
    std::vector<SortKey> sort_keys = {{"v1", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_025: SELECT id1, v1 + v2 AS total FROM groupby ORDER BY total DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_025(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "v1", "v2"}, device);
    auto proj_exprs = {
        expr(cp::field_ref("id1")),
        expr(cp::call("add", {cp::field_ref("v1"), cp::field_ref("v2")}))
    };
    std::vector<std::string> proj_names = {"id1", "total"};
    auto proj = project(source, proj_exprs, proj_names, device);
    std::vector<SortKey> sort_keys = {{"total", SortOrder::DESCENDING}};
    auto ob = order_by(proj, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_026: SELECT id1, v2 FROM groupby ORDER BY v2 DESC LIMIT 1000;
static std::shared_ptr<QueryPlan> w6_026(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"id1", "v2"}, device);
    std::vector<SortKey> sort_keys = {{"v2", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 1000, 0, device);
    return query_plan(table_sink(lim));
}

// w6_034: SELECT v1 FROM groupby ORDER BY v1;
static std::shared_ptr<QueryPlan> w6_034(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v1"}, device);
    std::vector<SortKey> sort_keys = {{"v1", SortOrder::ASCENDING}};
    auto ob = order_by(source, sort_keys, device);
    return query_plan(table_sink(ob));
}

// w6_035: SELECT v3 FROM groupby ORDER BY v3;
static std::shared_ptr<QueryPlan> w6_035(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "groupby", schema("groupby"), {"v3"}, device);
    std::vector<SortKey> sort_keys = {{"v3", SortOrder::ASCENDING}};
    auto ob = order_by(source, sort_keys, device);
    return query_plan(table_sink(ob));
}

// Dispatch
std::shared_ptr<QueryPlan> query_plan(const std::string& q,
                                       std::shared_ptr<Database>& db,
                                       DeviceType device) {
    if (q == "w1_001") return w1_001(db, device);
    if (q == "w1_002") return w1_002(db, device);
    if (q == "w1_003") return w1_003(db, device);
    if (q == "w1_004") return w1_004(db, device);
    if (q == "w1_005") return w1_005(db, device);
    if (q == "w1_006") return w1_006(db, device);
    if (q == "w1_007") return w1_007(db, device);
    if (q == "w2_008") return w2_008(db, device);
    if (q == "w2_009") return w2_009(db, device);
    if (q == "w2_010") return w2_010(db, device);
    if (q == "w2_011") return w2_011(db, device);
    if (q == "w2_012") return w2_012(db, device);
    if (q == "w2_013") return w2_013(db, device);
    if (q == "w2_014") return w2_014(db, device);
    if (q == "w3_016") return w3_016(db, device);
    if (q == "w3_017") return w3_017(db, device);
    if (q == "w3_018") return w3_018(db, device);
    if (q == "w3_019") return w3_019(db, device);
    if (q == "w3_020") return w3_020(db, device);
    if (q == "w3_021") return w3_021(db, device);
    if (q == "w3_023") return w3_023(db, device);
    if (q == "w4_027") return w4_027(db, device);
    if (q == "w4_028") return w4_028(db, device);
    if (q == "w4_029") return w4_029(db, device);
    if (q == "w4_030") return w4_030(db, device);
    if (q == "w4_031") return w4_031(db, device);
    if (q == "w4_032") return w4_032(db, device);
    if (q == "w4_033") return w4_033(db, device);
    if (q == "w6_015") return w6_015(db, device);
    if (q == "w6_022") return w6_022(db, device);
    if (q == "w6_024") return w6_024(db, device);
    if (q == "w6_025") return w6_025(db, device);
    if (q == "w6_026") return w6_026(db, device);
    if (q == "w6_034") return w6_034(db, device);
    if (q == "w6_035") return w6_035(db, device);
    throw std::runtime_error("Unknown microbench_h2o query: " + q);
}

}  // namespace maximus::microbench_h2o
