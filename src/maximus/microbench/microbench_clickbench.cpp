#include <maximus/frontend/expressions.hpp>
#include <maximus/frontend/query_plan_api.hpp>
#include <maximus/microbench/microbench_clickbench.hpp>
#include <maximus/types/expression.hpp>

namespace maximus::microbench_clickbench {

using maximus::clickbench::schema;
namespace cp = ::arrow::compute;

static const std::string TN = "t";

std::vector<std::string> table_names() { return {"t"}; }
std::vector<std::shared_ptr<Schema>> schemas() {
    return {clickbench::schema("t")};
}

// ============================================================
// W1: Full-table scan + aggregation (no filter, no group-by keys)
// ============================================================

// w1_001: SELECT COUNT(*) FROM hits;
static std::shared_ptr<QueryPlan> w1_001(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"WatchID"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "WatchID", "count")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_002: SELECT SUM(AdvEngineID) FROM hits;
static std::shared_ptr<QueryPlan> w1_002(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"AdvEngineID"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "AdvEngineID", "sum_advengineid")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_003: SELECT AVG(ResolutionWidth) FROM hits;
static std::shared_ptr<QueryPlan> w1_003(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"ResolutionWidth"}, device);
    auto aggs = {aggregate("hash_mean", "ResolutionWidth", "avg_reswidth")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_004: SELECT MIN(EventTime), MAX(EventTime) FROM hits;
static std::shared_ptr<QueryPlan> w1_004(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"EventTime"}, device);
    auto aggs = {aggregate("min", "EventTime", "min_eventtime"), aggregate("max", "EventTime", "max_eventtime")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_005: SELECT COUNT(*), SUM(GoodEvent) FROM hits;
static std::shared_ptr<QueryPlan> w1_005(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"GoodEvent"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "GoodEvent", "count"),
        aggregate("hash_sum", sum_defaults(), "GoodEvent", "sum_goodevent")
    };
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_006: SELECT SUM(ResolutionWidth), AVG(ResolutionHeight), MIN(ClientIP), MAX(UserID) FROM hits;
static std::shared_ptr<QueryPlan> w1_006(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN),
        {"ResolutionWidth", "ResolutionHeight", "ClientIP", "UserID"}, device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "ResolutionWidth", "sum_reswidth"),
        aggregate("hash_mean", "ResolutionHeight", "avg_resheight"),
        aggregate("min", "ClientIP", "min_clientip"),
        aggregate("max", "UserID", "max_userid")
    };
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// ============================================================
// W2: Filter queries
// ============================================================

// w2_007: SELECT COUNT(*) FROM hits WHERE AdvEngineID > 0;
static std::shared_ptr<QueryPlan> w2_007(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"AdvEngineID"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("AdvEngineID"), ">", int16_literal(0))), device);
    auto aggs = {aggregate("hash_count", count_all(), "AdvEngineID", "count")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_008: SELECT COUNT(*) FROM hits WHERE EventDate >= '2013-07-15' AND EventDate < '2013-08-01';
static std::shared_ptr<QueryPlan> w2_008(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"EventDate"}, device);
    auto ge_filter = arrow_expr(cp::field_ref("EventDate"), ">=", timestamp_nano_literal("2013-07-15"));
    auto lt_filter = arrow_expr(cp::field_ref("EventDate"), "<", timestamp_nano_literal("2013-08-01"));
    auto combined = cp::and_(ge_filter, lt_filter);
    auto flt = filter(source, expr(combined), device);
    auto aggs = {aggregate("hash_count", count_all(), "EventDate", "count")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_009: SELECT AVG(ResolutionWidth) FROM hits WHERE GoodEvent = 1;
static std::shared_ptr<QueryPlan> w2_009(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"GoodEvent", "ResolutionWidth"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("GoodEvent"), "==", int16_literal(1))), device);
    auto aggs = {aggregate("hash_mean", "ResolutionWidth", "avg_reswidth")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_010: SELECT COUNT(*) FROM hits WHERE UserID != 0;
static std::shared_ptr<QueryPlan> w2_010(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"UserID"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("UserID"), "!=", int64_literal(0))), device);
    auto aggs = {aggregate("hash_count", count_all(), "UserID", "count")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_018 (cross-benchmark from tpch): SELECT SUM(ResolutionWidth) FROM hits WHERE RegionID = 229;
static std::shared_ptr<QueryPlan> w2_018(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"RegionID", "ResolutionWidth"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("RegionID"), "==", int32_literal(229))), device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "ResolutionWidth", "sum_reswidth")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_019 (cross-benchmark from tpch): SELECT SUM(GoodEvent) FROM hits WHERE CounterID > 10000 AND RegionID > 100;
static std::shared_ptr<QueryPlan> w2_019(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"CounterID", "RegionID", "GoodEvent"}, device);
    auto f1 = arrow_expr(cp::field_ref("CounterID"), ">", int32_literal(10000));
    auto f2 = arrow_expr(cp::field_ref("RegionID"), ">", int32_literal(100));
    auto combined = cp::and_(f1, f2);
    auto flt = filter(source, expr(combined), device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "GoodEvent", "sum_goodevent")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// ============================================================
// W3: Low-cardinality group-by
// ============================================================

// w3_011: SELECT GoodEvent, COUNT(*) FROM hits GROUP BY GoodEvent;
static std::shared_ptr<QueryPlan> w3_011(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"GoodEvent"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "GoodEvent", "count")};
    auto gb = group_by(source, {"GoodEvent"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_012: SELECT AdvEngineID, COUNT(*) FROM hits GROUP BY AdvEngineID;
static std::shared_ptr<QueryPlan> w3_012(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"AdvEngineID"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "AdvEngineID", "count")};
    auto gb = group_by(source, {"AdvEngineID"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_013: SELECT OS, COUNT(*) FROM hits GROUP BY OS;
static std::shared_ptr<QueryPlan> w3_013(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"OS"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "OS", "count")};
    auto gb = group_by(source, {"OS"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_014: SELECT ResolutionDepth, COUNT(*), AVG(ResolutionWidth) FROM hits GROUP BY ResolutionDepth;
static std::shared_ptr<QueryPlan> w3_014(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"ResolutionDepth", "ResolutionWidth"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "ResolutionDepth", "count"),
        aggregate("hash_mean", "ResolutionWidth", "avg_reswidth")
    };
    auto gb = group_by(source, {"ResolutionDepth"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_015: SELECT TraficSourceID, COUNT(*), SUM(GoodEvent) FROM hits GROUP BY TraficSourceID;
static std::shared_ptr<QueryPlan> w3_015(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"TraficSourceID", "GoodEvent"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "TraficSourceID", "count"),
        aggregate("hash_sum", sum_defaults(), "GoodEvent", "sum_goodevent")
    };
    auto gb = group_by(source, {"TraficSourceID"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_027 (cross-benchmark from tpch): SELECT RegionID, COUNT(*) FROM hits GROUP BY RegionID;
static std::shared_ptr<QueryPlan> w3_027(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"RegionID"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "RegionID", "count")};
    auto gb = group_by(source, {"RegionID"}, aggs, device);
    return query_plan(table_sink(gb));
}

// ============================================================
// W4: High-cardinality group-by
// ============================================================

// w4_021: SELECT CounterID, COUNT(*) FROM hits GROUP BY CounterID;
static std::shared_ptr<QueryPlan> w4_021(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"CounterID"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "CounterID", "count")};
    auto gb = group_by(source, {"CounterID"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_022: SELECT UserID, COUNT(*) FROM hits GROUP BY UserID;
static std::shared_ptr<QueryPlan> w4_022(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"UserID"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "UserID", "count")};
    auto gb = group_by(source, {"UserID"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_023: SELECT URLHash, COUNT(*) FROM hits GROUP BY URLHash;
static std::shared_ptr<QueryPlan> w4_023(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"URLHash"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "URLHash", "count")};
    auto gb = group_by(source, {"URLHash"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_024: SELECT RefererHash, COUNT(*), SUM(GoodEvent) FROM hits GROUP BY RefererHash;
static std::shared_ptr<QueryPlan> w4_024(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"RefererHash", "GoodEvent"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "RefererHash", "count"),
        aggregate("hash_sum", sum_defaults(), "GoodEvent", "sum_goodevent")
    };
    auto gb = group_by(source, {"RefererHash"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_025: SELECT EventDate, CounterID, COUNT(*) FROM hits GROUP BY EventDate, CounterID;
static std::shared_ptr<QueryPlan> w4_025(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"EventDate", "CounterID"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "CounterID", "count")};
    auto gb = group_by(source, {"EventDate", "CounterID"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_058 (cross-benchmark from tpch): SELECT CounterID, RegionID, COUNT(*) FROM hits GROUP BY CounterID, RegionID;
static std::shared_ptr<QueryPlan> w4_058(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"CounterID", "RegionID"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "CounterID", "count")};
    auto gb = group_by(source, {"CounterID", "RegionID"}, aggs, device);
    return query_plan(table_sink(gb));
}

// ============================================================
// W6: Sort / Order-by / Limit
// ============================================================

// w6_016: SELECT WatchID, EventTime FROM hits ORDER BY EventTime LIMIT 100;
static std::shared_ptr<QueryPlan> w6_016(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"WatchID", "EventTime"}, device);
    std::vector<SortKey> sort_keys = {{"EventTime", SortOrder::ASCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_017: SELECT WatchID FROM hits ORDER BY WatchID DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_017(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"WatchID"}, device);
    std::vector<SortKey> sort_keys = {{"WatchID", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_018: SELECT CounterID, EventDate, COUNT(*) AS cnt FROM hits GROUP BY CounterID, EventDate ORDER BY cnt DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_018(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"CounterID", "EventDate"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "CounterID", "cnt")};
    auto gb = group_by(source, {"CounterID", "EventDate"}, aggs, device);
    std::vector<SortKey> sort_keys = {{"cnt", SortOrder::DESCENDING}};
    auto ob = order_by(gb, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_019: SELECT WatchID, ResolutionWidth FROM hits ORDER BY ResolutionWidth DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_019(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"WatchID", "ResolutionWidth"}, device);
    std::vector<SortKey> sort_keys = {{"ResolutionWidth", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_020: SELECT UserID, CounterID FROM hits ORDER BY UserID DESC LIMIT 1000;
static std::shared_ptr<QueryPlan> w6_020(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"UserID", "CounterID"}, device);
    std::vector<SortKey> sort_keys = {{"UserID", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 1000, 0, device);
    return query_plan(table_sink(lim));
}

// w6_022 (cross-benchmark from tpch): SELECT EventTime, CounterID FROM hits WHERE RegionID = 229 ORDER BY EventTime LIMIT 100;
static std::shared_ptr<QueryPlan> w6_022(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, TN, schema(TN), {"EventTime", "CounterID", "RegionID"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("RegionID"), "==", int32_literal(229))), device);
    auto proj = project(flt, {"EventTime", "CounterID"}, device);
    std::vector<SortKey> sort_keys = {{"EventTime", SortOrder::ASCENDING}};
    auto ob = order_by(proj, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// ============================================================
// Dispatch
// ============================================================

std::shared_ptr<QueryPlan> query_plan(const std::string& q,
                                       std::shared_ptr<Database>& db,
                                       DeviceType device) {
    if (q == "w1_001") return w1_001(db, device);
    if (q == "w1_002") return w1_002(db, device);
    if (q == "w1_003") return w1_003(db, device);
    if (q == "w1_004") return w1_004(db, device);
    if (q == "w1_005") return w1_005(db, device);
    if (q == "w1_006") return w1_006(db, device);
    if (q == "w2_007") return w2_007(db, device);
    if (q == "w2_008") return w2_008(db, device);
    if (q == "w2_009") return w2_009(db, device);
    if (q == "w2_010") return w2_010(db, device);
    if (q == "w2_018") return w2_018(db, device);
    if (q == "w2_019") return w2_019(db, device);
    if (q == "w3_011") return w3_011(db, device);
    if (q == "w3_012") return w3_012(db, device);
    if (q == "w3_013") return w3_013(db, device);
    if (q == "w3_014") return w3_014(db, device);
    if (q == "w3_015") return w3_015(db, device);
    if (q == "w3_027") return w3_027(db, device);
    if (q == "w4_021") return w4_021(db, device);
    if (q == "w4_022") return w4_022(db, device);
    if (q == "w4_023") return w4_023(db, device);
    if (q == "w4_024") return w4_024(db, device);
    if (q == "w4_025") return w4_025(db, device);
    if (q == "w4_058") return w4_058(db, device);
    if (q == "w6_016") return w6_016(db, device);
    if (q == "w6_017") return w6_017(db, device);
    if (q == "w6_018") return w6_018(db, device);
    if (q == "w6_019") return w6_019(db, device);
    if (q == "w6_020") return w6_020(db, device);
    if (q == "w6_022") return w6_022(db, device);
    throw std::runtime_error("Unknown microbench_clickbench query: " + q);
}

}  // namespace maximus::microbench_clickbench
