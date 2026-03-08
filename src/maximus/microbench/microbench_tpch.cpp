#include <maximus/frontend/expressions.hpp>
#include <maximus/frontend/query_plan_api.hpp>
#include <maximus/microbench/microbench_tpch.hpp>
#include <maximus/types/expression.hpp>

namespace maximus::microbench_tpch {

using maximus::tpch::schema;
namespace cp = ::arrow::compute;

std::vector<std::string> table_names() {
    return maximus::tpch::table_names();
}
std::vector<std::shared_ptr<Schema>> schemas() {
    return maximus::tpch::schemas();
}

// w1_002: SELECT COUNT(*), SUM(o_totalprice) FROM orders;
static std::shared_ptr<QueryPlan> w1_002(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "orders", schema("orders"), {"o_totalprice"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "o_totalprice", "count"),
        aggregate("hash_sum", sum_defaults(), "o_totalprice", "sum_price")
    };
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_004: SELECT COUNT(*) FROM lineitem;
static std::shared_ptr<QueryPlan> w1_004(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "l_orderkey", "count")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_005: SELECT SUM(l_quantity) FROM lineitem;
static std::shared_ptr<QueryPlan> w1_005(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_quantity"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_006: SELECT AVG(l_extendedprice) FROM lineitem;
static std::shared_ptr<QueryPlan> w1_006(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_extendedprice"}, device);
    auto aggs = {aggregate("hash_mean", "l_extendedprice", "avg_price")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_007: SELECT MIN(l_discount), MAX(l_discount) FROM lineitem;
static std::shared_ptr<QueryPlan> w1_007(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_discount"}, device);
    auto aggs = {aggregate("min", "l_discount", "min_disc"), aggregate("max", "l_discount", "max_disc")};
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_008: SELECT SUM(l_extendedprice * l_discount) FROM lineitem;
static std::shared_ptr<QueryPlan> w1_008(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_extendedprice", "l_discount"}, device);
    auto proj_exprs = {expr(cp::call("multiply", {cp::field_ref("l_extendedprice"), cp::field_ref("l_discount")}))};
    std::vector<std::string> proj_names = {"product"};
    auto proj = project(source, proj_exprs, proj_names, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "product", "sum_product")};
    auto gb = group_by(proj, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w1_011: SELECT SUM(l_quantity), SUM(l_extendedprice), AVG(l_discount), AVG(l_tax) FROM lineitem;
static std::shared_ptr<QueryPlan> w1_011(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_quantity", "l_extendedprice", "l_discount", "l_tax"}, device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty"),
        aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price"),
        aggregate("hash_mean", "l_discount", "avg_disc"),
        aggregate("hash_mean", "l_tax", "avg_tax")
    };
    auto gb = group_by(source, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_003: SELECT COUNT(*), SUM(o_totalprice) FROM orders WHERE o_totalprice > 200000;
static std::shared_ptr<QueryPlan> w2_003(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "orders", schema("orders"), {"o_totalprice"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("o_totalprice"), ">", float64_literal(200000))), device);
    auto aggs = {
        aggregate("hash_count", count_all(), "o_totalprice", "count"),
        aggregate("hash_sum", sum_defaults(), "o_totalprice", "sum_price")
    };
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_012: SELECT COUNT(*) FROM lineitem WHERE l_quantity > 25;
static std::shared_ptr<QueryPlan> w2_012(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_quantity"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("l_quantity"), ">", float64_literal(25))), device);
    auto aggs = {aggregate("hash_count", count_all(), "l_quantity", "count")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_013: SELECT SUM(l_extendedprice) FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07;
static std::shared_ptr<QueryPlan> w2_013(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_extendedprice", "l_discount"}, device);
    auto flt = filter(source, expr(arrow_between(cp::field_ref("l_discount"), float64_literal(0.05), float64_literal(0.07))), device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_014: SELECT AVG(l_quantity) FROM lineitem WHERE l_shipdate >= '1995-01-01';
static std::shared_ptr<QueryPlan> w2_014(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_quantity", "l_shipdate"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("l_shipdate"), ">=", date_literal("1995-01-01"))), device);
    auto aggs = {aggregate("hash_mean", "l_quantity", "avg_qty")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_015: SELECT COUNT(*) FROM lineitem WHERE l_returnflag = 'R';
static std::shared_ptr<QueryPlan> w2_015(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_returnflag"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("l_returnflag"), "==", string_literal("R"))), device);
    auto aggs = {aggregate("hash_count", count_all(), "l_returnflag", "count")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_016: SELECT SUM(l_extendedprice * l_discount) FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;
static std::shared_ptr<QueryPlan> w2_016(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_extendedprice", "l_discount", "l_quantity"}, device);
    auto flt = filter(source, expr(arrow_all({
        arrow_between(cp::field_ref("l_discount"), float64_literal(0.05), float64_literal(0.07)),
        arrow_expr(cp::field_ref("l_quantity"), "<", float64_literal(24))
    })), device);
    auto proj_exprs = {expr(cp::call("multiply", {cp::field_ref("l_extendedprice"), cp::field_ref("l_discount")}))};
    std::vector<std::string> proj_names = {"product"};
    auto proj = project(flt, proj_exprs, proj_names, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "product", "sum_product")};
    auto gb = group_by(proj, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w2_017: SELECT SUM(l_quantity) FROM lineitem WHERE l_extendedprice > 10000;
static std::shared_ptr<QueryPlan> w2_017(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_quantity", "l_extendedprice"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("l_extendedprice"), ">", float64_literal(10000))), device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty")};
    auto gb = group_by(flt, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_001: SELECT c_mktsegment, COUNT(*), AVG(c_acctbal) FROM customer GROUP BY c_mktsegment;
static std::shared_ptr<QueryPlan> w3_001(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "customer", schema("customer"), {"c_mktsegment", "c_acctbal"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "c_mktsegment", "count"),
        aggregate("hash_mean", "c_acctbal", "avg_acctbal")
    };
    auto gb = group_by(source, {"c_mktsegment"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_009: SELECT o_orderstatus, COUNT(*), SUM(o_totalprice) FROM orders GROUP BY o_orderstatus;
static std::shared_ptr<QueryPlan> w3_009(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "orders", schema("orders"), {"o_orderstatus", "o_totalprice"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "o_orderstatus", "count"),
        aggregate("hash_sum", sum_defaults(), "o_totalprice", "sum_price")
    };
    auto gb = group_by(source, {"o_orderstatus"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_010: SELECT o_orderpriority, COUNT(*) FROM orders GROUP BY o_orderpriority;
static std::shared_ptr<QueryPlan> w3_010(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "orders", schema("orders"), {"o_orderpriority"}, device);
    auto aggs = {aggregate("hash_count", count_all(), "o_orderpriority", "count")};
    auto gb = group_by(source, {"o_orderpriority"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_023: SELECT l_returnflag, COUNT(*), SUM(l_quantity) FROM lineitem GROUP BY l_returnflag;
static std::shared_ptr<QueryPlan> w3_023(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_returnflag", "l_quantity"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "l_returnflag", "count"),
        aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty")
    };
    auto gb = group_by(source, {"l_returnflag"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_024: SELECT l_linestatus, SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_linestatus;
static std::shared_ptr<QueryPlan> w3_024(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_linestatus", "l_extendedprice", "l_discount"}, device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price"),
        aggregate("hash_mean", "l_discount", "avg_disc")
    };
    auto gb = group_by(source, {"l_linestatus"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_025: SELECT l_shipmode, COUNT(*), SUM(l_extendedprice) FROM lineitem GROUP BY l_shipmode;
static std::shared_ptr<QueryPlan> w3_025(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_shipmode", "l_extendedprice"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "l_shipmode", "count"),
        aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")
    };
    auto gb = group_by(source, {"l_shipmode"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w3_028: SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity), AVG(l_extendedprice) FROM lineitem GROUP BY l_returnflag, l_linestatus;
static std::shared_ptr<QueryPlan> w3_028(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "l_returnflag", "count"),
        aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty"),
        aggregate("hash_mean", "l_extendedprice", "avg_price")
    };
    auto gb = group_by(source, {"l_returnflag", "l_linestatus"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_033: SELECT o_custkey, COUNT(*), SUM(o_totalprice) FROM orders GROUP BY o_custkey;
static std::shared_ptr<QueryPlan> w4_033(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "orders", schema("orders"), {"o_custkey", "o_totalprice"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "o_custkey", "count"),
        aggregate("hash_sum", sum_defaults(), "o_totalprice", "sum_price")
    };
    auto gb = group_by(source, {"o_custkey"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_052: SELECT l_partkey, SUM(l_quantity) FROM lineitem GROUP BY l_partkey;
static std::shared_ptr<QueryPlan> w4_052(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_partkey", "l_quantity"}, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty")};
    auto gb = group_by(source, {"l_partkey"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_053: SELECT l_suppkey, COUNT(*), SUM(l_extendedprice) FROM lineitem GROUP BY l_suppkey;
static std::shared_ptr<QueryPlan> w4_053(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_suppkey", "l_extendedprice"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "l_suppkey", "count"),
        aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")
    };
    auto gb = group_by(source, {"l_suppkey"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_054: SELECT l_orderkey, SUM(l_quantity), SUM(l_extendedprice) FROM lineitem GROUP BY l_orderkey;
static std::shared_ptr<QueryPlan> w4_054(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_quantity", "l_extendedprice"}, device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty"),
        aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")
    };
    auto gb = group_by(source, {"l_orderkey"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_055: SELECT l_shipdate, COUNT(*), SUM(l_quantity) FROM lineitem GROUP BY l_shipdate;
static std::shared_ptr<QueryPlan> w4_055(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_shipdate", "l_quantity"}, device);
    auto aggs = {
        aggregate("hash_count", count_all(), "l_shipdate", "count"),
        aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty")
    };
    auto gb = group_by(source, {"l_shipdate"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_057: SELECT l_partkey, l_shipmode, AVG(l_discount) FROM lineitem GROUP BY l_partkey, l_shipmode;
static std::shared_ptr<QueryPlan> w4_057(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_partkey", "l_shipmode", "l_discount"}, device);
    auto aggs = {aggregate("hash_mean", "l_discount", "avg_disc")};
    auto gb = group_by(source, {"l_partkey", "l_shipmode"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w4_059: SELECT l_suppkey, MIN(l_extendedprice), MAX(l_extendedprice), AVG(l_quantity) FROM lineitem GROUP BY l_suppkey;
static std::shared_ptr<QueryPlan> w4_059(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_suppkey", "l_extendedprice", "l_quantity"}, device);
    auto aggs = {
        aggregate("min", "l_extendedprice", "min_price"),
        aggregate("max", "l_extendedprice", "max_price"),
        aggregate("hash_mean", "l_quantity", "avg_qty")
    };
    auto gb = group_by(source, {"l_suppkey"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_029: SELECT c_mktsegment, COUNT(*), SUM(o_totalprice) FROM orders JOIN customer ON o_custkey = c_custkey GROUP BY c_mktsegment;
static std::shared_ptr<QueryPlan> w5a_029(std::shared_ptr<Database>& db, DeviceType device) {
    auto orders_src = table_source(db, "orders", schema("orders"), {"o_custkey", "o_totalprice"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey", "c_mktsegment"}, device);
    auto j = inner_join(orders_src, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto aggs = {
        aggregate("hash_count", count_all(), "c_mktsegment", "count"),
        aggregate("hash_sum", sum_defaults(), "o_totalprice", "sum_price")
    };
    auto gb = group_by(j, {"c_mktsegment"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_034: SELECT COUNT(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey;
static std::shared_ptr<QueryPlan> w5a_034(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey"}, device);
    auto j = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto aggs = {aggregate("hash_count", count_all(), "l_orderkey", "count")};
    auto gb = group_by(j, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_035: SELECT SUM(l_extendedprice) FROM lineitem JOIN orders ON l_orderkey = o_orderkey;
static std::shared_ptr<QueryPlan> w5a_035(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_extendedprice"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey"}, device);
    auto j = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")};
    auto gb = group_by(j, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_036: SELECT COUNT(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey WHERE o_orderstatus = 'F';
static std::shared_ptr<QueryPlan> w5a_036(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_orderstatus"}, device);
    auto ord_flt = filter(ord_src, expr(arrow_expr(cp::field_ref("o_orderstatus"), "==", string_literal("F"))), device);
    auto j = inner_join(li_src, ord_flt, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto aggs = {aggregate("hash_count", count_all(), "l_orderkey", "count")};
    auto gb = group_by(j, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_037: SELECT SUM(l_extendedprice) FROM lineitem JOIN part ON l_partkey = p_partkey WHERE p_size < 10;
static std::shared_ptr<QueryPlan> w5a_037(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_partkey", "l_extendedprice"}, device);
    auto part_src = table_source(db, "part", schema("part"), {"p_partkey", "p_size"}, device);
    auto part_flt = filter(part_src, expr(arrow_expr(cp::field_ref("p_size"), "<", int32_literal(10))), device);
    auto j = inner_join(li_src, part_flt, {"l_partkey"}, {"p_partkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")};
    auto gb = group_by(j, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_038: 3table orders-customer-nation
static std::shared_ptr<QueryPlan> w5a_038(std::shared_ptr<Database>& db, DeviceType device) {
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_custkey", "o_totalprice"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey", "c_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_name"}, device);
    auto j1 = inner_join(ord_src, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto j2 = inner_join(j1, nat_src, {"c_nationkey"}, {"n_nationkey"}, "", "", device);
    auto aggs = {
        aggregate("hash_count", count_all(), "n_name", "count"),
        aggregate("hash_sum", sum_defaults(), "o_totalprice", "sum_price")
    };
    auto gb = group_by(j2, {"n_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_048: SELECT o_orderstatus, SUM(l_quantity) FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY o_orderstatus;
static std::shared_ptr<QueryPlan> w5a_048(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_quantity"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_orderstatus"}, device);
    auto j = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty")};
    auto gb = group_by(j, {"o_orderstatus"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_049: SELECT s_nationkey, SUM(l_extendedprice) FROM lineitem JOIN supplier ON l_suppkey = s_suppkey GROUP BY s_nationkey;
static std::shared_ptr<QueryPlan> w5a_049(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_suppkey", "l_extendedprice"}, device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto j = inner_join(li_src, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")};
    auto gb = group_by(j, {"s_nationkey"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_050: 3table lineitem-orders-customer count
static std::shared_ptr<QueryPlan> w5a_050(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_custkey"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey"}, device);
    auto j1 = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto j2 = inner_join(j1, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto aggs = {aggregate("hash_count", count_all(), "l_orderkey", "count")};
    auto gb = group_by(j2, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5a_056: 3table group by segment
static std::shared_ptr<QueryPlan> w5a_056(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_extendedprice"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_custkey"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey", "c_mktsegment"}, device);
    auto j1 = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto j2 = inner_join(j1, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")};
    auto gb = group_by(j2, {"c_mktsegment"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_030: 5table filter on region
static std::shared_ptr<QueryPlan> w5b_030(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_extendedprice", "l_discount"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_custkey"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey", "c_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_regionkey"}, device);
    auto reg_src = table_source(db, "region", schema("region"), {"r_regionkey", "r_name"}, device);
    auto reg_flt = filter(reg_src, expr(arrow_expr(cp::field_ref("r_name"), "==", string_literal("ASIA"))), device);
    auto j1 = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto j2 = inner_join(j1, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto j3 = inner_join(j2, nat_src, {"c_nationkey"}, {"n_nationkey"}, "", "", device);
    auto j4 = inner_join(j3, reg_flt, {"n_regionkey"}, {"r_regionkey"}, "", "", device);
    auto proj_exprs = {expr(cp::call("multiply", {cp::field_ref("l_extendedprice"),
        cp::call("subtract", {cp::literal(1.0), cp::field_ref("l_discount")})}))};
    std::vector<std::string> proj_names = {"revenue"};
    auto proj = project(j4, proj_exprs, proj_names, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "revenue", "sum_revenue")};
    auto gb = group_by(proj, {}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_039-w5b_051: Complex multi-table joins - I'll implement the key ones
// w5b_039: 5table lineitem-orders-customer-nation-region group by r_name
static std::shared_ptr<QueryPlan> w5b_039(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_extendedprice"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_custkey"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey", "c_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_regionkey"}, device);
    auto reg_src = table_source(db, "region", schema("region"), {"r_regionkey", "r_name"}, device);
    auto j1 = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto j2 = inner_join(j1, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto j3 = inner_join(j2, nat_src, {"c_nationkey"}, {"n_nationkey"}, "", "", device);
    auto j4 = inner_join(j3, reg_src, {"n_regionkey"}, {"r_regionkey"}, "", "", device);
    auto aggs = {
        aggregate("hash_count", count_all(), "r_name", "count"),
        aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")
    };
    auto gb = group_by(j4, {"r_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_040: 5table lineitem-part-supplier-nation-region
static std::shared_ptr<QueryPlan> w5b_040(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_partkey", "l_suppkey", "l_extendedprice"}, device);
    auto part_src = table_source(db, "part", schema("part"), {"p_partkey"}, device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_regionkey"}, device);
    auto reg_src = table_source(db, "region", schema("region"), {"r_regionkey", "r_name"}, device);
    auto j1 = inner_join(li_src, part_src, {"l_partkey"}, {"p_partkey"}, "", "", device);
    auto j2 = inner_join(j1, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto j3 = inner_join(j2, nat_src, {"s_nationkey"}, {"n_nationkey"}, "", "", device);
    auto j4 = inner_join(j3, reg_src, {"n_regionkey"}, {"r_regionkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")};
    auto gb = group_by(j4, {"r_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_041: 5table filter on part
static std::shared_ptr<QueryPlan> w5b_041(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_partkey", "l_suppkey", "l_quantity"}, device);
    auto part_src = table_source(db, "part", schema("part"), {"p_partkey", "p_size"}, device);
    auto part_flt = filter(part_src, expr(arrow_expr(cp::field_ref("p_size"), "<", int32_literal(10))), device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_regionkey", "n_name"}, device);
    auto reg_src = table_source(db, "region", schema("region"), {"r_regionkey"}, device);
    auto j1 = inner_join(li_src, part_flt, {"l_partkey"}, {"p_partkey"}, "", "", device);
    auto j2 = inner_join(j1, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto j3 = inner_join(j2, nat_src, {"s_nationkey"}, {"n_nationkey"}, "", "", device);
    auto j4 = inner_join(j3, reg_src, {"n_regionkey"}, {"r_regionkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty")};
    auto gb = group_by(j4, {"n_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_042: 5table lineitem-orders-customer-supplier-nation
static std::shared_ptr<QueryPlan> w5b_042(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_suppkey", "l_extendedprice"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_custkey"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey"}, device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_name"}, device);
    auto j1 = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto j2 = inner_join(j1, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto j3 = inner_join(j2, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto j4 = inner_join(j3, nat_src, {"s_nationkey"}, {"n_nationkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")};
    auto gb = group_by(j4, {"n_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_043: 6table with date filter
static std::shared_ptr<QueryPlan> w5b_043(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_custkey", "o_orderdate"}, device);
    auto ord_flt = filter(ord_src, expr(arrow_all({
        arrow_expr(cp::field_ref("o_orderdate"), ">=", date_literal("1995-01-01")),
        arrow_expr(cp::field_ref("o_orderdate"), "<", date_literal("1996-01-01"))
    })), device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey", "c_nationkey"}, device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_regionkey"}, device);
    auto reg_src = table_source(db, "region", schema("region"), {"r_regionkey", "r_name"}, device);
    auto j1 = inner_join(li_src, ord_flt, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto j2 = inner_join(j1, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto j3 = inner_join(j2, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto j4 = inner_join(j3, nat_src, {"s_nationkey"}, {"n_nationkey"}, "", "", device);
    auto j5 = inner_join(j4, reg_src, {"n_regionkey"}, {"r_regionkey"}, "", "", device);
    auto proj_exprs = {
        expr(cp::field_ref("r_name")),
        expr(cp::call("multiply", {cp::field_ref("l_extendedprice"),
            cp::call("subtract", {cp::literal(1.0), cp::field_ref("l_discount")})}))
    };
    std::vector<std::string> proj_names = {"r_name", "revenue"};
    auto proj = project(j5, proj_exprs, proj_names, device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "revenue", "sum_revenue")};
    auto gb = group_by(proj, {"r_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_044: 5table lineitem-partsupp-part-supplier-nation
static std::shared_ptr<QueryPlan> w5b_044(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_partkey", "l_suppkey", "l_quantity"}, device);
    auto ps_src = table_source(db, "partsupp", schema("partsupp"), {"ps_partkey", "ps_suppkey", "ps_supplycost"}, device);
    auto part_src = table_source(db, "part", schema("part"), {"p_partkey"}, device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_name"}, device);
    auto j1 = inner_join(li_src, ps_src, {"l_partkey", "l_suppkey"}, {"ps_partkey", "ps_suppkey"}, "", "", device);
    auto j2 = inner_join(j1, part_src, {"l_partkey"}, {"p_partkey"}, "", "", device);
    auto j3 = inner_join(j2, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto j4 = inner_join(j3, nat_src, {"s_nationkey"}, {"n_nationkey"}, "", "", device);
    auto aggs = {
        aggregate("hash_sum", sum_defaults(), "l_quantity", "sum_qty"),
        aggregate("hash_sum", sum_defaults(), "ps_supplycost", "sum_cost")
    };
    auto gb = group_by(j4, {"n_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_045: 6table lineitem-partsupp-part-supplier-nation-region
static std::shared_ptr<QueryPlan> w5b_045(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_partkey", "l_suppkey", "l_extendedprice"}, device);
    auto ps_src = table_source(db, "partsupp", schema("partsupp"), {"ps_partkey", "ps_suppkey"}, device);
    auto part_src = table_source(db, "part", schema("part"), {"p_partkey"}, device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_regionkey"}, device);
    auto reg_src = table_source(db, "region", schema("region"), {"r_regionkey", "r_name"}, device);
    auto j1 = inner_join(li_src, ps_src, {"l_partkey", "l_suppkey"}, {"ps_partkey", "ps_suppkey"}, "", "", device);
    auto j2 = inner_join(j1, part_src, {"l_partkey"}, {"p_partkey"}, "", "", device);
    auto j3 = inner_join(j2, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto j4 = inner_join(j3, nat_src, {"s_nationkey"}, {"n_nationkey"}, "", "", device);
    auto j5 = inner_join(j4, reg_src, {"n_regionkey"}, {"r_regionkey"}, "", "", device);
    auto aggs = {
        aggregate("hash_count", count_all(), "r_name", "count"),
        aggregate("hash_mean", "l_extendedprice", "avg_price")
    };
    auto gb = group_by(j5, {"r_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_047: 6table lineitem-orders-customer-supplier-nation-region
static std::shared_ptr<QueryPlan> w5b_047(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_custkey"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey", "c_nationkey"}, device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_regionkey"}, device);
    auto reg_src = table_source(db, "region", schema("region"), {"r_regionkey", "r_name"}, device);
    auto j1 = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto j2 = inner_join(j1, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto j3 = inner_join(j2, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto j4 = inner_join(j3, nat_src, {"s_nationkey"}, {"n_nationkey"}, "", "", device);
    auto j5 = inner_join(j4, reg_src, {"n_regionkey"}, {"r_regionkey"}, "", "", device);
    auto aggs = {
        aggregate("hash_count", count_all(), "r_name", "count"),
        aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price"),
        aggregate("hash_mean", "l_discount", "avg_disc")
    };
    auto gb = group_by(j5, {"r_name"}, aggs, device);
    return query_plan(table_sink(gb));
}

// w5b_051: 5table with aggregation by n_name, c_mktsegment
static std::shared_ptr<QueryPlan> w5b_051(std::shared_ptr<Database>& db, DeviceType device) {
    auto li_src = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_suppkey", "l_extendedprice"}, device);
    auto ord_src = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_custkey"}, device);
    auto cust_src = table_source(db, "customer", schema("customer"), {"c_custkey", "c_mktsegment"}, device);
    auto sup_src = table_source(db, "supplier", schema("supplier"), {"s_suppkey", "s_nationkey"}, device);
    auto nat_src = table_source(db, "nation", schema("nation"), {"n_nationkey", "n_name"}, device);
    auto j1 = inner_join(li_src, ord_src, {"l_orderkey"}, {"o_orderkey"}, "", "", device);
    auto j2 = inner_join(j1, cust_src, {"o_custkey"}, {"c_custkey"}, "", "", device);
    auto j3 = inner_join(j2, sup_src, {"l_suppkey"}, {"s_suppkey"}, "", "", device);
    auto j4 = inner_join(j3, nat_src, {"s_nationkey"}, {"n_nationkey"}, "", "", device);
    auto aggs = {aggregate("hash_sum", sum_defaults(), "l_extendedprice", "sum_price")};
    auto gb = group_by(j4, {"n_name", "c_mktsegment"}, aggs, device);
    return query_plan(table_sink(gb));
}

// Sort queries
// w6_020: SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_totalprice DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_020(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "orders", schema("orders"), {"o_orderkey", "o_totalprice"}, device);
    std::vector<SortKey> sort_keys = {{"o_totalprice", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_021: SELECT l_orderkey, l_extendedprice FROM lineitem WHERE l_returnflag = 'R' ORDER BY l_extendedprice DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_021(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_extendedprice", "l_returnflag"}, device);
    auto flt = filter(source, expr(arrow_expr(cp::field_ref("l_returnflag"), "==", string_literal("R"))), device);
    auto proj_node = project(flt, {"l_orderkey", "l_extendedprice"}, device);
    std::vector<SortKey> sort_keys = {{"l_extendedprice", SortOrder::DESCENDING}};
    auto ob = order_by(proj_node, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_026: SELECT l_orderkey, l_extendedprice FROM lineitem ORDER BY l_extendedprice DESC LIMIT 10;
static std::shared_ptr<QueryPlan> w6_026(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_extendedprice"}, device);
    std::vector<SortKey> sort_keys = {{"l_extendedprice", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 10, 0, device);
    return query_plan(table_sink(lim));
}

// w6_031: SELECT l_orderkey, l_quantity FROM lineitem ORDER BY l_quantity DESC LIMIT 100;
static std::shared_ptr<QueryPlan> w6_031(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_quantity"}, device);
    std::vector<SortKey> sort_keys = {{"l_quantity", SortOrder::DESCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_032: SELECT l_orderkey, l_shipdate FROM lineitem ORDER BY l_shipdate LIMIT 100;
static std::shared_ptr<QueryPlan> w6_032(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_shipdate"}, device);
    std::vector<SortKey> sort_keys = {{"l_shipdate", SortOrder::ASCENDING}};
    auto ob = order_by(source, sort_keys, device);
    auto lim = limit(ob, 100, 0, device);
    return query_plan(table_sink(lim));
}

// w6_046: SELECT l_orderkey, l_extendedprice * (1 - l_discount) AS net_price FROM lineitem ORDER BY net_price DESC LIMIT 1000;
static std::shared_ptr<QueryPlan> w6_046(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_orderkey", "l_extendedprice", "l_discount"}, device);
    auto proj_exprs = {
        expr(cp::field_ref("l_orderkey")),
        expr(cp::call("multiply", {cp::field_ref("l_extendedprice"),
            cp::call("subtract", {cp::literal(1.0), cp::field_ref("l_discount")})}))
    };
    std::vector<std::string> proj_names = {"l_orderkey", "net_price"};
    auto proj = project(source, proj_exprs, proj_names, device);
    std::vector<SortKey> sort_keys = {{"net_price", SortOrder::DESCENDING}};
    auto ob = order_by(proj, sort_keys, device);
    auto lim = limit(ob, 1000, 0, device);
    return query_plan(table_sink(lim));
}

// w6_060: SELECT l_extendedprice FROM lineitem ORDER BY l_extendedprice;
static std::shared_ptr<QueryPlan> w6_060(std::shared_ptr<Database>& db, DeviceType device) {
    auto source = table_source(db, "lineitem", schema("lineitem"), {"l_extendedprice"}, device);
    std::vector<SortKey> sort_keys = {{"l_extendedprice", SortOrder::ASCENDING}};
    auto ob = order_by(source, sort_keys, device);
    return query_plan(table_sink(ob));
}

// Note: w2_018, w2_019, w3_027, w4_058, w6_022 reference ClickBench 'hits' table
// These are cross-benchmark queries and will be skipped for TPC-H microbench
// They should be run under the clickbench microbench instead.

// Dispatch
std::shared_ptr<QueryPlan> query_plan(const std::string& q,
                                       std::shared_ptr<Database>& db,
                                       DeviceType device) {
    if (q == "w1_002") return w1_002(db, device);
    if (q == "w1_004") return w1_004(db, device);
    if (q == "w1_005") return w1_005(db, device);
    if (q == "w1_006") return w1_006(db, device);
    if (q == "w1_007") return w1_007(db, device);
    if (q == "w1_008") return w1_008(db, device);
    if (q == "w1_011") return w1_011(db, device);
    if (q == "w2_003") return w2_003(db, device);
    if (q == "w2_012") return w2_012(db, device);
    if (q == "w2_013") return w2_013(db, device);
    if (q == "w2_014") return w2_014(db, device);
    if (q == "w2_015") return w2_015(db, device);
    if (q == "w2_016") return w2_016(db, device);
    if (q == "w2_017") return w2_017(db, device);
    if (q == "w3_001") return w3_001(db, device);
    if (q == "w3_009") return w3_009(db, device);
    if (q == "w3_010") return w3_010(db, device);
    if (q == "w3_023") return w3_023(db, device);
    if (q == "w3_024") return w3_024(db, device);
    if (q == "w3_025") return w3_025(db, device);
    if (q == "w3_028") return w3_028(db, device);
    if (q == "w4_033") return w4_033(db, device);
    if (q == "w4_052") return w4_052(db, device);
    if (q == "w4_053") return w4_053(db, device);
    if (q == "w4_054") return w4_054(db, device);
    if (q == "w4_055") return w4_055(db, device);
    if (q == "w4_057") return w4_057(db, device);
    if (q == "w4_059") return w4_059(db, device);
    if (q == "w5a_029") return w5a_029(db, device);
    if (q == "w5a_034") return w5a_034(db, device);
    if (q == "w5a_035") return w5a_035(db, device);
    if (q == "w5a_036") return w5a_036(db, device);
    if (q == "w5a_037") return w5a_037(db, device);
    if (q == "w5a_038") return w5a_038(db, device);
    if (q == "w5a_048") return w5a_048(db, device);
    if (q == "w5a_049") return w5a_049(db, device);
    if (q == "w5a_050") return w5a_050(db, device);
    if (q == "w5a_056") return w5a_056(db, device);
    if (q == "w5b_030") return w5b_030(db, device);
    if (q == "w5b_039") return w5b_039(db, device);
    if (q == "w5b_040") return w5b_040(db, device);
    if (q == "w5b_041") return w5b_041(db, device);
    if (q == "w5b_042") return w5b_042(db, device);
    if (q == "w5b_043") return w5b_043(db, device);
    if (q == "w5b_044") return w5b_044(db, device);
    if (q == "w5b_045") return w5b_045(db, device);
    if (q == "w5b_047") return w5b_047(db, device);
    if (q == "w5b_051") return w5b_051(db, device);
    if (q == "w6_020") return w6_020(db, device);
    if (q == "w6_021") return w6_021(db, device);
    if (q == "w6_026") return w6_026(db, device);
    if (q == "w6_031") return w6_031(db, device);
    if (q == "w6_032") return w6_032(db, device);
    if (q == "w6_046") return w6_046(db, device);
    if (q == "w6_060") return w6_060(db, device);
    throw std::runtime_error("Unknown microbench_tpch query: " + q);
}

}  // namespace maximus::microbench_tpch
