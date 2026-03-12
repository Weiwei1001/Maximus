#!/usr/bin/env python3
"""
Generate Sirius GPU query SQL files for TPC-H, H2O, and ClickBench benchmarks.

Each SQL file contains:
  call gpu_buffer_init("<buffer_size>", "<pinned_size>");
  call gpu_processing("<SQL_QUERY>");

Usage:
    python generate_sirius_sql.py --output-dir /workspace

Output directories:
    <output-dir>/tpch_sql/queries/1/   (q01.sql - q22.sql)
    <output-dir>/h2o_sql/queries/1/    (q1.sql - q10.sql)
    <output-dir>/click_sql/queries/1/  (q0.sql - q42.sql)
"""
from __future__ import annotations

import argparse
import os

# ── Buffer init sizes ─────────────────────────────────────────────────────────
TPCH_BUFFER = 'call gpu_buffer_init("10 GB", "10 GB");'
H2O_BUFFER = 'call gpu_buffer_init("20 GB", "10 GB");'
CLICK_BUFFER = 'call gpu_buffer_init("10 GB", "10 GB");'

# ── TPC-H Queries (22 standard queries) ──────────────────────────────────────
# Each entry is (filename, [list of gpu_processing calls])
# Most queries have a single gpu_processing call; q11 has two (threshold + main).
TPCH_QUERIES = [
    ("q1.sql", [
        'call gpu_processing("SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST(\'1998-09-02\' AS date) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;");',
    ]),
    ("q2.sql", [
        'call gpu_processing("SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE \'%BRASS\' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = \'EUROPE\' AND ps_supplycost = ( SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = \'EUROPE\') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;");',
    ]),
    ("q3.sql", [
        'call gpu_processing("SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = \'BUILDING\' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < CAST(\'1995-03-15\' AS date) AND l_shipdate > CAST(\'1995-03-15\' AS date) GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;");',
    ]),
    ("q4.sql", [
        'call gpu_processing("SELECT o_orderpriority, count(*) AS order_count FROM orders WHERE o_orderdate >= CAST(\'1993-07-01\' AS date) AND o_orderdate < CAST(\'1993-10-01\' AS date) AND EXISTS ( SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority;");',
    ]),
    ("q5.sql", [
        'call gpu_processing("SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = \'ASIA\' AND o_orderdate >= CAST(\'1994-01-01\' AS date) AND o_orderdate < CAST(\'1995-01-01\' AS date) GROUP BY n_name ORDER BY revenue DESC;");',
    ]),
    ("q6.sql", [
        'call gpu_processing("SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST(\'1994-01-01\' AS date) AND l_shipdate < CAST(\'1995-01-01\' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;");',
    ]),
    ("q7.sql", [
        'call gpu_processing("SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM ( SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = \'FRANCE\' AND n2.n_name = \'GERMANY\') OR (n1.n_name = \'GERMANY\' AND n2.n_name = \'FRANCE\')) AND l_shipdate BETWEEN CAST(\'1995-01-01\' AS date) AND CAST(\'1996-12-31\' AS date)) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year;");',
    ]),
    ("q8.sql", [
        'call gpu_processing("SELECT o_year, sum( CASE WHEN nation = \'BRAZIL\' THEN volume ELSE 0 END) / sum(volume) AS mkt_share FROM ( SELECT extract(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = \'AMERICA\' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN CAST(\'1995-01-01\' AS date) AND CAST(\'1996-12-31\' AS date) AND p_type = \'ECONOMY ANODIZED STEEL\') AS all_nations GROUP BY o_year ORDER BY o_year;");',
    ]),
    ("q9.sql", [
        'call gpu_processing("SELECT nation, o_year, sum(amount) AS sum_profit FROM ( SELECT n_name AS nation, extract(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE \'%green%\') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC;");',
    ]),
    ("q10.sql", [
        'call gpu_processing("SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= CAST(\'1993-10-01\' AS date) AND o_orderdate < CAST(\'1994-01-01\' AS date) AND l_returnflag = \'R\' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20;");',
    ]),
    ("q11.sql", [
        'call gpu_processing("SELECT sum(CAST(ps_supplycost AS DOUBLE) * CAST(ps_availqty AS DOUBLE)) * 0.0001000000 AS thresh FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = \'GERMANY\'");',
        'call gpu_processing("SELECT ps_partkey, sum(CAST(ps_supplycost AS DOUBLE) * CAST(ps_availqty AS DOUBLE)) AS value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = \'GERMANY\' GROUP BY ps_partkey HAVING sum(CAST(ps_supplycost AS DOUBLE) * CAST(ps_availqty AS DOUBLE)) > 62992824.87524 ORDER BY value DESC");',
    ]),
    ("q12.sql", [
        'call gpu_processing("SELECT l_shipmode, sum( CASE WHEN o_orderpriority = \'1-URGENT\' OR o_orderpriority = \'2-HIGH\' THEN 1 ELSE 0 END) AS high_line_count, sum( CASE WHEN o_orderpriority <> \'1-URGENT\' AND o_orderpriority <> \'2-HIGH\' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN (\'MAIL\', \'SHIP\') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= CAST(\'1994-01-01\' AS date) AND l_receiptdate < CAST(\'1995-01-01\' AS date) GROUP BY l_shipmode ORDER BY l_shipmode;");',
    ]),
    ("q13.sql", [
        'call gpu_processing("SELECT c_count, count(*) AS custdist FROM ( SELECT c_custkey, count(o_orderkey) FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE \'%special%requests%\' GROUP BY c_custkey) AS c_orders (c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC;");',
    ]),
    ("q14.sql", [
        "call gpu_processing(\"SELECT 100.00 * sum( CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01' AND l_shipdate < CAST('1995-10-01' AS date);\");",
    ]),
    ("q15.sql", [
        'call gpu_processing("WITH revenue AS ( SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= CAST(\'1996-01-01\' AS date) AND l_shipdate < CAST(\'1996-04-01\' AS date) GROUP BY supplier_no ) SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue WHERE s_suppkey = supplier_no AND total_revenue = ( SELECT max(total_revenue) FROM revenue) ORDER BY s_suppkey;");',
    ]),
    ("q16.sql", [
        'call gpu_processing("SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> \'Brand#45\' AND p_type NOT LIKE \'MEDIUM POLISHED%\' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) AND ps_suppkey NOT IN ( SELECT s_suppkey FROM supplier WHERE s_comment LIKE \'%Customer%Complaints%\') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;");',
    ]),
    ("q17.sql", [
        'call gpu_processing("SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = \'Brand#23\' AND p_container = \'MED BOX\' AND l_quantity < ( SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey);");',
    ]),
    ("q18.sql", [
        'call gpu_processing("SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey IN ( SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;");',
    ]),
    ("q19.sql", [
        'call gpu_processing("SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brand = \'Brand#12\' AND p_container IN (\'SM CASE\', \'SM BOX\', \'SM PACK\', \'SM PKG\') AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN (\'AIR\', \'AIR REG\') AND l_shipinstruct = \'DELIVER IN PERSON\') OR (p_partkey = l_partkey AND p_brand = \'Brand#23\' AND p_container IN (\'MED BAG\', \'MED BOX\', \'MED PKG\', \'MED PACK\') AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN (\'AIR\', \'AIR REG\') AND l_shipinstruct = \'DELIVER IN PERSON\') OR (p_partkey = l_partkey AND p_brand = \'Brand#34\' AND p_container IN (\'LG CASE\', \'LG BOX\', \'LG PACK\', \'LG PKG\') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN (\'AIR\', \'AIR REG\') AND l_shipinstruct = \'DELIVER IN PERSON\');");',
    ]),
    ("q20.sql", [
        'call gpu_processing("SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey IN ( SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN ( SELECT p_partkey FROM part WHERE p_name LIKE \'forest%\') AND ps_availqty > ( SELECT 0.5 * sum(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= CAST(\'1994-01-01\' AS date) AND l_shipdate < CAST(\'1995-01-01\' AS date))) AND s_nationkey = n_nationkey AND n_name = \'CANADA\' ORDER BY s_name;");',
    ]),
    ("q21.sql", [
        'call gpu_processing("SELECT s_name, count(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = \'F\' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS ( SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS ( SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = \'SAUDI ARABIA\' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;");',
    ]),
    ("q22.sql", [
        'call gpu_processing("SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal FROM ( SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal FROM customer WHERE substring(c_phone FROM 1 FOR 2) IN (\'13\', \'31\', \'23\', \'29\', \'30\', \'18\', \'17\') AND c_acctbal > ( SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN (\'13\', \'31\', \'23\', \'29\', \'30\', \'18\', \'17\')) AND NOT EXISTS ( SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode;");',
    ]),
]

# ── H2O Queries (10 group-by queries) ────────────────────────────────────────
H2O_QUERIES = [
    ("q1.sql", [
        'call gpu_processing("SELECT id1, sum(v1) AS v1_sum FROM groupby GROUP BY id1 ORDER BY v1_sum;");',
    ]),
    ("q2.sql", [
        'call gpu_processing("SELECT id1, id2, sum(v1) AS v1 FROM groupby GROUP BY id1, id2;");',
    ]),
    ("q3.sql", [
        'call gpu_processing("SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM groupby GROUP BY id3;");',
    ]),
    ("q4.sql", [
        'call gpu_processing("SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM groupby GROUP BY id4;");',
    ]),
    ("q5.sql", [
        'call gpu_processing("SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM groupby GROUP BY id6;");',
    ]),
    ("q6.sql", [
        'call gpu_processing("SELECT id4, id5, avg(v3) AS median_v3, avg(v3*v3)-avg(v3)*avg(v3) AS var_v3 FROM groupby GROUP BY id4, id5;");',
    ]),
    ("q7.sql", [
        'call gpu_processing("SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM groupby GROUP BY id3;");',
    ]),
    ("q8.sql", [
        'call gpu_processing("SELECT id6, MAX(v3) AS max_v3, MIN(v3) AS min_v3, SUM(v3) AS sum_v3, COUNT(*) AS cnt FROM groupby WHERE v3 IS NOT NULL GROUP BY id6;");',
    ]),
    ("q9.sql", [
        'call gpu_processing("SELECT id2, id4, CASE WHEN (avg(CAST(v1 AS DOUBLE)*CAST(v1 AS DOUBLE))-avg(CAST(v1 AS DOUBLE))*avg(CAST(v1 AS DOUBLE))) * (avg(CAST(v2 AS DOUBLE)*CAST(v2 AS DOUBLE))-avg(CAST(v2 AS DOUBLE))*avg(CAST(v2 AS DOUBLE))) = 0 THEN 0 ELSE (avg(CAST(v1 AS DOUBLE)*CAST(v2 AS DOUBLE))-avg(CAST(v1 AS DOUBLE))*avg(CAST(v2 AS DOUBLE))) * (avg(CAST(v1 AS DOUBLE)*CAST(v2 AS DOUBLE))-avg(CAST(v1 AS DOUBLE))*avg(CAST(v2 AS DOUBLE))) / ((avg(CAST(v1 AS DOUBLE)*CAST(v1 AS DOUBLE))-avg(CAST(v1 AS DOUBLE))*avg(CAST(v1 AS DOUBLE))) * (avg(CAST(v2 AS DOUBLE)*CAST(v2 AS DOUBLE))-avg(CAST(v2 AS DOUBLE))*avg(CAST(v2 AS DOUBLE)))) END AS r2 FROM groupby GROUP BY id2, id4;");',
    ]),
    ("q10.sql", [
        'call gpu_processing("SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS cnt FROM groupby GROUP BY id1, id2, id3, id4, id5, id6;");',
    ]),
]

# ── ClickBench Queries (43 queries against table t) ──────────────────────────
CLICK_QUERIES = [
    ("q0.sql", [
        'call gpu_processing("SELECT COUNT(*) FROM t;");',
    ]),
    ("q1.sql", [
        'call gpu_processing("SELECT COUNT(*) FROM t WHERE AdvEngineID <> 0;");',
    ]),
    ("q2.sql", [
        'call gpu_processing("SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM t;");',
    ]),
    ("q3.sql", [
        'call gpu_processing("SELECT AVG(UserID) FROM t;");',
    ]),
    ("q4.sql", [
        'call gpu_processing("SELECT COUNT(DISTINCT UserID) FROM t;");',
    ]),
    ("q5.sql", [
        'call gpu_processing("SELECT COUNT(*) FROM (SELECT SearchPhrase FROM t GROUP BY SearchPhrase) sub;");',
    ]),
    ("q6.sql", [
        'call gpu_processing("SELECT CAST(MIN(EventTime)/86400 AS INTEGER) AS min_date, CAST(MAX(EventTime)/86400 AS INTEGER) AS max_date FROM t;");',
    ]),
    ("q7.sql", [
        'call gpu_processing("SELECT AdvEngineID, COUNT(*) FROM t WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;");',
    ]),
    ("q8.sql", [
        'call gpu_processing("SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM t GROUP BY RegionID ORDER BY u DESC LIMIT 10;");',
    ]),
    ("q9.sql", [
        'call gpu_processing("SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM t GROUP BY RegionID ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q10.sql", [
        'call gpu_processing("SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM t WHERE MobilePhoneModel <> \'\' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;");',
    ]),
    ("q11.sql", [
        'call gpu_processing("SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM t WHERE MobilePhoneModel <> \'\' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;");',
    ]),
    ("q12.sql", [
        'call gpu_processing("SELECT SearchPhrase, COUNT(*) AS c FROM t WHERE SearchPhrase <> \'\' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q13.sql", [
        'call gpu_processing("SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM t WHERE SearchPhrase <> \'\' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;");',
    ]),
    ("q14.sql", [
        'call gpu_processing("SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM t WHERE SearchPhrase <> \'\' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q15.sql", [
        'call gpu_processing("SELECT UserID, COUNT(*) FROM t GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;");',
    ]),
    ("q16.sql", [
        'call gpu_processing("SELECT UserID, SearchPhrase, COUNT(*) FROM t GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;");',
    ]),
    ("q17.sql", [
        'call gpu_processing("SELECT UserID, SearchPhrase, COUNT(*) FROM t GROUP BY UserID, SearchPhrase LIMIT 10;");',
    ]),
    ("q18.sql", [
        'call gpu_processing("SELECT UserID, CAST((EventTime % 3600) / 60 AS INTEGER) AS m, SearchPhrase, COUNT(*) FROM t GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;");',
    ]),
    ("q19.sql", [
        'call gpu_processing("SELECT UserID FROM t WHERE UserID = 435090932899640449;");',
    ]),
    ("q20.sql", [
        'call gpu_processing("SELECT COUNT(*) FROM t WHERE URL LIKE \'%google%\';");',
    ]),
    ("q21.sql", [
        'call gpu_processing("SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM t WHERE URL LIKE \'%google%\' AND SearchPhrase <> \'\' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q22.sql", [
        'call gpu_processing("SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM t WHERE Title LIKE \'%Google%\' AND URL NOT LIKE \'%.google.%\' AND SearchPhrase <> \'\' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q23.sql", [
        'call gpu_processing("SELECT EventTime, MIN(WatchID) AS WatchID, MIN(URL) AS URL, MIN(Title) AS Title, COUNT(*) AS cnt FROM t WHERE URL LIKE \'%google%\' GROUP BY EventTime ORDER BY EventTime LIMIT 10;");',
    ]),
    ("q24.sql", [
        'call gpu_processing("SELECT SearchPhrase FROM t WHERE SearchPhrase <> \'\' ORDER BY EventTime LIMIT 10;");',
    ]),
    ("q25.sql", [
        'call gpu_processing("SELECT SearchPhrase FROM t WHERE SearchPhrase <> \'\' ORDER BY SearchPhrase LIMIT 10;");',
    ]),
    ("q26.sql", [
        'call gpu_processing("SELECT SearchPhrase FROM t WHERE SearchPhrase <> \'\' ORDER BY EventTime, SearchPhrase LIMIT 10;");',
    ]),
    ("q27.sql", [
        'call gpu_processing("SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM t WHERE URL <> \'\' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;");',
    ]),
    ("q28.sql", [
        "call gpu_processing(\"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM t WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;\");",
    ]),
    ("q29.sql", [
        'call gpu_processing("SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM t;");',
    ]),
    ("q30.sql", [
        'call gpu_processing("SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM t WHERE SearchPhrase <> \'\' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q31.sql", [
        'call gpu_processing("SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM t WHERE SearchPhrase <> \'\' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q32.sql", [
        'call gpu_processing("SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q33.sql", [
        'call gpu_processing("SELECT URL, COUNT(*) AS c FROM t GROUP BY URL ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q34.sql", [
        'call gpu_processing("SELECT 1, URL, COUNT(*) AS c FROM t GROUP BY 1, URL ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q35.sql", [
        'call gpu_processing("SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM t GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;");',
    ]),
    ("q36.sql", [
        'call gpu_processing("SELECT URL, COUNT(*) AS PageViews FROM t WHERE CounterID = 62 AND EventTime >= 1372636800 AND EventTime <= 1375315199 AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> \'\' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;");',
    ]),
    ("q37.sql", [
        'call gpu_processing("SELECT Title, COUNT(*) AS PageViews FROM t WHERE CounterID = 62 AND EventTime >= 1372636800 AND EventTime <= 1375315199 AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> \'\' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;");',
    ]),
    ("q38.sql", [
        'call gpu_processing("SELECT URL, COUNT(*) AS PageViews FROM t WHERE CounterID = 62 AND EventTime >= 1372636800 AND EventTime <= 1375315199 AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;");',
    ]),
    ("q39.sql", [
        'call gpu_processing("SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE \'\' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM t WHERE CounterID = 62 AND EventTime >= 1372636800 AND EventTime <= 1375315199 AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;");',
    ]),
    ("q40.sql", [
        'call gpu_processing("SELECT URLHash, CAST(EventTime/86400 AS INTEGER) AS day, COUNT(*) AS PageViews FROM t WHERE CounterID = 62 AND EventTime >= 1372636800 AND EventTime <= 1375315199 AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, day ORDER BY PageViews DESC LIMIT 10 OFFSET 100;");',
    ]),
    ("q41.sql", [
        'call gpu_processing("SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM t WHERE CounterID = 62 AND EventTime >= 1372636800 AND EventTime <= 1375315199 AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;");',
    ]),
    ("q42.sql", [
        'call gpu_processing("SELECT CAST(EventTime/60 AS INTEGER)*60 AS M, COUNT(*) AS PageViews FROM t WHERE CounterID = 62 AND EventTime >= 1373760000 AND EventTime <= 1373932799 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY M ORDER BY M LIMIT 10 OFFSET 1000;");',
    ]),
]


def write_queries(output_dir: str, subdir: str, buffer_init: str, queries: list):
    """Write SQL files to output_dir/subdir/queries/1/."""
    d = os.path.join(output_dir, subdir, "queries", "1")
    os.makedirs(d, exist_ok=True)
    for fname, gpu_lines in queries:
        path = os.path.join(d, fname)
        with open(path, "w") as f:
            f.write(buffer_init + "\n")
            for line in gpu_lines:
                f.write(line + "\n")
        print(f"  {subdir}/queries/1/{fname}")


def load_microbench_queries(microbench_dir: str, benchmark: str) -> list:
    """Read microbench SQL files from microbench/{benchmark}/ and wrap in gpu_processing().

    Each .sql file (excluding _all_*) becomes one Sirius query file.
    The query name is derived from the filename prefix (e.g., w1_002).
    """
    src_dir = os.path.join(microbench_dir, benchmark)
    if not os.path.isdir(src_dir):
        return []
    queries = []
    for sql_file in sorted(os.listdir(src_dir)):
        if not sql_file.endswith(".sql") or sql_file.startswith("_"):
            continue
        # Extract query name: w1_002_orders_table_count__sum.sql -> w1_002
        parts = sql_file.replace(".sql", "").split("_", 2)
        qname = "_".join(parts[:2]) if len(parts) >= 2 else parts[0]
        filepath = os.path.join(src_dir, sql_file)
        with open(filepath) as f:
            lines = f.read().strip().splitlines()
        # Extract actual SQL (skip comment lines)
        sql_lines = [l.strip() for l in lines if l.strip() and not l.strip().startswith("--")]
        sql_text = " ".join(sql_lines)
        # Escape single quotes for gpu_processing wrapper
        sql_escaped = sql_text.replace("'", "\\'")
        gpu_line = f"call gpu_processing(\"{sql_escaped}\");"
        queries.append((f"{qname}.sql", [gpu_line]))
    return queries


def main():
    parser = argparse.ArgumentParser(description="Generate Sirius GPU SQL files")
    parser.add_argument("--output-dir", default="/workspace",
                        help="Root output directory (default: /workspace)")
    args = parser.parse_args()

    out = args.output_dir
    # Locate microbench source directory (relative to this script's repo root)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.join(script_dir, "..", "..")
    microbench_dir = os.path.join(repo_root, "microbench")

    print("Generating Sirius GPU query SQL files...")

    total = 0

    # Standard benchmarks
    print(f"\n  TPC-H ({len(TPCH_QUERIES)} queries):")
    write_queries(out, "tpch_sql", TPCH_BUFFER, TPCH_QUERIES)
    total += len(TPCH_QUERIES)

    print(f"\n  H2O ({len(H2O_QUERIES)} queries):")
    write_queries(out, "h2o_sql", H2O_BUFFER, H2O_QUERIES)
    total += len(H2O_QUERIES)

    print(f"\n  ClickBench ({len(CLICK_QUERIES)} queries):")
    write_queries(out, "click_sql", CLICK_BUFFER, CLICK_QUERIES)
    total += len(CLICK_QUERIES)

    # Microbenchmarks (read from microbench/ source directory)
    MICROBENCH_MAP = {
        "tpch": ("microbench_tpch_sql", TPCH_BUFFER),
        "h2o": ("microbench_h2o_sql", H2O_BUFFER),
        "clickbench": ("microbench_clickbench_sql", CLICK_BUFFER),
    }
    for bench, (subdir, buf_init) in MICROBENCH_MAP.items():
        mb_queries = load_microbench_queries(microbench_dir, bench)
        if mb_queries:
            print(f"\n  Microbench {bench} ({len(mb_queries)} queries):")
            write_queries(out, subdir, buf_init, mb_queries)
            total += len(mb_queries)
        else:
            print(f"\n  Microbench {bench}: no SQL files found in {microbench_dir}/{bench}/")

    print(f"\nDone: {total} SQL files generated in {out}/")


if __name__ == "__main__":
    main()
