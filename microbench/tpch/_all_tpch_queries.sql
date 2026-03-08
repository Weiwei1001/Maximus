-- All micro-benchmark queries for TPCH
-- Ordered by estimated execution time (fastest first)
-- Total: 60 queries

-- [001] w3 | ~1.0ms | -- q7: 5 groups (c_mktsegment)
SELECT c_mktsegment, COUNT(*), AVG(c_acctbal) FROM customer GROUP BY c_mktsegment;

-- [002] w1 | ~1.5ms | -- q7: orders table count + sum
SELECT COUNT(*), SUM(o_totalprice) FROM orders;

-- [003] w2 | ~1.8ms | -- q7: orders filter
SELECT COUNT(*), SUM(o_totalprice) FROM orders WHERE o_totalprice > 200000;

-- [004] w1 | ~3.0ms | -- q1: single column count
SELECT COUNT(*) FROM lineitem;

-- [005] w1 | ~3.0ms | -- q2: single column sum
SELECT SUM(l_quantity) FROM lineitem;

-- [006] w1 | ~3.0ms | -- q3: single column average
SELECT AVG(l_extendedprice) FROM lineitem;

-- [007] w1 | ~3.0ms | -- q4: min + max
SELECT MIN(l_discount), MAX(l_discount) FROM lineitem;

-- [008] w1 | ~3.0ms | -- q5: arithmetic expression
SELECT SUM(l_extendedprice * l_discount) FROM lineitem;

-- [009] w3 | ~3.0ms | -- q5: 3 groups (o_orderstatus)
SELECT o_orderstatus, COUNT(*), SUM(o_totalprice) FROM orders GROUP BY o_orderstatus;

-- [010] w3 | ~3.0ms | -- q6: 5 groups (o_orderpriority)
SELECT o_orderpriority, COUNT(*) FROM orders GROUP BY o_orderpriority;

-- [011] w1 | ~3.6ms | -- q6: multi-column aggregation
SELECT SUM(l_quantity), SUM(l_extendedprice), AVG(l_discount), AVG(l_tax) FROM lineitem;

-- [012] w2 | ~3.6ms | -- q1: ~50% selectivity (l_quantity > 25)
SELECT COUNT(*) FROM lineitem WHERE l_quantity > 25;

-- [013] w2 | ~3.6ms | -- q2: ~10% selectivity (narrow range)
SELECT SUM(l_extendedprice) FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07;

-- [014] w2 | ~3.6ms | -- q3: ~33% selectivity (date range)
SELECT AVG(l_quantity) FROM lineitem WHERE l_shipdate >= '1995-01-01';

-- [015] w2 | ~3.6ms | -- q4: ~25% selectivity (flag equality)
SELECT COUNT(*) FROM lineitem WHERE l_returnflag = 'R';

-- [016] w2 | ~3.6ms | -- q5: ~2% selectivity (compound predicate)
SELECT SUM(l_extendedprice * l_discount) FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

-- [017] w2 | ~3.6ms | -- q6: high selectivity ~80%
SELECT SUM(l_quantity) FROM lineitem WHERE l_extendedprice > 10000;

-- [018] w2 | ~4.2ms | -- q16: medium selectivity (region filter)
SELECT SUM(ResolutionWidth) FROM hits WHERE RegionID = 229;

-- [019] w2 | ~4.2ms | -- q20: compound predicate
SELECT SUM(GoodEvent) FROM hits WHERE CounterID > 10000 AND RegionID > 100;

-- [020] w6 | ~4.5ms | -- q6: orders sort by price
SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_totalprice DESC LIMIT 100;

-- [021] w6 | ~4.5ms | -- q7: sort + filter
SELECT l_orderkey, l_extendedprice FROM lineitem WHERE l_returnflag = 'R' ORDER BY l_extendedprice DESC LIMIT 100;

-- [022] w6 | ~5.2ms | -- q17: sort with filter
SELECT EventTime, CounterID FROM hits WHERE RegionID = 229 ORDER BY EventTime LIMIT 100;

-- [023] w3 | ~6.0ms | -- q1: 2 groups (l_returnflag)
SELECT l_returnflag, COUNT(*), SUM(l_quantity) FROM lineitem GROUP BY l_returnflag;

-- [024] w3 | ~6.0ms | -- q2: 4 groups (l_linestatus)
SELECT l_linestatus, SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_linestatus;

-- [025] w3 | ~6.0ms | -- q4: 7 groups (l_shipmode)
SELECT l_shipmode, COUNT(*), SUM(l_extendedprice) FROM lineitem GROUP BY l_shipmode;

-- [026] w6 | ~6.0ms | -- q2: top-10 by price
SELECT l_orderkey, l_extendedprice FROM lineitem ORDER BY l_extendedprice DESC LIMIT 10;

-- [027] w3 | ~7.0ms | -- q19: ~250 groups (RegionID top values)
SELECT RegionID, COUNT(*) FROM hits GROUP BY RegionID;

-- [028] w3 | ~7.2ms | -- q3: ~6 groups (returnflag x linestatus)
SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity), AVG(l_extendedprice) FROM lineitem GROUP BY l_returnflag, l_linestatus;

-- [029] w5a | ~9.0ms | -- q5: orders JOIN customer (~1.5M x 150K)
SELECT c_mktsegment, COUNT(*), SUM(o_totalprice) FROM orders JOIN customer ON o_custkey = c_custkey GROUP BY c_mktsegment;

-- [030] w5b | ~9.0ms | -- q2: 5-table + filter on region
SELECT SUM(l_extendedprice * (1 - l_discount))
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON o_custkey = c_custkey
JOIN nation ON c_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE r_name = 'ASIA';

-- [031] w6 | ~9.0ms | -- q3: top-100 by quantity
SELECT l_orderkey, l_quantity FROM lineitem ORDER BY l_quantity DESC LIMIT 100;

-- [032] w6 | ~9.0ms | -- q5: sort by date
SELECT l_orderkey, l_shipdate FROM lineitem ORDER BY l_shipdate LIMIT 100;

-- [033] w4 | ~12.0ms | -- q4: ~150K groups (c_custkey)
SELECT o_custkey, COUNT(*), SUM(o_totalprice) FROM orders GROUP BY o_custkey;

-- [034] w5a | ~12.0ms | -- q1: lineitem JOIN orders (largest 2-table join, ~6M x 1.5M)
SELECT COUNT(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey;

-- [035] w5a | ~12.0ms | -- q2: lineitem JOIN orders + aggregation
SELECT SUM(l_extendedprice) FROM lineitem JOIN orders ON l_orderkey = o_orderkey;

-- [036] w5a | ~12.0ms | -- q3: lineitem JOIN orders + filter on orders
SELECT COUNT(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey WHERE o_orderstatus = 'F';

-- [037] w5a | ~12.0ms | -- q6: lineitem JOIN part (~6M x 200K)
SELECT SUM(l_extendedprice) FROM lineitem JOIN part ON l_partkey = p_partkey WHERE p_size < 10;

-- [038] w5a | ~13.5ms | -- q10: 3-table: orders-customer-nation
SELECT n_name, COUNT(*), SUM(o_totalprice) FROM orders JOIN customer ON o_custkey = c_custkey JOIN nation ON c_nationkey = n_nationkey GROUP BY n_name;

-- [039] w5b | ~13.5ms | -- q1: 5-table: lineitem-orders-customer-nation-region
SELECT r_name, COUNT(*), SUM(l_extendedprice)
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON o_custkey = c_custkey
JOIN nation ON c_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
GROUP BY r_name;

-- [040] w5b | ~13.5ms | -- q3: 5-table: lineitem-part-supplier-nation-region
SELECT r_name, SUM(l_extendedprice)
FROM lineitem
JOIN part ON l_partkey = p_partkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
GROUP BY r_name;

-- [041] w5b | ~13.5ms | -- q4: 5-table + filter on part
SELECT n_name, SUM(l_quantity)
FROM lineitem
JOIN part ON l_partkey = p_partkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE p_size < 10
GROUP BY n_name;

-- [042] w5b | ~13.5ms | -- q5: 5-table: lineitem-orders-customer-supplier-nation (supplier side)
SELECT n_name, SUM(l_extendedprice)
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON o_custkey = c_custkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
GROUP BY n_name;

-- [043] w5b | ~13.5ms | -- q7: 6-table + date filter
SELECT r_name, SUM(l_extendedprice * (1 - l_discount))
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON o_custkey = c_custkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE o_orderdate >= '1995-01-01' AND o_orderdate < '1996-01-01'
GROUP BY r_name;

-- [044] w5b | ~13.5ms | -- q8: 5-table: lineitem-partsupp-part-supplier-nation
SELECT n_name, SUM(l_quantity), SUM(ps_supplycost)
FROM lineitem
JOIN partsupp ON l_partkey = ps_partkey AND l_suppkey = ps_suppkey
JOIN part ON l_partkey = p_partkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
GROUP BY n_name;

-- [045] w5b | ~13.5ms | -- q9: 6-table: lineitem-partsupp-part-supplier-nation-region
SELECT r_name, COUNT(*), AVG(l_extendedprice)
FROM lineitem
JOIN partsupp ON l_partkey = ps_partkey AND l_suppkey = ps_suppkey
JOIN part ON l_partkey = p_partkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
GROUP BY r_name;

-- [046] w6 | ~15.0ms | -- q4: top-1000 by compound expression
SELECT l_orderkey, l_extendedprice * (1 - l_discount) AS net_price FROM lineitem ORDER BY net_price DESC LIMIT 1000;

-- [047] w5b | ~16.2ms | -- q6: 6-table: lineitem-orders-customer-supplier-nation-region
SELECT r_name, COUNT(*), SUM(l_extendedprice), AVG(l_discount)
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON o_custkey = c_custkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
GROUP BY r_name;

-- [048] w5a | ~18.0ms | -- q4: lineitem JOIN orders + group by
SELECT o_orderstatus, SUM(l_quantity) FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY o_orderstatus;

-- [049] w5a | ~18.0ms | -- q7: lineitem JOIN supplier (~6M x 10K)
SELECT s_nationkey, SUM(l_extendedprice) FROM lineitem JOIN supplier ON l_suppkey = s_suppkey GROUP BY s_nationkey;

-- [050] w5a | ~18.0ms | -- q8: 3-table: lineitem-orders-customer
SELECT COUNT(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey;

-- [051] w5b | ~20.2ms | -- q10: 5-table with aggregation: lineitem-orders-customer + supplier-nation
SELECT n_name, c_mktsegment, SUM(l_extendedprice)
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON o_custkey = c_custkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
GROUP BY n_name, c_mktsegment;

-- [052] w4 | ~24.0ms | -- q1: ~200K groups (l_partkey, 200K distinct values at SF=1)
SELECT l_partkey, SUM(l_quantity) FROM lineitem GROUP BY l_partkey;

-- [053] w4 | ~24.0ms | -- q2: ~10K groups (l_suppkey)
SELECT l_suppkey, COUNT(*), SUM(l_extendedprice) FROM lineitem GROUP BY l_suppkey;

-- [054] w4 | ~24.0ms | -- q3: ~1.5M groups (l_orderkey)
SELECT l_orderkey, SUM(l_quantity), SUM(l_extendedprice) FROM lineitem GROUP BY l_orderkey;

-- [055] w4 | ~24.0ms | -- q7: date group (~2500 distinct dates)
SELECT l_shipdate, COUNT(*), SUM(l_quantity) FROM lineitem GROUP BY l_shipdate;

-- [056] w5a | ~27.0ms | -- q9: 3-table + group by segment
SELECT c_mktsegment, SUM(l_extendedprice) FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey GROUP BY c_mktsegment;

-- [057] w4 | ~48.0ms | -- q5: ~200K x 7 = high cardinality compound key
SELECT l_partkey, l_shipmode, AVG(l_discount) FROM lineitem GROUP BY l_partkey, l_shipmode;

-- [058] w4 | ~56.0ms | -- q19: high cardinality compound key
SELECT CounterID, RegionID, COUNT(*) FROM hits GROUP BY CounterID, RegionID;

-- [059] w4 | ~57.6ms | -- q6: ~10K groups, multi-agg
SELECT l_suppkey, MIN(l_extendedprice), MAX(l_extendedprice), AVG(l_quantity) FROM lineitem GROUP BY l_suppkey;

-- [060] w6 | ~60.0ms | -- q1: full sort single column (numeric)
SELECT l_extendedprice FROM lineitem ORDER BY l_extendedprice;

