-- All micro-benchmark queries for H2O
-- Ordered by estimated execution time (fastest first)
-- Total: 35 queries

-- [001] w1 | ~4.0ms | -- q8: single count
SELECT COUNT(*) FROM groupby;

-- [002] w1 | ~4.0ms | -- q9: single sum integer
SELECT SUM(v1) FROM groupby;

-- [003] w1 | ~4.0ms | -- q10: single sum double
SELECT SUM(v3) FROM groupby;

-- [004] w1 | ~4.0ms | -- q13: arithmetic expression
SELECT SUM(v1 * v2) FROM groupby;

-- [005] w1 | ~4.0ms | -- q14: compound expression
SELECT SUM(v1 + v2), AVG(v3 * v3) FROM groupby;

-- [006] w1 | ~4.8ms | -- q11: average
SELECT AVG(v1), AVG(v2), AVG(v3) FROM groupby;

-- [007] w1 | ~4.8ms | -- q12: min max
SELECT MIN(v1), MAX(v1), MIN(v3), MAX(v3) FROM groupby;

-- [008] w2 | ~4.8ms | -- q8: ~50% selectivity (numeric range)
SELECT COUNT(*) FROM groupby WHERE id4 > 50;

-- [009] w2 | ~4.8ms | -- q9: ~50% selectivity (value filter)
SELECT SUM(v1) FROM groupby WHERE v2 > 50;

-- [010] w2 | ~4.8ms | -- q10: ~1% selectivity (equality on low-card key)
SELECT AVG(v3) FROM groupby WHERE id1 = 'id001';

-- [011] w2 | ~4.8ms | -- q11: ~20% selectivity (numeric range)
SELECT SUM(v1) FROM groupby WHERE id4 BETWEEN 10 AND 30;

-- [012] w2 | ~4.8ms | -- q12: compound filter
SELECT COUNT(*) FROM groupby WHERE v1 > 0 AND v2 > 0;

-- [013] w2 | ~4.8ms | -- q13: very low selectivity ~0.1%
SELECT MIN(v3), MAX(v3) FROM groupby WHERE id6 < 100;

-- [014] w2 | ~4.8ms | -- q14: high selectivity ~90%
SELECT SUM(v1 + v2) FROM groupby WHERE v3 > -100;

-- [015] w6 | ~6.0ms | -- q13: sort with filter
SELECT id1, v3 FROM groupby WHERE id4 > 50 ORDER BY v3 DESC LIMIT 100;

-- [016] w3 | ~8.0ms | -- q8: ~100 groups (id1)
SELECT id1, SUM(v1) FROM groupby GROUP BY id1;

-- [017] w3 | ~8.0ms | -- q9: ~100 groups (id2)
SELECT id2, SUM(v1), AVG(v2) FROM groupby GROUP BY id2;

-- [018] w3 | ~8.0ms | -- q11: ~100 groups (id4 integer key)
SELECT id4, SUM(v1), SUM(v2) FROM groupby GROUP BY id4;

-- [019] w3 | ~8.0ms | -- q12: 2 keys, ~100x100 = ~10K groups
SELECT id1, id2, SUM(v1) FROM groupby GROUP BY id1, id2;

-- [020] w3 | ~8.0ms | -- q13: ~100 groups (id5)
SELECT id5, AVG(v3) FROM groupby GROUP BY id5;

-- [021] w3 | ~8.0ms | -- q14: single key, single agg
SELECT id1, COUNT(*) FROM groupby GROUP BY id1;

-- [022] w6 | ~8.0ms | -- q9: top-10 by v3
SELECT id1, v3 FROM groupby ORDER BY v3 DESC LIMIT 10;

-- [023] w3 | ~9.6ms | -- q10: ~100 groups, multi-agg
SELECT id1, COUNT(*), SUM(v1), AVG(v2), MIN(v3), MAX(v3) FROM groupby GROUP BY id1;

-- [024] w6 | ~12.0ms | -- q10: top-100 by v1
SELECT id1, id2, v1 FROM groupby ORDER BY v1 DESC LIMIT 100;

-- [025] w6 | ~12.0ms | -- q12: sort by compound expression
SELECT id1, v1 + v2 AS total FROM groupby ORDER BY total DESC LIMIT 100;

-- [026] w6 | ~20.0ms | -- q11: top-1000 by v2
SELECT id1, v2 FROM groupby ORDER BY v2 DESC LIMIT 1000;

-- [027] w4 | ~32.0ms | -- q8: ~100K groups (id3)
SELECT id3, SUM(v1) FROM groupby GROUP BY id3;

-- [028] w4 | ~32.0ms | -- q9: ~100K groups (id6)
SELECT id6, SUM(v1), SUM(v2) FROM groupby GROUP BY id6;

-- [029] w4 | ~32.0ms | -- q12: integer high cardinality
SELECT id6, MIN(v3), MAX(v3) FROM groupby GROUP BY id6;

-- [030] w4 | ~32.0ms | -- q13: ~100K groups single agg
SELECT id3, COUNT(*) FROM groupby GROUP BY id3;

-- [031] w4 | ~64.0ms | -- q11: compound key ~100 x 100K = very high cardinality
SELECT id1, id3, SUM(v1) FROM groupby GROUP BY id1, id3;

-- [032] w4 | ~64.0ms | -- q14: compound key id4 x id6
SELECT id4, id6, SUM(v1) FROM groupby GROUP BY id4, id6;

-- [033] w4 | ~76.8ms | -- q10: ~100K groups, multi-agg
SELECT id3, COUNT(*), AVG(v1), AVG(v2), AVG(v3) FROM groupby GROUP BY id3;

-- [034] w6 | ~80.0ms | -- q8: full sort single column
SELECT v1 FROM groupby ORDER BY v1;

-- [035] w6 | ~80.0ms | -- q14: sort double column
SELECT v3 FROM groupby ORDER BY v3;

