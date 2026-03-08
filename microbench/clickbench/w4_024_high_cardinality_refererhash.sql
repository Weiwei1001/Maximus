-- -- q18: high cardinality (RefererHash)
-- Workload: w4 | Estimated GPU time: ~28.0ms

SELECT RefererHash, COUNT(*), SUM(GoodEvent) FROM hits GROUP BY RefererHash;
