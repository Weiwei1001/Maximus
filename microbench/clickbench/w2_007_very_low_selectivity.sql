-- -- q15: very low selectivity
-- Workload: w2 | Estimated GPU time: ~4.2ms

SELECT COUNT(*) FROM hits WHERE AdvEngineID > 0;
