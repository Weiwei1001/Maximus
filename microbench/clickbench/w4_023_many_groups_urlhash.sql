-- -- q17: ~many groups (URLHash)
-- Workload: w4 | Estimated GPU time: ~28.0ms

SELECT URLHash, COUNT(*) FROM hits GROUP BY URLHash;
