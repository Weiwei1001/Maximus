-- -- q20: ~few groups (TraficSourceID)
-- Workload: w3 | Estimated GPU time: ~7.0ms

SELECT TraficSourceID, COUNT(*), SUM(GoodEvent) FROM hits GROUP BY TraficSourceID;
