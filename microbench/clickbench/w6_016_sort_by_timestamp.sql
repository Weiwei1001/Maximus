-- -- q15: sort by timestamp
-- Workload: w6 | Estimated GPU time: ~10.5ms

SELECT WatchID, EventTime FROM hits ORDER BY EventTime LIMIT 100;
