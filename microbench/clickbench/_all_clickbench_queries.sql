-- All micro-benchmark queries for CLICKBENCH
-- Ordered by estimated execution time (fastest first)
-- Total: 25 queries

-- [001] w1 | ~3.5ms | -- q15: count
SELECT COUNT(*) FROM hits;

-- [002] w1 | ~3.5ms | -- q16: sum integer column
SELECT SUM(AdvEngineID) FROM hits;

-- [003] w1 | ~3.5ms | -- q17: average
SELECT AVG(ResolutionWidth) FROM hits;

-- [004] w1 | ~3.5ms | -- q18: min max timestamp
SELECT MIN(EventTime), MAX(EventTime) FROM hits;

-- [005] w1 | ~3.5ms | -- q19: count + sum
SELECT COUNT(*), SUM(GoodEvent) FROM hits;

-- [006] w1 | ~4.2ms | -- q20: multi-column aggregation
SELECT SUM(ResolutionWidth), AVG(ResolutionHeight), MIN(ClientIP), MAX(UserID) FROM hits;

-- [007] w2 | ~4.2ms | -- q15: very low selectivity
SELECT COUNT(*) FROM hits WHERE AdvEngineID > 0;

-- [008] w2 | ~4.2ms | -- q17: date range (~3% selectivity)
SELECT COUNT(*) FROM hits WHERE EventDate >= '2013-07-15' AND EventDate < '2013-08-01';

-- [009] w2 | ~4.2ms | -- q18: boolean filter
SELECT AVG(ResolutionWidth) FROM hits WHERE GoodEvent = 1;

-- [010] w2 | ~4.2ms | -- q19: high selectivity
SELECT COUNT(*) FROM hits WHERE UserID != 0;

-- [011] w3 | ~7.0ms | -- q15: ~3 groups (GoodEvent)
SELECT GoodEvent, COUNT(*) FROM hits GROUP BY GoodEvent;

-- [012] w3 | ~7.0ms | -- q16: few groups (AdvEngineID, most are 0)
SELECT AdvEngineID, COUNT(*) FROM hits GROUP BY AdvEngineID;

-- [013] w3 | ~7.0ms | -- q17: few groups (OS)
SELECT OS, COUNT(*) FROM hits GROUP BY OS;

-- [014] w3 | ~7.0ms | -- q18: few groups (ResolutionDepth)
SELECT ResolutionDepth, COUNT(*), AVG(ResolutionWidth) FROM hits GROUP BY ResolutionDepth;

-- [015] w3 | ~7.0ms | -- q20: ~few groups (TraficSourceID)
SELECT TraficSourceID, COUNT(*), SUM(GoodEvent) FROM hits GROUP BY TraficSourceID;

-- [016] w6 | ~10.5ms | -- q15: sort by timestamp
SELECT WatchID, EventTime FROM hits ORDER BY EventTime LIMIT 100;

-- [017] w6 | ~10.5ms | -- q16: top-100 by WatchID
SELECT WatchID FROM hits ORDER BY WatchID DESC LIMIT 100;

-- [018] w6 | ~10.5ms | -- q19: top-100 with compound sort key
SELECT CounterID, EventDate, COUNT(*) AS cnt FROM hits GROUP BY CounterID, EventDate ORDER BY cnt DESC LIMIT 100;

-- [019] w6 | ~10.5ms | -- q20: sort by ResolutionWidth
SELECT WatchID, ResolutionWidth FROM hits ORDER BY ResolutionWidth DESC LIMIT 100;

-- [020] w6 | ~17.5ms | -- q18: top-1000 by UserID
SELECT UserID, CounterID FROM hits ORDER BY UserID DESC LIMIT 1000;

-- [021] w4 | ~28.0ms | -- q15: ~500K+ groups (CounterID)
SELECT CounterID, COUNT(*) FROM hits GROUP BY CounterID;

-- [022] w4 | ~28.0ms | -- q16: ~many groups (UserID)
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID;

-- [023] w4 | ~28.0ms | -- q17: ~many groups (URLHash)
SELECT URLHash, COUNT(*) FROM hits GROUP BY URLHash;

-- [024] w4 | ~28.0ms | -- q18: high cardinality (RefererHash)
SELECT RefererHash, COUNT(*), SUM(GoodEvent) FROM hits GROUP BY RefererHash;

-- [025] w4 | ~56.0ms | -- q20: date + counter (medium-high cardinality)
SELECT EventDate, CounterID, COUNT(*) FROM hits GROUP BY EventDate, CounterID;

