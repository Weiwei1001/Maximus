call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT EventTime, CounterID FROM hits WHERE RegionID = 229 ORDER BY EventTime LIMIT 100;");
