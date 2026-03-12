call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT SUM(GoodEvent) FROM hits WHERE CounterID > 10000 AND RegionID > 100;");
