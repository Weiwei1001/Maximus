call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT WatchID FROM hits ORDER BY WatchID DESC LIMIT 100;");
