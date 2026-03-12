call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT UserID, CounterID FROM hits ORDER BY UserID DESC LIMIT 1000;");
