call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT WatchID, ResolutionWidth FROM hits ORDER BY ResolutionWidth DESC LIMIT 100;");
