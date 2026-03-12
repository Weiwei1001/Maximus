call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT COUNT(*) FROM hits WHERE AdvEngineID > 0;");
