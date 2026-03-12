call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT MIN(v1), MAX(v1), MIN(v3), MAX(v3) FROM groupby;");
