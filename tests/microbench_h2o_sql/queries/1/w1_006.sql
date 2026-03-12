call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT AVG(v1), AVG(v2), AVG(v3) FROM groupby;");
