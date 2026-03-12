call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT SUM(v1 * v2) FROM groupby;");
