call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT COUNT(*) FROM groupby WHERE v1 > 0 AND v2 > 0;");
