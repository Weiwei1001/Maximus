call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id1, v2 FROM groupby ORDER BY v2 DESC LIMIT 1000;");
