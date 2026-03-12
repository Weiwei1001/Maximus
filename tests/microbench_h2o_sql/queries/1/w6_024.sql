call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id1, id2, v1 FROM groupby ORDER BY v1 DESC LIMIT 100;");
