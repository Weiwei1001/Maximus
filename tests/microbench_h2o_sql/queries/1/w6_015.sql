call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id1, v3 FROM groupby WHERE id4 > 50 ORDER BY v3 DESC LIMIT 100;");
