call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id2, SUM(v1), AVG(v2) FROM groupby GROUP BY id2;");
