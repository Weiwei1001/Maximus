call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id6, SUM(v1), SUM(v2) FROM groupby GROUP BY id6;");
