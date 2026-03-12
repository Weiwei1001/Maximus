call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id3, SUM(v1) FROM groupby GROUP BY id3;");
