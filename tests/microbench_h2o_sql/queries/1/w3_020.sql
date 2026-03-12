call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id5, AVG(v3) FROM groupby GROUP BY id5;");
