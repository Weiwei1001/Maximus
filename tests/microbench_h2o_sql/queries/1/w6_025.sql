call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id1, v1 + v2 AS total FROM groupby ORDER BY total DESC LIMIT 100;");
