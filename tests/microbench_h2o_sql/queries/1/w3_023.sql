call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT id1, COUNT(*), SUM(v1), AVG(v2), MIN(v3), MAX(v3) FROM groupby GROUP BY id1;");
