call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT SUM(v1) FROM groupby WHERE id4 BETWEEN 10 AND 30;");
