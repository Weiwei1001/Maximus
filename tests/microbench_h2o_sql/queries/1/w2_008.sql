call gpu_buffer_init("20 GB", "10 GB");
call gpu_processing("SELECT COUNT(*) FROM groupby WHERE id4 > 50;");
