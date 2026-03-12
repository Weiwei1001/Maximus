call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT OS, COUNT(*) FROM hits GROUP BY OS;");
