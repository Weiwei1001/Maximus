call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT COUNT(*), SUM(o_totalprice) FROM orders WHERE o_totalprice > 200000;");
