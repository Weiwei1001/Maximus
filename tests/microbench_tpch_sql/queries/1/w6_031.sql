call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_orderkey, l_quantity FROM lineitem ORDER BY l_quantity DESC LIMIT 100;");
