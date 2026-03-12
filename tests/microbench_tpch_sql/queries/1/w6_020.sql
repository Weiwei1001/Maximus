call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_totalprice DESC LIMIT 100;");
