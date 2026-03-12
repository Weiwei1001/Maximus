call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT o_custkey, COUNT(*), SUM(o_totalprice) FROM orders GROUP BY o_custkey;");
