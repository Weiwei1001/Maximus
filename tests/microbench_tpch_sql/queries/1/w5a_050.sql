call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT COUNT(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey;");
