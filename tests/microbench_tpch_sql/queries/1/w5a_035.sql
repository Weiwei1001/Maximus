call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT SUM(l_extendedprice) FROM lineitem JOIN orders ON l_orderkey = o_orderkey;");
