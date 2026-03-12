call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT SUM(l_extendedprice) FROM lineitem JOIN part ON l_partkey = p_partkey WHERE p_size < 10;");
