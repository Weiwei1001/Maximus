call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_orderkey, l_extendedprice FROM lineitem ORDER BY l_extendedprice DESC LIMIT 10;");
