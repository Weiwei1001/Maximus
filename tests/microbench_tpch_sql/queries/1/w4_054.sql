call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_orderkey, SUM(l_quantity), SUM(l_extendedprice) FROM lineitem GROUP BY l_orderkey;");
