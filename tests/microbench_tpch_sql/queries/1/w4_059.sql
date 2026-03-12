call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_suppkey, MIN(l_extendedprice), MAX(l_extendedprice), AVG(l_quantity) FROM lineitem GROUP BY l_suppkey;");
