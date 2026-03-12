call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_partkey, l_shipmode, AVG(l_discount) FROM lineitem GROUP BY l_partkey, l_shipmode;");
