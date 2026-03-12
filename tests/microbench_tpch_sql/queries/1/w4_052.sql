call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_partkey, SUM(l_quantity) FROM lineitem GROUP BY l_partkey;");
