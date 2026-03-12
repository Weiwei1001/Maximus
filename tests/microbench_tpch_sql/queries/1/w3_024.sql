call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_linestatus, SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_linestatus;");
