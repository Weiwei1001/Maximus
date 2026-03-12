call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity), AVG(l_extendedprice) FROM lineitem GROUP BY l_returnflag, l_linestatus;");
