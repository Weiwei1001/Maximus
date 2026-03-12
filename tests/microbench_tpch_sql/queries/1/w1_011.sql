call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT SUM(l_quantity), SUM(l_extendedprice), AVG(l_discount), AVG(l_tax) FROM lineitem;");
