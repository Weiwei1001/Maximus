call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT AVG(l_quantity) FROM lineitem WHERE l_shipdate >= \'1995-01-01\';");
