call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT MIN(l_discount), MAX(l_discount) FROM lineitem;");
