call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT SUM(l_extendedprice * l_discount) FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;");
