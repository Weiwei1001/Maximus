call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_extendedprice FROM lineitem ORDER BY l_extendedprice;");
