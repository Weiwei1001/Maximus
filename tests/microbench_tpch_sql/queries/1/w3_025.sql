call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT l_shipmode, COUNT(*), SUM(l_extendedprice) FROM lineitem GROUP BY l_shipmode;");
