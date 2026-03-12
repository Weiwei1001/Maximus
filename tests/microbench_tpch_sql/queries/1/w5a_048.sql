call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT o_orderstatus, SUM(l_quantity) FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY o_orderstatus;");
