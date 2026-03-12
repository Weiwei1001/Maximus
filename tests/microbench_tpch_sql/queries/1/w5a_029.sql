call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT c_mktsegment, COUNT(*), SUM(o_totalprice) FROM orders JOIN customer ON o_custkey = c_custkey GROUP BY c_mktsegment;");
