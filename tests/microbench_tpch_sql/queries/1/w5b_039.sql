call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT r_name, COUNT(*), SUM(l_extendedprice) FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey JOIN nation ON c_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey GROUP BY r_name;");
