call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT n_name, COUNT(*), SUM(o_totalprice) FROM orders JOIN customer ON o_custkey = c_custkey JOIN nation ON c_nationkey = n_nationkey GROUP BY n_name;");
