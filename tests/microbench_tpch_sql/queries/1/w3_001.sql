call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT c_mktsegment, COUNT(*), AVG(c_acctbal) FROM customer GROUP BY c_mktsegment;");
