call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT o_orderpriority, COUNT(*) FROM orders GROUP BY o_orderpriority;");
