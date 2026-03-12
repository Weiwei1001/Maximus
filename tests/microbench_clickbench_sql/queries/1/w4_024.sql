call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT RefererHash, COUNT(*), SUM(GoodEvent) FROM hits GROUP BY RefererHash;");
