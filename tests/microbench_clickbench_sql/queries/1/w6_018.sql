call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT CounterID, EventDate, COUNT(*) AS cnt FROM hits GROUP BY CounterID, EventDate ORDER BY cnt DESC LIMIT 100;");
