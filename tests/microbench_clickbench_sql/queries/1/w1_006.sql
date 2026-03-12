call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT SUM(ResolutionWidth), AVG(ResolutionHeight), MIN(ClientIP), MAX(UserID) FROM hits;");
