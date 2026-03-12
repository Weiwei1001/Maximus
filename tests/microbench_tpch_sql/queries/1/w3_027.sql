call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT RegionID, COUNT(*) FROM hits GROUP BY RegionID;");
