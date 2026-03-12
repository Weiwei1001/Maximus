call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT ResolutionDepth, COUNT(*), AVG(ResolutionWidth) FROM hits GROUP BY ResolutionDepth;");
