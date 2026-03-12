call gpu_buffer_init("10 GB", "10 GB");
call gpu_processing("SELECT COUNT(*) FROM hits WHERE EventDate >= \'2013-07-15\' AND EventDate < \'2013-08-01\';");
