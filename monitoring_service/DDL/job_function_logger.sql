create monitoring_service.job_function_logger (

	id	int8 default nextval('sequence') not null,
	procedure	varchar(150) not null,
	status	varchar(20) null,
	message_text	text null,
	dt_begin	timestamp null,
	dt_end	timestamp null

)
	with (appendoptimized=true, compresstype=zstd, compresslevel=1, orientation = column)

 	distributed by (id)