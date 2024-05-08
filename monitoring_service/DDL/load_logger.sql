create monitoring_service.load_logger (

	id	int8 default nextval('sequence') not null,
	target_table	varchar(150) not null,
	last_status	varchar (11) null,
	quantity_ rows	int8 null,
	message_text	text null,
	dt_begin	timestamp null,
	dt_end	timestamp null
)
	with (appendoptimized=true, compresstype=zstd, compresslevel=1, orientation = column)

 	distributed by (id)
