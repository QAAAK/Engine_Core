create table etl_service.tasks (

	id	int8 default nextval('sequence') not null,
	target_table	varchar(150) not null,
	status 	varchar (11) null,
	md_id	int8 not null,
	task_code	text null,
	launch_interval	date null,
	last_launch	date null
)
	with (appendoptimized=true, compresstype=zstd, compresslevel=1, orientation = column)

 	distributed by (id)
