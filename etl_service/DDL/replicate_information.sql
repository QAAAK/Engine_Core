create table etl_service.replicate_information (
	
	id	int8 default nextval('sequence') not null,
	loadtype	varchar(9) not null,
	source_table	varchar(150) not null,
	target_table	varchar(150) not null,
	list_column	text not null,
	field_increment date null,
	distrib	varchar(50) null,
	last_date	date null

)
	with (appendoptimized=true, compresstype=zstd, compresslevel=1, orientation = column)

 	distributed by (id)
