CREATE TABLE a_tmp_b2b.replicate_information (
	id int8 NOT NULL DEFAULT nextval('a_tmp_b2b.sequence_replicate_information'::regclass),
	loadtype numeric(1) NOT NULL,
	schema_name varchar(50) NOT NULL,
	source_table varchar(150) NOT NULL,
	target_table varchar(150) NOT NULL,
	field_increment varchar(50) NULL,
	last_date date NULL
)
WITH (
	appendonly=true,
	compresstype=zstd,
	compresslevel=1,
	orientation=column
)
DISTRIBUTED BY (id);