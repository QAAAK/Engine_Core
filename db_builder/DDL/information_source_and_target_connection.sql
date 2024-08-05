create table db_builder.information_source_and_target_connection (

	id int8 NOT NULL DEFAULT nextval('db_builder.sequence_information_source_and_target_connection'::regclass),	
	source_table text,
	target_table text,
	type_load varchar(50),
	script text,
	last_update text
) distributed by (id)