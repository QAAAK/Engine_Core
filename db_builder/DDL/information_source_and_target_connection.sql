-- db_builder.information_source_and_target_connection definition

-- Drop table

-- DROP TABLE db_builder.information_source_and_target_connection;

CREATE TABLE db_builder.information_source_and_target_connection (
	id int8 NOT NULL DEFAULT nextval('db_builder.sequence_information_source_and_target_connection'::regclass),
	source_table text NULL,
	target_table text NULL,
	type_load varchar(50) NULL,
	script text NULL,
	last_update timestamp NULL,
	create_column varchar(20) NULL,
	increment_column varchar(20) NULL
)
DISTRIBUTED BY (id);

-- Permissions

ALTER TABLE db_builder.information_source_and_target_connection OWNER TO santalovdv;
GRANT ALL ON TABLE db_builder.information_source_and_target_connection TO santalovdv;
GRANT ALL ON TABLE db_builder.information_source_and_target_connection TO analyze_bi_owner;