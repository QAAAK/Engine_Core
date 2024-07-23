-- db_builder.information_tables_dds definition

-- Drop table

-- DROP TABLE db_builder.information_tables_dds;

CREATE TABLE db_builder.information_tables_dds (
	id int8 NOT NULL DEFAULT nextval('db_builder.sequence_information_table'::regclass),
	table_name text NOT NULL,
	comment_table_name text NULL,
	when_update text NULL,
	last_dttm timestamp NULL,
	next_dttm timestamp NULL
)
DISTRIBUTED BY (id);

-- Permissions

ALTER TABLE db_builder.information_tables_dds OWNER TO analyze_bi_owner;
GRANT ALL ON TABLE db_builder.information_tables_dds TO analyze_bi_owner;
GRANT SELECT ON TABLE db_builder.information_tables_dds TO analyze_bi_ro;