CREATE SEQUENCE db_builder.sequence_information_table
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	NO CYCLE;

-- Permissions

ALTER SEQUENCE db_builder.sequence_information_table OWNER TO analyze_bi_owner;
GRANT ALL ON SEQUENCE  db_builder.sequence_information_table TO analyze_bi_owner;