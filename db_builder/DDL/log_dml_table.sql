
create table db_builder.log_dml_table(

	id bigint not null DEFAULT nextval('db_builder.sequence_log_dml_table'::regclass),
	table_name text not null,
	status text not null,
	operation text not null,
	quantity_rows bigint not null,
	message_text text null,
	dttm timestamp not null
	
) distributed by (id);