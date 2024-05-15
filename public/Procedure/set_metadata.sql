CREATE OR REPLACE FUNCTION public.set_metadata ()
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	cur refcursor;

	tbl_name text;
	tbl_schema text;
	type_load numeric(1);
	is_table_in_metadata int2;


begin
	
	open cur for execute 'select table_schema, table_name from information_schema.tables
					  where 1= 1 and 
                      table_type = ''BASE TABLE'' and 
                      table_name not like ''%_prt_%'';';
                     
                     
    loop
	    fetch cur into tbl_schema, tbl_name;

		exit when not found;
		
		is_table_in_metadata = public.is_table_in_replicate_metadata(tbl_name);
		
		CONTINUE when is_table_in_metadata = 1;
	
		type_load := public.determine_loadtype(tbl_schema, tbl_name);
	
	
		insert into etl_service.replicate_information (loadtype,schema_name,  source_table, target_table, field_increment, last_date)
		values (type_load, tbl_schema, tbl_name, 'ext_'||tbl_name, 'period', current_date);
		
	end loop;


	
		
return 'PASSED';	
	
end;









$$
EXECUTE ON ANY;