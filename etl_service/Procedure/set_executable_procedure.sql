the executable procedure


CREATE OR REPLACE FUNCTION etl_service.set_executable_procedure( id_table int8 )
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	set_script text;
	
	tbl_name text;
	clmn_list text;

begin


	select source_table, list_column
		into tbl_name, clmn_list
		from etl_service.replicate_information 
	where id = id_table;


	set_script := 'insert into etl_service.' || tbl_name || ' (' || clmn_list || ') (select ' ||  clmn_list || ' from source_hive.'|| tbl_name || ')';
	
	
	
	
	
return set_script;
	
end;









$$
EXECUTE ON ANY;