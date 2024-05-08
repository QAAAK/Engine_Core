the executable procedure


CREATE OR REPLACE FUNCTION etl_service.set_executable_procedure( table_id int8 )
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	set_script text;
	
	tbl_name text;
	clmn_list text;
	
	column_name_for_increment varchar(20);

begin


	select source_table
		into tbl_name
		from etl_service.replicate_information 
	where id = table_id;
	
	
	clmn_list := etl_service.set_column_list(tbl_name);
	
	column_name_for_increment := set_field_increment(table_id);
	
	if column_name_for_increment isnull 
		then 
			set_script := 'insert into etl_service.' || tbl_name || ' (' || clmn_list || ') (select ' ||  clmn_list || ' from source_hive.'|| tbl_name || ')';
	else if column_name_for_increment isnotnull 
		then 
			set_script := 'insert into etl_service.' || tbl_name || ' (' || clmn_list || ') (select ' ||  clmn_list || ' from source_hive.'|| tbl_name || 
						  ' where '|| column_name_for_increment || ' > (select '|| column_name_for_increment || ' from etl_service.' || tbl_name '))';
	end if;
	
	
	
return set_script;
	
end;









$$
EXECUTE ON ANY;