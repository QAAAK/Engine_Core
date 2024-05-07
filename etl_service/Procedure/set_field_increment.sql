CREATE OR REPLACE FUNCTION etl_service.set_field_increment( table_id int8 )
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	
	column_name_for_increment varchar(20);
	
	

begin


	select field_increment
		into column_name_for_increment
		from etl_service.infromation_replicate
	where id = table_id;
	
		
	
return column_name_for_increment;
	
end;









$$
EXECUTE ON ANY;