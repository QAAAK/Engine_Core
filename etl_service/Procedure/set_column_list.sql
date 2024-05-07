CREATE OR REPLACE FUNCTION etl_service.set_column_list(tbl_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	list_column text;

begin

	array_agg(column_name::text order by ordinal_position) 
	into list_column
	from information_schema.columns
	where table_name = tbl_name;
	
	
return list_column;
	
end;









$$
EXECUTE ON ANY;