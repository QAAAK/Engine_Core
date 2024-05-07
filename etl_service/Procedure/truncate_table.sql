CREATE OR REPLACE FUNCTION etl_service.truncate_table(p_tbl_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
			
declare	
	
begin
	
	-- очищаем таблицу
	execute 'truncate table_schema.' || p_tbl_name || ';' ;


end;









$$
EXECUTE ON ANY;