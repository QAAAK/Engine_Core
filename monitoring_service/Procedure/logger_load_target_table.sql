CREATE OR REPLACE FUNCTION monitoring_service.logger_load_target_table(table_id bigint, error_text text, count_rows int8)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	


	

begin


	
	update monitoring_service.load_logger 
	set  quantity_rows = count_rows, message_text = error_text
	where id = table_id;

	
	
	

	
end;









$$
EXECUTE ON ANY;