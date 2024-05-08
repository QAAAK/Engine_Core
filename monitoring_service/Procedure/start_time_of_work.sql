CREATE OR REPLACE FUNCTION monitoring_service.start_time_of_work( table_id int8 )
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

		

begin


	update monitoring_service.load_logger set dt_begin = clock_timestamp() , dt_end = null
	where id = table_id;
	
		

	
end;









$$
EXECUTE ON ANY;