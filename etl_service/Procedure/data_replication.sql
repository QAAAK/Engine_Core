CREATE OR REPLACE FUNCTION etl_service.data_replication ( table_id int8 )
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	
	interval_is_included boolean;
	
	script_replication text;
	
	quantity_insert_rows int8;
	
	message_error_text text := null;
	
	process_status varchar := 'PASSED'
	

begin
	begin
	
	perform monitoring_etl.start_time_of_work(table_id);
	

	interval_is_included := etl_service.data_included_in_the_interval(table_id);
	
	
	
	
	
	if interval_is_included == true 
		then 
			
			perform monitoring_service.change_of_status(table_id, interval_is_included);
			script_replication := etl_service.set_executable_procedure(table_id);
	else
			perform monitoring_service.change_of_status(table_id, interval_is_included);
	end if;
	
	
	execute script_replication;
	
	
	
	perform monitoring_etl.end_time_of_work(table_id);
	
	
	
	GET DIAGNOSTICS quantity_insert_rows = ROW_COUNT;
	
	perform monitoring_service.logger_load_target_table(table_id, message_error_text, quantity_insert_rows);
	perform monitoring_service.change_of_status(table_id, interval_is_included, process_status);
	
	
	
	EXCEPTION WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS message_error_text = RETURNED_SQLSTATE + '' + PG_EXCEPTION_DETAIL;
	
	process_status = 'FAIL';
		
	perform monitoring_service.logger_load_target_table(table_id, message_error_text, quantity_insert_rows);
	perform monitoring_service.change_of_status(table_id, interval_is_included, process_status);

	end;
		
	
return process_status;
	
end;









$$
EXECUTE ON ANY;