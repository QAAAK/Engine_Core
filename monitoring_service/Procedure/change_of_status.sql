CREATE OR REPLACE FUNCTION monitoring_service.change_of_status(table_id bigint, included_interval boolean, status text default null)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	
	

begin


	

	if included_interval = true and status = null 
		then 
			update monitoring_service.load_logger 
			set  last_status = 'PROGRESS'
			where id = table_id;

	else if included_interval = false and status = null
		then 
			update monitoring_service.load_logger 
			set  last_status = 'SUCCESFUL'
			where id = table_id;

	else if status isnotnull 
		then 
			update monitoring_service.load_logger
			set last_status = status
			where id = table_id;
	
	end if;
	

	
end;









$$
EXECUTE ON ANY;