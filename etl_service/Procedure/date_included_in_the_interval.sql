the executable procedure


CREATE OR REPLACE FUNCTION etl_service.date_included_in_the_interval( table_id text )
	RETURNS boolean
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	
	true_or_false boolean := false;
	
	interval_time date;
	last_date time;
	

begin


	select launch_interval, launch_date
		into interval_time, last_date
		from etl_service.tasks
	where md_id = table_id;
	
	
	
	if clock_timestamp() - last_date >= interval_time 
		then 
			true_or_false := true;
			return true_or_false;
	end if;
	
	
	
return true_or_false;
	
end;









$$
EXECUTE ON ANY;