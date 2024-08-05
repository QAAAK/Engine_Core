CREATE OR REPLACE FUNCTION db_builder.updatetime(tablename text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	



			
declare	

currentDTTM timestamp := clock_timestamp() ;
nextDTTM timestamp;
transferDTTM text;


begin 
	
	
	select when_update  
	into transferDTTM
	from db_builder.information_tables_dds a
	where a.table_name = tablename;

	if transferDTTM = 'Ежедневно' 
		then 
			nextDTTM = currentDTTM + '1 day';
	elsif transferDTTM = 'Еженедельно' 
		then 
			nextDTTM = currentDTTM + '1 week';
	elsif transferDTTM = 'Ежемесячно' 
		then nextDTTM = currentDTTM + '1 month';	
	end if;

	update db_builder.information_tables_dds 
    set last_dttm = currentDTTM, next_dttm = nextDTTM
    where table_name = tablename;

	
	
end;







$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.updatetime(text) OWNER TO santalovdv;
GRANT ALL ON FUNCTION db_builder.updatetime(text) TO santalovdv;
