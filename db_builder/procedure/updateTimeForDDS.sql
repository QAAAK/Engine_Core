DO $$
DECLARE

cur refcursor;
tableName text;
dttm timestamp;
updateTime text;

BEGIN

	
	open cur for execute 'select table_name from db_builder.information_tables_dds';
loop
	    fetch cur into tableName;
	   
	   	select when_update, next_dttm from db_builder.information_tables_dds itd 
	   	into updateTime, dttm
	   	where table_name = tableName;
	    
	    if updateTime == 'Ежедневно' 
	    	then 
	    		if dttm = clock_timestamp() 
	    			then 
	    				update db_builder.information_tables_dds set last_dttm = last_dttm  + '1 day' and next_dttm = next_dttm + '1 day';
	    			
	    else if updateTime == 'Еженедельно'
	    	then 
	    		if dttm = clock_timestamp() 
	    			then 
	    				update db_builder.information_tables_dds set last_dttm = last_dttm  + '7 day' and next_dttm = next_dttm + '7 day';
	    			
	    			
	    else if updateTime == 'Ежемесячно'
	    	then 
	    		if dttm = clock_timestamp() 
	    			then 
	    				update db_builder.information_tables_dds set last_dttm = last_dttm  + '1 month' and next_dttm = next_dttm + '1 month';
	   
	    end if;
	   
	   
end loop;

	
	
	
END $$;
