CREATE OR REPLACE FUNCTION db_builder.logwritingfunc(id_table int8, function_name_table text, error_message_table text, dtl_time timestamp)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

/* Процедура логгирования  
 *  
 *  @author : santalovdv@mts.ru
 * 	
 * 
 * 
 */
	

DECLARE
                messageStatus boolean := 0;

BEGIN

                if error_message_table is null

                               then

                                               messageStatus = 1;
                end if;

                if id_table is not null

                               then

                                               insert into db_builder.func_log  
                                               (function_name,status,error_message,date_end) 
                                               VALUES(
                                               function_name_table, messageStatus, error_message_table, dtl_time);
                                               
                               else
                                               insert into db_builder.func_log  
                                               (function_name,status,error_message,date_start) 
                                               VALUES(
                                               function_name_table, messageStatus, error_message_table, dtl_time);

                end if;

END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.logwritingfunc(int8, text, text, timestamp) OWNER TO kirsanovni;
GRANT ALL ON FUNCTION db_builder.logwritingfunc(int8, text, text, timestamp) TO kirsanovni;
