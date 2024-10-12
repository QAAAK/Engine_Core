CREATE OR REPLACE FUNCTION db_builder.generatescriptinsert(tableid int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
/* Процедура - генератор DML запроса
 *  
 *  @author : santalovdv@mts.ru
 * 	
 * 
 * 
 */
	
	
			
declare	

	sourceTable text;
	targetTable text;
	typeLoad varchar(25);
	columnName varchar(25);
	insertQuery text;
	
begin
	
	select source_table, target_table, type_load
	into sourceTable targetTable, typeLoad
	from db_builder.information_source_and_target_connection isatc
	where id = tableId;

	if typeLoad = 'FULL'
		then 
			insertQuery = 'insert into ' || sourceTable || ' select * from ' || targetTable;
			
	elsif typeLoad = 'INCREMENT'
		then 
			select increment_column 
			into columnName
			from db_builder.information_source_and_target_connection isatc 
			where id = tableId;
		
			insertQuery = 'insert into ' || sourceTable || ' select * from ' || targetTable || ' where ' || columnName || '> (select max(' || columnName || ') from ' || sourceTable;
	
	else 
			insertQuery = 'No such table or id';
			
	end if;

		
return insertQuery;

end;





$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.generatescriptinsert(int8) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION db_builder.generatescriptinsert(int8) TO analyze_bi_owner;
