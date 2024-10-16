CREATE OR REPLACE FUNCTION db_builder.setscript(query text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
	
	

/* Логгирование
 *  
 *  @author : santalovdv@mts.ru
 * 	
 * 
 * 
 */
	

			
declare

	tableName text;
	sourceTableName text;

begin
	
	
	tableName = db_builder.gettablename(query);
	
	select source_table
	into sourceTableName
	from db_builder.information_source_and_target_connection isatc 
	where source_table like '%' || tableName;
	

	update db_builder.information_source_and_target_connection 
	set script = query, last_update = clock_timestamp()
	where source_table = sourceTableName;


	perform db_builder.updatetime(tableName);


end;
















$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.setscript(text) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION db_builder.setscript(text) TO public;
GRANT ALL ON FUNCTION db_builder.setscript(text) TO analyze_bi_owner;
