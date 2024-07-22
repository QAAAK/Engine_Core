CREATE OR REPLACE FUNCTION db_builder.createexternaltable(tablename text, connectiiondb text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

/*
 *  
 *  @author : santalovdv@mts.ru
 * 	
 * 	Процедура генерации DDL external table на основе существующей таблицы
 * 
 * 
 */
	

			
declare	


	columnName text = '';
	locationType text;
	externalTableName text;
	aboutComment text;
	returnQuery text = 'Такой таблицы нет';

	

begin
	
	
	externalTableName := lower(connectiionDB) || '_' || tableName;

	aboutComment := '-- Заменить <schema_name>.<table_name> на схему и таблицу как на источнике' || chr(10);

	locationType := 'LOCATION (''pxf://<schema_name>.<table_name>?PROFILE=hive&SERVER=rndDWHB2B )'' 
					 ON ALL FORMAT ''CUSTOM'' ( FORMATTER=''pxfwritable_import'' )
					  ENCODING ''UTF8'';';
	
		
	-- получаем  поля с типами данных
	select string_agg(column_name || ' ' || data_type   , ',' || chr(10)  order by ordinal_position )
	into columnName
	from information_schema.columns 
	where table_name = tableName;

	
	-- Собираем запрос

	if columnName != ''
		then 
		
			returnQuery = aboutComment || chr(10) || 'create external table dds.' || externalTableName || '(' || chr(10) ||
			columnName || ')' || chr(10) || locationType;
	
	end if;


			
	return returnQuery;
	
end;











$$
EXECUTE ON ANY;





