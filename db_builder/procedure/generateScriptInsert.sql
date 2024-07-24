CREATE OR REPLACE FUNCTION db_builder.generatescriptinsert(schemaname text, tablename text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
			
declare	

	cntRows bigint;
	schemaNameAndTableName text;
	insertQuery text;

	
begin
	
	schemaNameAndTableName = schemaName || '.' || tableName;
	
	insertQuery = 'insert into ' || schemaNameAndTableName || ' select * from ' || schemaName || '.<tableName>';
	
	return insertQuery;
	

end;




$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.generatescriptinsert(text, text) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION db_builder.generatescriptinsert(text, text) TO analyze_bi_owner;
