CREATE OR REPLACE FUNCTION db_builder.gettablename(query text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	


			
declare	

	tableName text;

begin 
	
	tableName = SPLIT_PART(query, ' ', 3);
	
return tableName;

end;







$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.gettablename(text) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION db_builder.gettablename(text) TO analyze_bi_owner;
