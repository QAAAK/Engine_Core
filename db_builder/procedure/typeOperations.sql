CREATE OR REPLACE FUNCTION db_builder.typeoperations(query text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	/*  Процедура, которая определяет тип операции
 *  
 *  @author : santalovdv@mts.ru
 * 	
 * 
 * 
 */
	


			
declare	

operation text;


begin 
		
	operation = upper(substring(query, 1, 6));
	
return operation;

end;



$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.typeoperations(text) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION db_builder.typeoperations(text) TO public;
GRANT ALL ON FUNCTION db_builder.typeoperations(text) TO analyze_bi_owner;
