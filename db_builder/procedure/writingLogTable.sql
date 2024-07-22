CREATE OR REPLACE FUNCTION db_builder.writinglogtable(query text, statusquery text, cntrows int8, messagetext text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	


			
declare	

operationQuery text;
tableName text;


begin 
		
		operationQuery := db_builder.typeOperations(query);
		tableName := db_builder.getTableName(query);
	
	
		INSERT INTO db_builder.log_dml_table (table_name, status, operation, quantity_rows, message_text, dttm)
		VALUES(tableName, statusQuery , operationQuery , cntRows, messageText, clock_timestamp());

end;



$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.writinglogtable(text, text, int8, text) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION db_builder.writinglogtable(text, text, int8, text) TO analyze_bi_owner;
