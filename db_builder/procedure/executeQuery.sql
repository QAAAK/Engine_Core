CREATE OR REPLACE FUNCTION db_builder.executequery(query text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	


			
declare	


	quantityRows int8 := 0;

	errorCode text;
	errorText text;
	messageErrorText text;

	status text := 'PASSED';


begin 
	
	begin
	
	
		execute query;
	
	
		perform db_builder.setscript(query);
	
	
		GET  diagnostics
		quantityRows = ROW_COUNT;
		
	
		exception when others then 
		
			get STACKED diagnostics 
		
			errorCode = RETURNED_SQLSTATE,
			errorText = MESSAGE_TEXT;
			messageErrorText = errorCode || ' ' || errorText;
		
			status = 'FAILED';
		
			perform db_builder.writingLogTable(query, status, quantityRows, messageErrorText);
			
			return 'OK';

	end;

	perform db_builder.writingLogTable(query, status, quantityRows, messageErrorText);	

return messageErrorText;

end;





$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.executequery(text) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION db_builder.executequery(text) TO public;
GRANT ALL ON FUNCTION db_builder.executequery(text) TO analyze_bi_owner;
