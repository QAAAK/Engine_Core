CREATE OR REPLACE FUNCTION public.determine_loadtype (table_schema text, table_name text)
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

			
declare	

	cnt_rows bigint;
	size_table text;

	type_load int2 := 1;

begin
	
	with t as (
	SELECT schemaname,
       C.relname AS tbl_name,
       pg_size_pretty (pg_relation_size(C.oid)) as size,
       n_live_tup as quantity_rows
	   FROM pg_class C
    LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace)
    LEFT JOIN pg_stat_user_tables A ON C.relname = A.relname
    WHERE nspname NOT IN ('pg_catalog', 'information_schema')
       AND C.relkind <> 'i'
       AND nspname !~ '^pg_toast' )
       
    select size, quantity_rows 
    	into size_table, cnt_rows 
    	from t
    where 1=1
    	and schemaname = table_schema
    	and tbl_name = table_name;
    
    
    
    if cnt_rows > 90000000 
    	then
	    	type_load = 2;
	    	return type_load;
    end if;
   
   
   return type_load;
    
   

	
end;









$$
EXECUTE ON ANY;