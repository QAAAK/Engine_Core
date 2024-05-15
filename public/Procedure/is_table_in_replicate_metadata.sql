create or replace function public.is_table_in_replicate_metadata(tbl_name text)
	RETURNS int2
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare	

	
	is_table_in_metadata boolean;
	true_or_false int2 := 0;
	


begin
	
	
	select 
	into is_table_in_metadata
	exists (select target_table from etl_service.replicate_information where target_table = tbl_name);

	if is_table_in_metadata = true
		then 
			true_or_false = 1;
			return true_or_false;
	end if;
		
return true_or_false;	
	
end;





$$
EXECUTE ON ANY;