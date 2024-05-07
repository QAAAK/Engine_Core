CREATE OR REPLACE FUNCTION source_hive.create_external_table(
    in schema_name text,
    in table_name text,
    in text_in text,
    out text_out text
)
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
 AS
$$
begin
    text_in := replace(text_in, '`', '');
    text_in := replace(text_in , concat(schema_name, '.'), '');
    schema_name := replace(schema_name, 'prx_', '');
    schema_name := replace(schema_name, 'cap_', '');
    text_in := replace(text_in, ' tinyint', ' int2');
    text_in := replace(text_in, '''', '');
    text_in := replace(text_in, ' timestamp', ' timestamp');
    text_in := replace(text_in, ' string', ' text');
    text_in := replace(text_in, ' smallint', ' int2');
    text_in := replace(text_in, ' float', ' float4');
    text_in := replace(text_in, ' double', ' float8');
    text_in := replace(text_in, ' boolean', ' bool');
    text_in := replace(text_in, ' binary', ' bytea');
    text_in := replace(text_in, ' bigint', ' int8');
    text_in := replace(text_in, ' decimal', ' numeric');
    text_in := replace(text_in, 'CREATE EXTERNAL TABLE ', 'CREATE EXTERNAL TABLE source_hive.');
    text_in :=  concat(text_in, 'location');
    text_in := replace(text_in, '<схема>', schema_name);
    text_in := replace(text_in, '<таблица>', table_name);
    text_out:= text_in;
    execute text_out;
    RETURN;
END;
$$
EXECUTE ON ANY;