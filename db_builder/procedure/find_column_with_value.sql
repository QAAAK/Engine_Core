CREATE OR REPLACE FUNCTION db_builder.find_column_with_value(table_name text, search_value text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    column_names TEXT;
    column_record RECORD;
    query TEXT;
BEGIN
    -- Перебираем все колонки таблицы
    FOR column_record IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = table_name
    LOOP
        -- Формируем динамический SQL-запрос для поиска значения в каждой колонке
        query := format('SELECT 1 FROM %I WHERE %I::TEXT = %L LIMIT 1', table_name, column_record.column_name, search_value);
        
        -- Выполняем запрос
        EXECUTE query INTO column_names;

        -- Если найдено совпадение, возвращаем имя колонки
        IF column_names IS NOT NULL THEN
            RETURN column_record.column_name;
        END IF;
    END LOOP;

    -- Если ничего не найдено, возвращаем NULL
    RETURN NULL;
END;

$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION db_builder.find_column_with_value(text, text) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION db_builder.find_column_with_value(text, text) TO analyze_bi_owner;
