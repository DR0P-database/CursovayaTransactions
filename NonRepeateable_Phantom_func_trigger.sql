--================ Функция для любых SELECT ================--
CREATE OR REPLACE FUNCTION guarded_query(query text)
RETURNS SETOF jsonb AS $$
DECLARE
    tab record;
    lock_key bigint;
BEGIN
    -- 1. Находим все таблицы, упомянутые в запросе
    FOR tab IN
        SELECT tablename
        FROM pg_tables
        WHERE query ILIKE '%' || tablename || '%'
    LOOP
        lock_key := hashtext(tab.tablename);
        PERFORM pg_advisory_xact_lock_shared(lock_key);
    END LOOP;

    -- 2. Выполняем запрос под shared lock и возвращаем jsonb
    RETURN QUERY
        EXECUTE format('SELECT to_jsonb(t) FROM (%s) AS t', query);

END;
$$ LANGUAGE plpgsql;

-- Пример:
-- SELECT * FROM guarded_query('SELECT ... FROM bank_accounts JOIN ... ');

--================ Trigger на DML ================--
CREATE OR REPLACE FUNCTION guarded_write()
RETURNS TRIGGER AS $$
DECLARE
    lock_key bigint;
BEGIN
    lock_key := hashtext(TG_TABLE_NAME);
    PERFORM pg_advisory_xact_lock(lock_key);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER guarded_write_all
BEFORE INSERT OR UPDATE OR DELETE ON bank_accounts
FOR EACH STATEMENT EXECUTE FUNCTION guarded_write();

