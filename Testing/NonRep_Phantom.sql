--============================= WriteSkew =============================
--TESTING native ...

--============================= Non_repeatable + Phantom=============================
--TESTING native ...

CREATE OR REPLACE FUNCTION guarded_query_accounts(condition text)
RETURNS SETOF bank_accounts AS $$
DECLARE
    lock_key bigint := hashtext('bank_accounts');
    lock_ok boolean;
    full_query text;
BEGIN
    lock_ok := pg_try_advisory_xact_lock_shared(lock_key);

    IF NOT lock_ok THEN
        INSERT INTO conflict_log(
            operation, table_name, conflict_type, message
        ) VALUES (
            'SELECT', 'bank_accounts', 'TRY_LOCK_FAILURE_ON_SELECT',
            'Попытка блокировки не удалась: источник занят'
        );
        RETURN;
    END IF;

    full_query := format('SELECT * FROM bank_accounts WHERE %s', condition);
    RETURN QUERY EXECUTE full_query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION accounts_guarded_write()
RETURNS TRIGGER AS $$
DECLARE
    lock_key bigint := hashtext('bank_accounts');  -- уникальный ключ для advisory lock
    lock_ok boolean;
BEGIN
    -- пытаемся взять блокировку на уровне транзакции
    lock_ok := pg_try_advisory_xact_lock(lock_key);

    IF NOT lock_ok THEN
        -- другой процесс держит lock, логируем конфликт
        INSERT INTO conflict_log(
            operation, table_name, conflict_type, message,
            old_balance, new_balance
        ) VALUES (
            TG_OP,
            TG_TABLE_NAME,
            'TRY_LOCK_FAILURE',
            format(
                'Не удалось выполнить %s в таблице %s: advisory lock %s уже занят другой транзакцией',
                TG_OP,
                TG_TABLE_NAME,
                lock_key
            ),
            OLD.balance,
            NEW.balance
        );

        RETURN NULL;
    END IF;

    -- lock получен, продолжаем операцию
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для Non-repeatable Read и Phantom Read и Anomaly / Write Skew
CREATE or REPLACE TRIGGER trg_accounts_write_lock
    BEFORE INSERT OR UPDATE OR DELETE ON bank_accounts
    FOR EACH ROW
    EXECUTE FUNCTION accounts_guarded_write();