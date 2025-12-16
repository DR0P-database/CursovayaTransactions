--===========Update Lost===========--
-- TESTING native ...

-- Триггерная функция для предотвращения Update Lost
CREATE OR REPLACE FUNCTION prevent_lost_update()
RETURNS trigger AS $$
DECLARE
    saved_xmin xid := current_setting('myapp.xmin_' || NEW.id, true)::xid;
	current_xmin xid;
BEGIN
	SELECT xmin INTO current_xmin
    FROM bank_accounts
    WHERE id = NEW.id;
    -- Ключевое сравнение: версия при чтении ≠ текущая версия строки
    IF saved_xmin IS NOT NULL AND saved_xmin <> current_xmin THEN
        -- Логируем конфликт
		INSERT INTO conflict_log (
            operation, table_name, conflict_type,
            old_balance, new_balance, message
        ) VALUES (
            TG_OP,
            TG_TABLE_NAME,
            'LOST_UPDATE',
            OLD.balance,
            NEW.balance,
            format('Lost Update на счёте id=%s: ожидался xmin=%s, текущий xmin=%s',
                   NEW.id, saved_xmin, current_xmin)
        );

		-- выброс ошибки
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для Lost Update
CREATE TRIGGER trg_prevent_lost_update
	BEFORE UPDATE ON bank_accounts
	FOR EACH ROW
	EXECUTE FUNCTION prevent_lost_update();