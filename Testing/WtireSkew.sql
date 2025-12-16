--============================= WriteSkew =============================
UPDATE bank_accounts SET is_active = FALSE WHERE id <> 1 and id <> 2;
UPDATE bank_accounts SET is_active = TRUE  WHERE id = 1 or id = 2;
--TESTING with lock ...

-- Правило
CREATE OR REPLACE FUNCTION accounts_business_rule_check()
RETURNS TRIGGER AS $$
DECLARE
    cnt_active int;
BEGIN
    /*
     * Если мы:
     *  - удаляем активный счёт
     *  - или деактивируем активный счёт
     * то проверяем, что останется хотя бы один активный
     */

    IF (TG_OP = 'DELETE' AND OLD.is_active = TRUE)
       OR
       (TG_OP = 'UPDATE'
        AND OLD.is_active = TRUE
        AND NEW.is_active = FALSE) THEN

        SELECT COUNT(*) INTO cnt_active
        FROM bank_accounts
        WHERE is_active = TRUE
          AND id <> OLD.id;

        IF cnt_active = 0 THEN
            RAISE EXCEPTION
                'Нарушение бизнес-правила: нельзя удалить или деактивировать последний активный счёт (id=%)',
                OLD.id;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггеры для Бизнес правила
CREATE TRIGGER trg_accounts_business_update
BEFORE UPDATE ON bank_accounts
FOR EACH ROW
EXECUTE FUNCTION accounts_business_rule_check();

CREATE TRIGGER trg_accounts_business_delete
BEFORE DELETE ON bank_accounts
FOR EACH ROW
EXECUTE FUNCTION accounts_business_rule_check();

UPDATE bank_accounts SET is_active = FALSE WHERE id <> 1 and id <> 2;
UPDATE bank_accounts SET is_active = TRUE  WHERE id = 1 or id = 2;
--TESTING with lock + busines rule
