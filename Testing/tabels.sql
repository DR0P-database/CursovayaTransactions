-- 1. Таблица для конфликтов
-- Банковские счета
CREATE TABLE bank_accounts (
    id SERIAL PRIMARY KEY,
    holder TEXT NOT NULL,
    balance NUMERIC(8,2) NOT NULL CHECK (balance >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

-- Таблица для логгирования
CREATE TABLE conflict_log (
    id            bigserial PRIMARY KEY,
    logged_at     timestamptz DEFAULT now() NOT NULL,
    operation     text NOT NULL,              -- INSERT / UPDATE / DELETE
    table_name    text NOT NULL,              -- bank_account / doctors
    conflict_type text NOT NULL DEFAULT 'SUCCESS',  -- SUCCESS / LOST_UPDATE / WRITE_SKEW и т.д.
    old_balance   numeric(8,2),              -- NULL при INSERT
    new_balance   numeric(8,2),              -- NULL при DELETE
    old_on_duty   boolean,                    -- NULL для bank_account
    new_on_duty   boolean,                    -- NULL для bank_account
    message       text                        -- кастомное сообщение
);

-- Начальные данные для банковских счетов
INSERT INTO bank_accounts (holder, balance) VALUES
('Иванов И.И.',       15000.00),
('Петров П.П.',       23000.00),
('Сидорова А.В.',     18700.00),
('Козлов В.С.',       9200.00),
('Морозова Е.Н.',     31100.00);

UPDATE bank_accounts SET is_active = FALSE WHERE id <> 1 and id <> 2;
UPDATE bank_accounts SET is_active = TRUE  WHERE id = 1 or id = 2;
