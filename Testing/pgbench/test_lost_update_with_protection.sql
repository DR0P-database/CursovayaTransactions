BEGIN;
    SELECT * FROM bank_accounts;
    UPDATE bank_accounts
    SET balance = balance + 10
    WHERE id = 1;
COMMIT;