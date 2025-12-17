BEGIN;
UPDATE bank_accounts SET balance = balance + 1 WHERE is_active = TRUE;
COMMIT;