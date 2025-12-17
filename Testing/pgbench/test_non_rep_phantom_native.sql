BEGIN ISOLATION LEVEL SERIALIZABLE;
    SELECT COUNT(*) FROM bank_accounts WHERE is_active = TRUE;
    UPDATE bank_accounts SET is_active = FALSE WHERE id = 10;
COMMIT;