BEGIN;
    SELECT COUNT(*) FROM guarded_query_accounts('is_active = TRUE');
    UPDATE bank_accounts SET is_active = FALSE WHERE id = 10;
COMMIT;