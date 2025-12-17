TRUNCATE bank_accounts;

INSERT INTO bank_accounts (holder, balance, is_active)
SELECT 
    'User ' || g,
    (random() * 10000)::int,
    TRUE
FROM generate_series(1, 100000) g;