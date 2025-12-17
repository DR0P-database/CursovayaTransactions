import psycopg2
from concurrent.futures import ThreadPoolExecutor
import time

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}










SQL_TRANSACTION = """
BEGIN ISOLATION LEVEL SERIALIZABLE;
    SELECT COUNT(*) FROM bank_accounts WHERE is_active = TRUE;
    UPDATE bank_accounts SET is_active = FALSE WHERE id = 10;
"""

def run_transaction(client_id):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(SQL_TRANSACTION)

        conn.commit()
        cur.close()
        conn.close()

        return f"Client {client_id}: OK"

    except Exception as e:
        return f"Client {client_id}: ERROR -> {e}"

if __name__ == "__main__":
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=50) as executor:
        results = list(executor.map(run_transaction, range(50)))

    end_time = time.time()

    for r in results:
        print(r)

    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")
