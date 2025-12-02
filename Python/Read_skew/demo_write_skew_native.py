import threading
import time
import psycopg2
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

DSN = "dbname=postgres user=postgres password=postgres host=localhost port=5432"

def worker(name):
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")

        cur.execute("SELECT id FROM bank_accounts WHERE is_active = TRUE;")
        rows = cur.fetchall()

        logger.info(f"{name} read active bank_accounts: {rows}")

        time.sleep(1)

        cur.execute(
            "UPDATE bank_accounts SET is_active = FALSE WHERE id = %s;",
            (rows[0][0],)
        )

        conn.commit()
        logger.info(f"{name} COMMIT")
    except Exception as e:
        logger.error(f"{name} ERROR: {e}")
        conn.rollback()
    finally:
        conn.close()


t1 = threading.Thread(target=worker, args=("T1",))
t2 = threading.Thread(target=worker, args=("T2",))

t1.start()
t2.start()
t1.join()
t2.join()
