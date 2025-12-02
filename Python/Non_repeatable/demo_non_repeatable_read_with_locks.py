# demo_non_repeatable_read_with_locks.py
import logging
import threading, time
import psycopg2


DSN = "dbname=postgres user=postgres password=postgres host=localhost port=5432"
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def reader():
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level='READ COMMITTED', autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        # используем SELECT через lock-функцию: она берёт shared lock на table_key = 1001
        stmt = 'id = 1'
        cur.execute("SELECT * FROM guarded_query_accounts(%s);", (stmt,))
        r1 = cur.fetchall()
        logger.info("T1 прочитала запись: %s", r1)
        time.sleep(2)   # держит транзакцию и shared lock
        cur.execute("SELECT * FROM guarded_query_accounts(%s);", (stmt,))
        r2 = cur.fetchall()
        logger.info("T1 прочитала запись: %s", r2)
        conn.commit()
    except Exception as e:
        logger.error("T1 исключение: %s", e)
        conn.rollback()
    finally:
        conn.close()

def writer():
    time.sleep(0.5)
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level='READ COMMITTED', autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        print("T2: пытается UPDATE (будет заблокирован!)")
        cur.execute("UPDATE bank_accounts SET balance = balance + 100 WHERE id=1;")
        conn.commit()
        logger.info("T2 сделала коммит но не измеенила прочитанную T1 запись(тк Return null)")
    except Exception as e:
        logger.error("Т2 исключение: %s", e)
        conn.rollback()
    finally:
        conn.close()

t1 = threading.Thread(target=reader)
t2 = threading.Thread(target=writer)
t1.start(); t2.start()
t1.join(); t2.join()