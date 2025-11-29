# demo_phantom_with_locks.py
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

stmt = '''SELECT COUNT(*) FROM bank_accounts WHERE balance >= 10000'''


def reader():
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level='READ COMMITTED', autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        # Берём shared lock и считаем
        cur.execute("SELECT * FROM guarded_query(%s);", (stmt,))
        c1 = cur.fetchone()[0]
        logger.info("T1 счет в первый раз: %s", c1)
        time.sleep(2)
        cur.execute("SELECT * FROM guarded_query(%s);", (stmt,))
        c2 = cur.fetchone()[0]
        logger.info("T1 счет во второй раз: %s", c2)
        conn.commit()
    except Exception as e:
        logger.info("T1 exception: %s", e)
        conn.rollback()
    finally:
        conn.close()

def inserter():
    time.sleep(0.5)
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level='READ COMMITTED', autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        logger.info("Вставка: пытается вставить новую запись и блокируется (ждёт коммита T1)")
        cur.execute("INSERT INTO bank_accounts(holder,balance) VALUES ('Новый Клиент', 15000);")
        conn.commit()
        logger.info("T2 сделала коммит")
    except Exception as e:
        logger.error("T2 exception: %s", e)
        conn.rollback()
    finally:
        conn.close()

t1 = threading.Thread(target=reader)
t2 = threading.Thread(target=inserter)
t1.start(); t2.start()
t1.join(); t2.join()

