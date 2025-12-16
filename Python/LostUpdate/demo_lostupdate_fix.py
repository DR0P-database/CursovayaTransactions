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
stmt_read = '''SELECT * FROM get_account(%s)'''
stmt_update = '''UPDATE bank_accounts SET balance = %s WHERE id = 1'''
def updater(delay):
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level='READ COMMITTED', autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(stmt_read, (1,))
        row = cur.fetchone()
        balance = row[2]
        logger.info("T%s прочитала баланс: %s", delay, balance)
        time.sleep(delay)
        new_balance = balance + 100
        cur.execute(stmt_update, (new_balance,))
        logger.info("T%s обновила баланс на: %s (rowcount: %s)", delay, new_balance, cur.rowcount)
        conn.commit()
    except Exception as e:
        logger.error("T%s исключение: %s", delay, e)
        conn.rollback()
    finally:
        conn.close()

print('============Исправляем потерю обновления============')
t1 = threading.Thread(target=updater, args=(2,))
t2 = threading.Thread(target=updater, args=(0.5,))
t1.start(); t2.start()
t1.join(); t2.join()