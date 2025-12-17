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
stmt_deact = '''UPDATE bank_accounts SET is_active = FALSE WHERE id = %s'''
def deactivator(id_, delay):
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level='READ COMMITTED', autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")



        time.sleep(delay)
        cur.execute(stmt_deact, (id_,))
        time.sleep(delay)
        conn.commit()
        logger.info("T%s деактивировала id %s", id_, id_)


    except Exception as e:
        logger.error("T%s исключение: %s", id_, e)
        conn.rollback()
    finally:
        conn.close()
t1 = threading.Thread(target=deactivator, args=(2400012, 2))
t2 = threading.Thread(target=deactivator, args=(2400011, 2))
t1.start(); t2.start()
t1.join(); t2.join()