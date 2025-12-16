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
stmt_count = '''SELECT COUNT(*) FROM guarded_query_accounts('is_active = TRUE')'''
stmt_deact = '''UPDATE bank_accounts SET is_active = FALSE WHERE id = %s'''
def deactivator(id_, delay):
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level='READ COMMITTED', autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(stmt_count)
        count = cur.fetchone()[0]
        logger.info("T%s посчитала активных: %s", id_, count)
        if count > 1:
            time.sleep(delay)
            cur.execute(stmt_deact, (id_,))
            conn.commit()
            logger.info("T%s деактивировала id %s (rowcount: %s)", id_, id_, cur.rowcount)
        else:
            logger.info("T%s не деактивирует, т.к. count <=1", id_)
    except Exception as e:
        logger.error("T%s исключение: %s", id_, e)
        conn.rollback()
    finally:
        conn.close()
t1 = threading.Thread(target=deactivator, args=(1, 0.5))
t2 = threading.Thread(target=deactivator, args=(2, 2))
t1.start(); t2.start()
t1.join(); t2.join()