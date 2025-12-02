import psycopg2
import threading
import time
import sys
from datetime import datetime

DSN = "dbname=postgres user=postgres password=postgres host=localhost port=5432"

# Вспомогательная функция — одна транзакция пытается деактивировать счёт
def deactivate_account_thread(account_holder, thread_name, barrier):
    conn = None
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = False
        cur = conn.cursor()

        print(f"[{thread_name}] Стартую, жду синхронизации...")
        barrier.wait()  # синхронизируем старт обеих транзакций

        print(f"[{thread_name}] Начинаю транзакцию: деактивирую {account_holder}")

        # Это вызовет триггер trg_accounts_write_lock → попытается взять advisory lock
        cur.execute(
            "UPDATE bank_accounts SET is_active = FALSE WHERE holder = %s",
            (account_holder,)
        )

        # Имитируем небольшую "бизнес-логику" — задержка как в реальном Write Skew
        time.sleep(2)

        conn.commit()
        print(f"[{thread_name}] УСПЕШНО закоммитил! Счёт {account_holder} деактивирован.")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        print(f"[{thread_name}] ОТКАЗАНО: {e.pgerror.strip()}")
        print(f"[{thread_name}] Код ошибки: {e.pgcode}")

    finally:
        if conn:
            conn.close()

def test_write_skew_protection():
    # Подготовка: включаем обратно оба счёта
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("UPDATE bank_accounts SET is_active = TRUE WHERE is_active = FALSE")
    # cur.execute("DELETE FROM conflict_log WHERE conflict_type LIKE 'TRY_LOCK%' OR conflict_type = 'BUSINESS_RULE_VIOLATION'")
    conn.commit()
    conn.close()

    print("="*70)
    print("ЗАПУСК ТЕСТА: Защита от Write Skew через pg_try_advisory_xact_lock")
    print("="*70)

    # Синхронизация потоков
    barrier = threading.Barrier(2, timeout=10)

    t1 = threading.Thread(
        target=deactivate_account_thread,
        args=("Иванов И.И.", "Транзакция-1", barrier),
        daemon=True
    )
    t2 = threading.Thread(
        target=deactivate_account_thread,
        args=("Петров П.П.", "Транзакция-2", barrier),
        daemon=True
    )

    t1.start()
    t2.start()

    t1.join(timeout=15)
    t2.join(timeout=15)

    # Проверяем результат
    print("\n" + "="*70)
    print("РЕЗУЛЬТАТ ТЕСТА")
    print("="*70)

    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    cur.execute("SELECT holder, is_active FROM bank_accounts ORDER BY id")
    print("Текущее состояние счетов:")
    for row in cur.fetchall():
        print("  ", row)

    cur.execute("""
        SELECT logged_at, operation, conflict_type, message
        FROM conflict_log
        WHERE conflict_type = 'TRY_LOCK_FAILURE'
        ORDER BY id DESC LIMIT 1
    """)
    log = cur.fetchone()
    if log:
        print("\nВ conflict_log зафиксирован отказ из-за блокировки (это и есть защита от Write Skew):")
        print("  Время:", log[0])
        print("  Тип:", log[2])
        print("  Сообщение:", log[3])
    else:
        print("\nВНИМАНИЕ: Не найдено записи о TRY_LOCK_FAILURE — возможно, защита не сработала!")

    cur.execute("SELECT COUNT(*) FROM bank_accounts WHERE is_active = TRUE")
    active_count = cur.fetchone()[0]
    print(f"\nАктивных счетов осталось: {active_count} ← Должно быть 1!")

    if active_count == 1:
        print("WRITE SKEW УСПЕШНО ПРЕДОТВРАЩЁН!")
    else:
        print("ОШИБКА: Write Skew всё-таки произошёл!")

    conn.close()

if __name__ == "__main__":
    test_write_skew_protection()
