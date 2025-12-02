import psycopg2
from psycopg2 import extensions
import sys

DSN = "dbname=postgres user=postgres password=postgres host=localhost port=5432"

def test_last_active_account_cannot_be_deactivated():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False  # будем управлять транзакциями вручную
    cur = conn.cursor()

    try:
        print("=== Тест: нельзя деактивировать последний активный счёт ===")

        # 1. Убедимся, что сейчас ровно 2 активных счёта
        cur.execute("SELECT id, holder, balance, is_active FROM bank_accounts ORDER BY id")
        rows = cur.fetchall()
        print("Текущие счета:")
        for r in rows:
            print(r)

        # 2. Деактивируем первый счёт — должно пройти
        print("\nПытаемся деактивировать первый счёт (Иванов)...")
        cur.execute(
            "UPDATE bank_accounts SET is_active = FALSE WHERE holder = 'Иванов И.И.'"
        )
        conn.commit()
        print("Успешно деактивирован")

        # 3. Пытаемся деактивировать последний активный (Петров) — ДОЛЖНО УПАСТЬ
        print("\nПытаемся деактивировать последний активный счёт (Петров)...")
        try:
            cur.execute(
                "UPDATE bank_accounts SET is_active = FALSE WHERE holder = 'Петров П.П.'"
            )
            conn.commit()
            print("ОШИБКА: транзакция прошла, а не должна была!")
            sys.exit(1)
        except psycopg2.Error as e:
            conn.rollback()
            print("Ожидаемое исключение получено:")
            print(e.pgerror.strip())
            print("Код ошибки:", e.pgcode)

    finally:
        conn.rollback()  # на всякий случай
        cur.close()
        conn.close()

if __name__ == "__main__":
    test_last_active_account_cannot_be_deactivated()