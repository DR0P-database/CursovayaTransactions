import psycopg2
import json
import select

DSN = "dbname=postgres user=postgres password=postgres host=localhost port=5432"


def main():
    # Подключение
    conn = psycopg2.connect(DSN)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()

    # Подписка на канал
    cur.execute("LISTEN conflict_events;")
    print("Listening on channel conflict_events ...")

    while True:
        # Ждём уведомление
        if select.select([conn], [], [], None) == ([], [], []):
            continue

        conn.poll()

        while conn.notifies:
            notify = conn.notifies.pop(0)
            payload = notify.payload
            print("NOTIFY:", payload)

            try:
                data = json.loads(payload)
            except Exception as e:
                print("Invalid JSON:", e)
                continue

            # Запись непосредственно в таблицу conflict_log
            insert_conflict(data)


def insert_conflict(data: dict):
    conn2 = psycopg2.connect(DSN)
    conn2.autocommit = True
    cur2 = conn2.cursor()

    cur2.execute(
        """
        INSERT INTO conflict_log (
            operation,
            table_name,
            conflict_type,
            old_balance,
            new_balance,
            old_on_duty,
            new_on_duty,
            message
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            data.get("operation"),
            data.get("table_name"),
            data.get("conflict_type", "UNKNOWN"),
            data.get("old_balance"),
            data.get("new_balance"),
            data.get("old_on_duty"),
            data.get("new_on_duty"),
            data.get("message"),
        ),
    )

    cur2.close()
    conn2.close()


if __name__ == "__main__":
    main()
