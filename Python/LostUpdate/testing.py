import asyncio
import random
import time
import asyncpg

DSN = "postgresql://postgres:dimas_sport@localhost:5432/Transactions"
ACCOUNT_IDS = [1, 2, 3]
N_WORKERS = 20
MAX_RETRIES = 7


async def simulate_user_tx(conn, account_id):
    attempt = 1
    
    while attempt <= MAX_RETRIES:
        await conn.execute("BEGIN;")
        
        row = await conn.fetchrow("SELECT * FROM get_account($1)", account_id)
            
        await asyncio.sleep(random.uniform(0.05, 0.35))
        
        new_balance = row['balance'] + random.randint(-300, 1000)
        
        result = await conn.execute("""
            UPDATE bank_accounts
               SET balance = $1
             WHERE id = $2
        """, new_balance, account_id)
        
        if result == "UPDATE 1":
            await conn.execute("COMMIT;")
            return "success", attempt
        
        elif result == "UPDATE 0":
            await conn.execute("COMMIT;")
            attempt += 1
        else:
            await conn.execute("ROLLBACK;")
            return 'Fail', attempt
        
        if attempt <= MAX_RETRIES:
            await asyncio.sleep(0.1)
    
    await conn.execute("COMMIT;")
    return "failed_max_retries", MAX_RETRIES


async def worker(worker_id):
    conn = await asyncpg.connect(DSN)
    acc_id = random.choice(ACCOUNT_IDS)
    status, attempts = await simulate_user_tx(conn, acc_id)
    await conn.close()
    print(f"Worker {worker_id:2d} | id {acc_id:3d} | {status:10} | попыток: {attempts}")


SERIALIZABLE_N_WORKERS = 20  # можно сделать столько же или другое количество
SERIALIZABLE_MAX_RETRIES = 7
SERIALIZABLE_ACCOUNT_IDS = [1, 2, 3]


async def simulate_serializable_tx(conn, account_id, worker_id):
    attempt = 1
    
    while attempt <= SERIALIZABLE_MAX_RETRIES:
        try:
            # Начинаем транзакцию с нужным уровнем изоляции
            await conn.execute("BEGIN;")
            await conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            
            # Читаем текущее состояние
            row = await conn.fetchrow(
                "SELECT * FROM test_serializable WHERE id = $1", 
                account_id
            )
        
            # Имитация "работы" пользователя
            await asyncio.sleep(random.uniform(0.05, 0.35))
            
            new_balance = row['balance'] + random.randint(-300, 1000)
            
            # Пытаемся обновить
            result = await conn.execute(
                """
                UPDATE test_serializable
                   SET balance = $1
                 WHERE id = $2
                """,
                new_balance, account_id
            )
            
            
            await conn.execute("COMMIT;")
    
            return "success", attempt
        
        except asyncpg.exceptions.SerializationError as e:
            # Самая ожидаемая ошибка в serializable при конфликте
            await conn.execute("ROLLBACK;")
            attempt += 1
            
            if attempt <= SERIALIZABLE_MAX_RETRIES:
                await asyncio.sleep(0.1)
            continue
    return "failed_max_retries", SERIALIZABLE_MAX_RETRIES


async def serializable_worker(worker_id):
    conn = await asyncpg.connect(DSN)
    acc_id = random.choice(SERIALIZABLE_ACCOUNT_IDS)
        
    status, attempts = await simulate_serializable_tx(conn, acc_id, worker_id)
    await conn.close()
    print(f"[SERIALIZABLE] Worker {worker_id:2d} | id {acc_id:2d} | {status:18} | попыток: {attempts}")

async def main():
    print(f"\nЗапуск {N_WORKERS} параллельных транзакций...")
    
    start_time = time.perf_counter()
    
    tasks = [asyncio.create_task(worker(i)) for i in range(1, N_WORKERS + 1)]
    
    # ждём завершения всех
    await asyncio.gather(*tasks)
    
    total_time = time.perf_counter() - start_time
    
    print(f"\nВсе {N_WORKERS} транзакций завершили работу за {total_time:.2f} секунд")
    print(f"Среднее время на одну транзакцию (примерно): {total_time / N_WORKERS:.2f} с")

    # Даём немного "отдохнуть" базе
    await asyncio.sleep(1.0)
    
    
    # ── Второй тест — чистый SERIALIZABLE ──────────────────────────
    print(f"\n\n=== ТЕСТ 2: SERIALIZABLE isolation level ===")
    print(f"Запуск {SERIALIZABLE_N_WORKERS} параллельных транзакций...")
    
    start_time = time.perf_counter()
    
    serial_tasks = [asyncio.create_task(serializable_worker(i)) 
                    for i in range(1, SERIALIZABLE_N_WORKERS + 1)]
    
    await asyncio.gather(*serial_tasks)
    
    total_time_serial = time.perf_counter() - start_time
    
    print(f"\nВсе {SERIALIZABLE_N_WORKERS} транзакций (serializable) завершили работу за {total_time_serial:.2f} секунд")
    print(f"Среднее время на одну транзакцию (примерно): {total_time_serial / SERIALIZABLE_N_WORKERS:.2f} с")


if __name__ == "__main__":
    asyncio.run(main())
