import asyncio
import random
import time
import asyncpg


DSN = "postgresql://postgres:dimas_sport@localhost:5432/Transactions"

MAX_RETRIES = 7
N_ACCOUNTS = 50


async def try_deactivate_account(conn, account_id):
    attempt = 1

    while attempt <= MAX_RETRIES:
        await conn.execute("BEGIN;")
        row = await conn.fetchrow(
            "SELECT * FROM bank_accounts WHERE id = $1",
            account_id
        )
        await asyncio.sleep(random.uniform(0.1, 0.3))
        result = await conn.execute("""
            UPDATE bank_accounts
            SET is_active = false
            WHERE id = $1
        """, account_id)

        if result == "UPDATE 1":
            await conn.execute("COMMIT;")
            return "success", attempt

        elif result == "UPDATE 0":
            await conn.execute("COMMIT;")
            attempt += 1
            if attempt <= MAX_RETRIES:
                await asyncio.sleep(0.5)
            continue

    await conn.execute("COMMIT;")
    return "failed_max_retries", MAX_RETRIES

async def simulate_serializable_write(conn, account_id):
    attempt = 1

    while attempt <= MAX_RETRIES:
        await conn.execute("BEGIN;")
        
        row = await conn.fetchrow(
            "SELECT * FROM test_serializable WHERE id = $1",
            account_id
        )
        await asyncio.sleep(random.uniform(0.1, 0.3))
        try:
            result = await conn.execute("""
                UPDATE test_serializable
                SET is_active = false
                WHERE id = $1
            """, account_id)

            if result == "UPDATE 1":
                await conn.execute("COMMIT;")
                return "success", attempt
            
        except asyncpg.exceptions.SerializationError as e:
            await conn.execute("ROLLBACK;")
            attempt += 1
            
            if attempt <= MAX_RETRIES:
                await asyncio.sleep(0.5)
            continue
    return "failed_max_retries", MAX_RETRIES

async def worker(worker_id):
    conn = await asyncpg.connect(DSN)
    account_id = worker_id
    status, attempts = await try_deactivate_account(conn, account_id)
    await conn.close()
    print(f"Worker {worker_id:3d} | id {account_id} | {status} | попыток: {attempts}")

async def serializable_worker(worker_id):
    conn = await asyncpg.connect(DSN)
    account_id = worker_id
    status, attempts = await simulate_serializable_write(conn, account_id)
    await conn.close()
    print(f"Serializable Worker {worker_id} | id {account_id} | {status} | попыток: {attempts}")


async def main():
    print(f"\n\n=== ТЕСТ: Write Skew ===")
    print(f"Запуск {N_ACCOUNTS} параллельных транзакций...")

    start_total = time.time()

    tasks = [asyncio.create_task(worker(i)) for i in range(1, N_ACCOUNTS + 1)]
    await asyncio.gather(*tasks)

    total_time = time.time() - start_total
    print(f"\nВсего времени: {total_time:.2f} сек")

    print(f"\n\n=== ТЕСТ 2: SERIALIZABLE isolation level ===")
    print(f"Запуск {N_ACCOUNTS} параллельных транзакций...")
    
    start_total = time.time()
    
    serial_tasks = [asyncio.create_task(serializable_worker(i)) 
                    for i in range(1, N_ACCOUNTS + 1)]
    
    await asyncio.gather(*serial_tasks)
    
    total_time = time.time() - start_total
    
    print(f"\nВсего времени: {total_time:.2f} сек")



if __name__ == "__main__":
    asyncio.run(main())
