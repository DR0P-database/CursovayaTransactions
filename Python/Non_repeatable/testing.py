import asyncio
import random
import time
import asyncpg

DSN = "postgresql://postgres:dimas_sport@localhost:5432/Transactions"
ACCOUNT_IDS = [1, 2, 3]
N_READERS = 10
N_WRITERS = 20
READER_SLEEP = 1.0


async def simulate_reader_tx(conn):
    attempt = 1
    max_attempts = 7
    while attempt <= max_attempts:
        await conn.execute("BEGIN;")
        
        sum1_row = await conn.fetchrow("SELECT sum(balance) FROM guarded_query_accounts('true')")
        sum1 = sum1_row[0]
        if sum1 == 'null':
            await conn.execute("COMMIT;")
            attempt += 1
            if attempt <= max_attempts:
                await asyncio.sleep(1)
            continue

        await asyncio.sleep(READER_SLEEP)

        sum2_row = await conn.fetchrow("SELECT sum(balance) FROM bank_accounts")
        sum2 = sum2_row[0]
        
        await conn.execute("COMMIT;")

        if sum1 == sum2:
            return "no_anomaly", sum1, sum2, attempt
        else:
            return "non_repeatable_read_detected", sum1, sum2, attempt
    
    return "failed_max_attempts", None, None, max_attempts
        
async def simulate_serializable_reader(conn):
    await conn.execute("BEGIN;")
    await conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    sum1_row = await conn.fetchrow("SELECT sum(balance) FROM test_serializable")
    sum1 = sum1_row[0]

    await asyncio.sleep(READER_SLEEP)

    sum2_row = await conn.fetchrow("SELECT sum(balance) FROM test_serializable")
    sum2 = sum1_row[0]

    await conn.execute("COMMIT;")

    if sum1 == sum2:
        return "no_anomaly", sum1, sum2
    else:
        return "non_repeatable_read_detected", sum1, sum2

async def simulate_writer_tx(conn, account_id):
    attempt = 1
    max_attempts = 7
    
    while attempt <= max_attempts:
        await conn.execute("BEGIN;")
        price = random.randint(1000, 10000)
        result = await conn.execute(
            "UPDATE bank_accounts SET balance = $1 WHERE id = $2",
            price, account_id
        )
            
        if result == "UPDATE 1":
            await conn.execute("COMMIT;")
            return "update_success", attempt, price
            
        elif result == 'UPDATE 0':
            await conn.execute("COMMIT;")
            attempt += 1
        else:
            await conn.execute("ROLLBACK;")
            return 'Fail', attempt, price
            
        if attempt <= max_attempts:
                await asyncio.sleep(1.5)
        
    await conn.execute("COMMIT;")
    return "failed_max_attempts", max_attempts, price

async def simulate_writer_sirealizable(conn, account_id):
    await conn.execute("BEGIN;")
    price = random.randint(1000, 10000)
    result = await conn.execute(
            "UPDATE test_serializable SET balance = $1 WHERE id = $2",
            price, account_id
        )
    if result == "UPDATE 1":
            await conn.execute("COMMIT;")
            return "update_success", price

async def reader_worker(reader_id):
    conn = await asyncpg.connect(DSN)

    status, sum1, sum2, attempts = await simulate_reader_tx(conn)
    print(f"Reader {reader_id:2d} | {status:10} | {sum1:5} | {sum2:5} | {attempts}")

    await conn.close()


async def serializable_reader_worker(reader_id):
    conn = await asyncpg.connect(DSN)
    status, sum1, sum2 = await simulate_serializable_reader(conn)
    print(f"Serial Reader {reader_id} | {status} | {sum1} | {sum2}")
    await conn.close()

async def writer_worker(worker_id):
    conn = await asyncpg.connect(DSN)
    
    status, attempts, price = await simulate_writer_tx(conn, random.choice(ACCOUNT_IDS))
    print(f"Writer {worker_id:2d} | {status:10} | price:  {price} | attempts:  {attempts}")

    await conn.close()

async def serializable_writer_worker(worker_id):
    conn = await asyncpg.connect(DSN)
    account_id = random.choice(ACCOUNT_IDS)
    status, price = await simulate_writer_sirealizable(conn, account_id)
    print(f"Serial Writer {worker_id:2d} | {status} | price: {price}")
    await conn.close()

async def main():
    # print(f"\nТест Non-Repeatable Read: {N_READERS} читателей + {N_WRITERS} писателей")
    # print("Сценарий: половина читателей → все писатели → вторая половина читателей")
    # print("─" * 60)

    # start_time = time.perf_counter()

    # half_readers = N_READERS // 2

    # early_reader_tasks = [
    #     asyncio.create_task(reader_worker(i))
    #     for i in range(1, half_readers + 1)
    # ]
    # writer_tasks = [
    #     asyncio.create_task(writer_worker(i))
    #     for i in range(1, N_WRITERS + 1)
    # ]
    # await asyncio.sleep(3.0)  # ← здесь основная задержка, подбирай под себя

    # late_reader_tasks = [
    #     asyncio.create_task(reader_worker(i))
    #     for i in range(half_readers + 1, N_READERS + 1)
    # ]

    # # Ждём завершения всего
    # all_tasks = early_reader_tasks + writer_tasks + late_reader_tasks
    # await asyncio.gather(*all_tasks)

    # total_time = time.perf_counter() - start_time
    # print("─" * 60)
    # print(f"Все транзакции завершили работу за {total_time:.2f} секунд")


    print(f"\nТест SERIALIZABLE: {N_READERS} читателей + {N_WRITERS} писателей")
    print("Сценарий: половина читателей → все писатели → вторая половина читателей")
    print("─" * 70)

    start_time = time.perf_counter()

    half_readers = N_READERS // 2

    early_reader_tasks = [
        asyncio.create_task(serializable_reader_worker(i))
        for i in range(1, half_readers + 1)
    ]

    await asyncio.sleep(1.0)

    writer_tasks = [
        asyncio.create_task(serializable_writer_worker(i))
        for i in range(1, N_WRITERS + 1)
    ]

    await asyncio.sleep(1.0)
    late_reader_tasks = [
        asyncio.create_task(serializable_reader_worker(i))
        for i in range(half_readers + 1, N_READERS + 1)
    ]

    all_tasks = early_reader_tasks + writer_tasks + late_reader_tasks
    await asyncio.gather(*all_tasks)

    total_time = time.perf_counter() - start_time
    print("─" * 70)
    print(f"Все транзакции завершили работу за {total_time:.2f} секунд")

if __name__ == "__main__":
    asyncio.run(main())
