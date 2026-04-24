import asyncio
import aiohttp
import csv
import time
from typing import Optional
import os
from dotenv import load_dotenv

async def fetch_batch(
    session: aiohttp.ClientSession,
    offset: int,
    limit: int,
    semaphore: asyncio.Semaphore,
    request_times: list
) -> Optional[list]:
    """Получает пакет данных с указанным offset и limit"""
    params = {
        "limit": limit,
        "offset": offset,
        # "sorting": "",
        # "filters": '[[{"column":"rajon_razvitija","to_lower":true,"operator":"EQUAL","value":"00b23989-43bd-4420-a61d-0d79c90016e2"}]]'
    }

    async with semaphore:
        start = time.perf_counter()
        async with session.get(URL_TEMPLATE, params=params, cookies=COOKIES, headers=HEADERS) as response:
            end = time.perf_counter()
            elapsed = (end - start) * 1000  # в миллисекундах
            request_times.append((offset, elapsed))
            
            if response.status == 200:
                data = await response.json()
                items = data.get('items', [])
                print(f"Получено {len(items)} записей (offset={offset}, limit={limit}) за {elapsed:.2f} мс")
                return items
            else:
                print(f"Ошибка при запросе offset={offset}: статус {response.status} за {elapsed:.2f} мс")
                return None


async def worker(
    session: aiohttp.ClientSession,
    offset: int,
    limit: int,
    semaphore: asyncio.Semaphore,
    csv_writer: csv.writer,
    csv_file,
    results: list,
    request_times: list
) -> None:
    """Воркер для получения и записи данных"""
    items = await fetch_batch(session, offset, limit, semaphore, request_times)
    if items:
        for item in items:
            row_id = item.get('_id', '')
            results.append(row_id)
            csv_writer.writerow([row_id])


async def collect_all_ids(
    total_rows: int,
    limit: int = 100,
    max_concurrent: int = 5,
    start_offset: int = 0,
    filename: str = "row_ids.csv"
) -> tuple[list, list]:
    """
    Собирает все _id из таблицы асинхронно.

    Args:
        total_rows: Общее количество требуемых строк
        limit: Количество строк за один запрос
        max_concurrent: Максимальное количество одновременно работающих корутин
        filename: Имя файла для сохранения результатов
        start_offset: Начальный offset для выборки данных

    Returns:
        Кортеж из (список всех полученных _id, список времени запросов)
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    results = []
    request_times = []

    async with aiohttp.ClientSession() as session:
        # Открываем CSV файл для записи
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['row_id'])

            # Создаем задачи для всех offset, начиная с start_offset
            tasks = []
            for offset in range(start_offset, start_offset + total_rows, limit):
                task = asyncio.create_task(
                    worker(session, offset, limit, semaphore, writer, csvfile, results, request_times)
                )
                tasks.append(task)

            # Ждем выполнения всех задач
            await asyncio.gather(*tasks)

    print(f"Всего собрано {len(results)} ID в файл {filename}")
    return results, request_times


async def main():
    # Параметры
    TOTAL_ROWS = 1000      # Общее количество требуемых строк
    LIMIT = 100            # Сколько получать за один раз
    MAX_CONCURRENT = 10    # Количество одновременно работающих корутин
    START_OFFSET = 0       # Начальный offset для выборки
    FILENAME = "data/row_ids.csv"  # Имя файла для сохранения

    print(f"Запуск сбора данных: total={TOTAL_ROWS}, limit={LIMIT}, concurrent={MAX_CONCURRENT}, start_offset={START_OFFSET}, filename={FILENAME}")

    start_total = time.perf_counter()
    ids, request_times = await collect_all_ids(
        total_rows=TOTAL_ROWS,
        limit=LIMIT,
        max_concurrent=MAX_CONCURRENT,
        start_offset=START_OFFSET,
        filename=FILENAME
    )
    end_total = time.perf_counter()
    total_elapsed = (end_total - start_total) * 1000  # в миллисекундах

    print(f"Готово! Получено {len(ids)} уникальных ID")
    print(f"\nОбщее время выполнения: {total_elapsed:.2f} мс")
    print(f"Всего запросов: {len(request_times)}")

    # Расчет производительности
    total_seconds = total_elapsed / 1000
    rows_per_second = len(ids) / total_seconds if total_seconds > 0 else 0
    requests_per_second = len(request_times) / total_seconds if total_seconds > 0 else 0
    print(f"Строк в секунду: {rows_per_second:.2f}")
    print(f"Запросов в секунду: {requests_per_second:.2f}")

    if request_times:
        avg_time = sum(t for _, t in request_times) / len(request_times)
        min_time = min(t for _, t in request_times)
        max_time = max(t for _, t in request_times)
        print(f"Среднее время запроса: {avg_time:.2f} мс")
        print(f"Мин/Макс время запроса: {min_time:.2f} / {max_time:.2f} мс")


if __name__ == "__main__":

    load_dotenv()

    URL = os.getenv('URL')

    DATABASES_ID = os.getenv('DATABASES_ID')
    TABLE_ID = os.getenv('TABLE_ID')
    TOKEN = os.getenv('TOKEN')
    SESSION_COOKIE = os.getenv('SESSION_COOKIE')

    URL_TEMPLATE = f"https://{URL}/api/chiara/table_data/{TABLE_ID}"

    COOKIES = {
        "session-cookie": SESSION_COOKIE
    }

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://{URL}/databases/{DATABASES_ID}/{TABLE_ID}",
        "x-lamb-auth-token": TOKEN
    }

    asyncio.run(main())
