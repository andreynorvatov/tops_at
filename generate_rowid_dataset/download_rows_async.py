import asyncio
from datetime import datetime

import aiohttp
import csv
import time
from typing import Optional

from settings import settings

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
    }

    async with semaphore:
        start = time.perf_counter()
        async with session.get(URL_TEMPLATE, params=params, headers=HEADERS) as response:
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
    csv_writer,
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
                    worker(session, offset, limit, semaphore, writer, results, request_times)
                )
                tasks.append(task)

            # Ждем выполнения всех задач
            await asyncio.gather(*tasks)

    print(f"Всего собрано {len(results)} ID в файл {filename}")
    return results, request_times


async def main():

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

    # Настройки запуска
    TOTAL_ROWS = 100        # Общее количество требуемых строк
    LIMIT = 10              # Сколько получать за один раз
    MAX_CONCURRENT = 10     # Количество одновременно работающих корутин
    START_OFFSET = 0        # Начальный offset для выборки

    TOPS_URL = settings.TOPS_URL
    TOPS_DATA_TABLE_ID = settings.TOPS_DATA_TABLE_ID
    TOKEN = settings.TOKEN

    URL_TEMPLATE = f"{TOPS_URL}/api/chiara/table_data/{TOPS_DATA_TABLE_ID}"

    HEADERS = {"x-lamb-auth-token": TOKEN}

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    FILENAME = f"data/row_ids_{timestamp}.csv"  # Имя файла для сохранения

    asyncio.run(main())
