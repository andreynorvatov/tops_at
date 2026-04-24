import psycopg2
from psycopg2.extras import execute_batch
from pathlib import Path
from tqdm import tqdm
import os
from dotenv import load_dotenv


def count_csv_lines(filepath: Path) -> int:
    """Подсчет строк в CSV (без заголовка)"""
    with open(filepath, "r") as f:
        return sum(1 for _ in f) - 1  # минус заголовок


def check_uniqueness(cur, table_name: str, column: str = "row_id") -> dict:
    """Проверка уникальных и дублирующихся строк в таблице"""
    # Общее количество строк
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    total = cur.fetchone()[0]

    # Количество уникальных строк
    cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table_name}")
    unique = cur.fetchone()[0]

    # Количество дублирующихся строк
    duplicates = total - unique

    # Детализация по дубликатам (группировка по количеству)
    cur.execute(f"""
        SELECT cnt, COUNT(*) as num_groups
        FROM (
            SELECT {column}, COUNT(*) as cnt
            FROM {table_name}
            GROUP BY {column}
            HAVING COUNT(*) > 1
        ) t
        GROUP BY cnt
        ORDER BY cnt DESC
        LIMIT 10
    """)
    duplicate_details = cur.fetchall()

    return {
        "total": total,
        "unique": unique,
        "duplicates": duplicates,
        "duplicate_details": duplicate_details
    }


def upload_csv():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    conn.autocommit = False

    try:
        # Считаем строки в файле
        lines_in_file = count_csv_lines(CSV_FILE)
        print(f"📊 Строк в файле: {lines_in_file:,}")
        print(f"📤 Загрузка {CSV_FILE.name}...")

        # Быстрая загрузка через COPY
        with open(CSV_FILE, "r") as f:
            next(f)  # пропуск заголовка
            cur.copy_from(f, TABLE_NAME, sep=",", columns=("row_id",))
        
        conn.commit()

        # Проверка количества загруженных строк
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        rows_in_db = cur.fetchone()[0]

        # Проверка уникальности
        stats = check_uniqueness(cur, TABLE_NAME)

        print(f"✓ Загрузка завершена: {CSV_FILE.name}")
        print(f"📈 Строк в таблице: {stats['total']:,}")
        print(f"📊 Загружено: {lines_in_file:,}")
        print(f"🔹 Уникальных: {stats['unique']:,}")
        print(f"🔸 Дубликатов: {stats['duplicates']:,}")

        if stats['duplicate_details']:
            print(f"\n📋 Детализация дубликатов (кол-во повторений → кол-во групп):")
            for cnt, num_groups in stats['duplicate_details']:
                print(f"   {cnt}× → {num_groups:,} групп")

        if rows_in_db >= lines_in_file:
            print(f"✓ Все строки успешно загружены")
        else:
            print(f"⚠️ Внимание: загружено меньше строк, чем в файле")
        
    except Exception as e:
        conn.rollback()
        print(f"✗ Ошибка: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def upload_csv_with_progress():
    """Альтернативный метод с прогресс-баром для очень больших файлов"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    conn.autocommit = False

    try:
        # Считаем строки для прогресс-бара
        total_lines = count_csv_lines(CSV_FILE)
        print(f"📊 Строк в файле: {total_lines:,}")
        print(f"📤 Загрузка {CSV_FILE.name}...")

        batch = []
        loaded_count = 0

        with open(CSV_FILE, "r") as f:
            next(f)  # пропуск заголовка

            with tqdm(total=total_lines, desc="Загрузка", unit="стр") as pbar:
                for line in f:
                    row_id = line.strip()
                    if row_id:
                        batch.append((row_id,))

                        if len(batch) >= BATCH_SIZE:
                            execute_batch(
                                cur,
                                f"INSERT INTO {TABLE_NAME} (row_id) VALUES (%s)",
                                batch
                            )
                            conn.commit()
                            loaded_count += len(batch)
                            batch.clear()
                            pbar.update(BATCH_SIZE)

                # Финальный батч
                if batch:
                    execute_batch(
                        cur,
                        f"INSERT INTO {TABLE_NAME} (row_id) VALUES (%s)",
                        batch
                    )
                    conn.commit()
                    loaded_count += len(batch)
                    pbar.update(len(batch))

        # Проверка количества загруженных строк
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        rows_in_db = cur.fetchone()[0]

        # Проверка уникальности
        stats = check_uniqueness(cur, TABLE_NAME)

        print(f"✓ Загрузка завершена: {CSV_FILE.name}")
        print(f"📈 Строк в таблице: {stats['total']:,}")
        print(f"📊 Загружено (по счетчику): {loaded_count:,}")
        print(f"🔹 Уникальных: {stats['unique']:,}")
        print(f"🔸 Дубликатов: {stats['duplicates']:,}")

        if stats['duplicate_details']:
            print(f"\n📋 Детализация дубликатов (кол-во повторений → кол-во групп):")
            for cnt, num_groups in stats['duplicate_details']:
                print(f"   {cnt}× → {num_groups:,} групп")

        if rows_in_db >= loaded_count:
            print(f"✓ Все строки успешно загружены")
        else:
            print(f"⚠️ Внимание: загружено меньше строк, чем ожидалось")

    except Exception as e:
        conn.rollback()
        print(f"✗ Ошибка: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    CSV_FILE = Path("data/row_id_3m_shuffle.csv")
    TABLE_NAME = "tops_rowid_pp"
    BATCH_SIZE = 100_000  # Оптимально для 3-6 млн строк

    load_dotenv()

    # Конфигурация
    DB_CONFIG = {
        "host": os.getenv('DATA_PROVIDER_HOST'),
        "database": os.getenv('DATA_PROVIDER_DATABASE'),
        "user": os.getenv('DATA_PROVIDER_USER'),
        "password": os.getenv('DATA_PROVIDER_PWD'),
        "options": "-c search_path=api"
    }

    # Основной метод - быстрый COPY
    # upload_csv()
    
    # Для отслеживания прогресса раскомментировать:
    upload_csv_with_progress()
