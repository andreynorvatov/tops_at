import csv
import random
from collections import Counter

def read_csv_file(filepath: str) -> tuple[list[str], list[list[str]]]:
    """Читает CSV-файл и возвращает заголовок и данные."""
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    return header, data


def find_duplicates(data: list[list[str]], row_id_index: int) -> list[str]:
    """Находит дублирующиеся row_id в данных."""
    all_ids = [row[row_id_index] for row in data]
    return [id_val for id_val, count in Counter(all_ids).items() if count > 1]


def print_duplicate_info(duplicates: list[str], data: list[list[str]], row_id_index: int) -> None:
    """Выводит информацию о найденных дубликатах."""
    print(f"Найдены дубликаты row_id: {len(duplicates)} уникальных ID имеют повторы")
    print("Первые 5 дублирующихся ID:")
    all_ids = [row[row_id_index] for row in data]
    for dup in duplicates[:5]:
        count = all_ids.count(dup)
        print(f"  {dup}: встречается {count} раз(а)")


def remove_duplicates(data: list[list[str]], row_id_index: int) -> list[list[str]]:
    """Удаляет дубликаты, оставляя только первые вхождения."""
    seen = set()
    unique_data = []
    for row in data:
        if row[row_id_index] not in seen:
            seen.add(row[row_id_index])
            unique_data.append(row)
    return unique_data


def shuffle_data(data: list[list[str]]) -> None:
    """Перемешивает данные inplace."""
    random.shuffle(data)


def write_csv_file(filepath: str, header: list[str], data: list[list[str]]) -> None:
    """Записывает данные в CSV-файл."""
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)


def main() -> None:
    """Основная функция. Удаляет дубликаты и перемешивает файл."""

    # Чтение данных
    header, data = read_csv_file(INPUT_FILE)

    # Проверка уникальности row_id
    duplicates = find_duplicates(data, ROW_ID_INDEX)

    if duplicates:
        print_duplicate_info(duplicates, data, ROW_ID_INDEX)

        # Опция: удалить дубликаты
        remove_duplicates_flag = input("Удалить дубликаты? (y/n): ").lower() == 'y'

        if remove_duplicates_flag:
            data = remove_duplicates(data, ROW_ID_INDEX)
            print(f"Удалено дублирующихся строк: {len(data)}")
    else:
        print("✓ Все row_id уникальны!")

    # Перемешивание данных
    shuffle_data(data)

    # Запись результата
    write_csv_file(OUTPUT_FILE, header, data)

    print(f"\nФайл сохранен: {OUTPUT_FILE}")
    print(f"Всего строк в итоговом файле: {len(data)}")


if __name__ == '__main__':

    INPUT_FILE = 'data/row_ids_2026-04-24_14-43-26.csv'
    OUTPUT_FILE = 'data/row_ids_2026-04-24_14-43-26_shuffle.csv'
    ROW_ID_INDEX = 0  # row_id обычно первая колонка

    main()