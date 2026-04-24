import csv


def delete_rajon_razvitija_column(input_file, output_file):
    """
    Удаляет колонку rajon_razvitija из CSV файла

    Args:
        input_file (str): путь к исходному CSV файлу
        output_file (str): путь для сохранения результата (обязательный параметр)
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
                open(output_file, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # Читаем заголовки
            headers = next(reader)

            # Проверяем наличие колонки
            if 'rajon_razvitija' not in headers:
                print("Ошибка: колонка 'rajon_razvitija' не найдена в файле")
                return

            # Находим индекс колонки
            col_index = headers.index('rajon_razvitija')

            # Записываем заголовки без удаленной колонки
            writer.writerow([h for i, h in enumerate(headers) if i != col_index])

            # Записываем данные без удаленной колонки
            for row in reader:
                writer.writerow([value for i, value in enumerate(row) if i != col_index])

            print(f"✓ Колонка удалена. Результат сохранен в {output_file}")

    except FileNotFoundError:
        print(f"Ошибка: файл {input_file} не найден")
    except Exception as e:
        print(f"Ошибка: {e}")

# Пример использования:
# delete_rajon_razvitija_column('data/bkp/row_id_3m_shuffle.csv', 'data/row_id_3m_shuffle.csv')