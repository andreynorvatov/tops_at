import csv
import os
from datetime import datetime

def dict_to_csv(data: dict, filename: str) -> str:
    if not data:
        return ""

    # Добавляем task_id и записываем
    for task_id, task_data in data.items():
        task_data['task_id'] = task_id

    fieldnames = ['task_id'] + [k for k in data[list(data.keys())[0]].keys() if k != 'task_id']

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data.values())

    # Возвращаем абсолютный путь до файла
    return os.path.abspath(filename)



def generate_filename(prefix: str = "", ext: str = "csv") -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{prefix}_{timestamp}.{ext}" if prefix else f"{timestamp}.{ext}"


def generate_file(data: dict[str, dict[str, str]], file_dir: str = "artifacts"):
    # Создаем папку artifacts, если она не существует
    os.makedirs(file_dir, exist_ok=True)

    file_name = generate_filename("check")
    abs_path = dict_to_csv(data, f"{file_dir}/{file_name}")

    return abs_path

if __name__ == '__main__':
    d = {'75ba4e6e-62c1-45b7-90c1-e841c822e04b': {'row_id': 'f2d1f7b6-9279-400e-9241-ceb4b8cf2d99', 'mob': '+79111388863', 'email': 'ttgzohwj@lumns.com', 'start_time_msk': '2026-04-21 11:20:07', 'start_time': '1776756007567', 'file_name': 'Чертаново Центральное_200.csv', 'projection_id': 'c67055d9-da4d-48d5-86e9-2ae7b72b23eb', 'task_status': 'COMPLETED', 'task_duration_seconds': 1.124536, 'task_time_created': '2026-04-21T07:20:07.219370Z', 'task_time_updated': '2026-04-21T07:20:08.343906Z', 'task_projection_title': 'Чертаново Центральное_актив', 'task_type': 'FILE_IMPORT_PROJECTION', 'task_error_info': None, 'task_uploaded_rows': 'Загружено строк: 200', 'task_created_attr': 'Создано атрибутов: 0', 'row_id_mob': '+79111388863', 'row_id_email': 'ttgzohwj@lumns.com', 'mob_check': True, 'email_check': True}}

    filename = generate_filename("check")

    dict_to_csv(d, f"artifacts/{filename}")