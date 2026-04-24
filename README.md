## Автоматизация tops
### Проверка фоновых задач (импорт файлов) [check_uploaded_files](check_uploaded_files)

_Набор скриптов для проверки успешности фоновых операций по импорту файлов._

#### Запуск
1. Получить авторизационный токен пользователя за которого выполнялись импорты в тесте.
2. Добавить в .env файл
3. Запустить [main_async.py](check_uploaded_files/main_async.py)
4. В результате будет собран csv в директории [artifacts](check_uploaded_files/artifacts)
5. В консоль будет выведен краткий результат проверки

### Подготовка данных для upsert [generate_rowid_dataset](generate_rowid_dataset)

_Набор скриптов для подготовки тестовых данных для работы скрипта upsert (обновления строк из файла)_

#### Последовательный запуск 3-ех скриптов

1. [download_rows_async.py](generate_rowid_dataset/download_rows_async.py) - асинхронно через сервис ТОПС собирает row_id в csv
2. [shuffle.py](generate_rowid_dataset/shuffle.py) - в собранной csv удаляет дубликаты и перемешивает полученные row_id
3. [upload_data_in_db.py](generate_rowid_dataset/upload_data_in_db.py) - загружает данные в таблицу в Датапровайдер


### Общие ресурсы
1. [.env](.env) - создать по примеру [.env.example](.env.example)
2. [settings.py](settings.py) - настройки проекта

### Подготовка окружения
#### pip
```commandline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

#### uv
```commandline
uv venv
source .venv/bin/activate
uv sync

```