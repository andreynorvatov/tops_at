import asyncio

from dataprovider import count_data_size, get_data_from_dataprovider, mark_checked_rows_in_dataprovider_by_row_id
from service import get_data_from_service, prepare_url
from data_transform import extract_task_data, extract_row_id_data, union_data_dicts
from write_file import generate_file
from checks import check_task_data
from settings import settings

async def main():
    # 1. Проверка количества task_id в таблице для поверки импорта

    params = [("read_line", "eq.false")]    # Фильтр для task_id без отметок о проверке
    between_date_sql_template = ""          # Условие between для подготовки SQL для удаления данных из Датапровайдера

    if START_DATE and END_DATE:
        params.append(("start_time", f"gte.{START_DATE}"))  # Диапазон дат начало
        params.append(("start_time", f"lte.{END_DATE}"))    # Диапазон дат конец
        between_date_sql_template = f"and (start_time::bigint between {START_DATE} and {END_DATE})"


    task_id_count_to_check = count_data_size(DATAPROVIDER_API_URL, TABLE_NAME, params, headers=None)
    print(f"Количество записей в таблице для проверки: {task_id_count_to_check}")

    if task_id_count_to_check == 0:
        # завершить скрипт, если нет записей в таблице
        exit(0)

    elif task_id_count_to_check > MAX_TASK_ID:
        # завершить скрипт, если нет записей в таблице больше установленного лимита
        print(f"Слишком много задач за 1 запуск. Максимально {MAX_TASK_ID}, фактически {task_id_count_to_check}.")
        exit(-1)

    # 2. Получение данных для проверки из Датапровайдера
    data_from_dataprovider = get_data_from_dataprovider(DATAPROVIDER_API_URL, TABLE_NAME, params)

    # 3. Получение данных по task_id из сервиса
    api_path_task_id = "/api/mona/background_tasks/{task_id}"
    # подготовка url для запуска асинхронного цикла
    background_tasks_urls = prepare_url(TOPS_URL, api_path_task_id, data_from_dataprovider)
    # асинхронное получение данных из сервиса
    raw_tasks_data_from_service = await get_data_from_service(background_tasks_urls, TOKEN, MAX_CONCURRENT_HTTP_REQUESTS)
    # обработка данных
    tasks_data_from_service = extract_task_data(raw_tasks_data_from_service)
    print(f"Получено записей для task_id из сервиса: {len(tasks_data_from_service)}")

    # 4. Получение данных по row_id из сервиса
    api_path_row_id = "/api/chiara/projection_data/{projection_id}/{row_id}"
    # подготовка url для запуска асинхронного цикла
    row_id_urls = prepare_url(TOPS_URL, api_path_row_id, data_from_dataprovider)
    # асинхронное получение данных из сервиса
    raw_row_id_data_from_service = await get_data_from_service(row_id_urls, TOKEN, MAX_CONCURRENT_HTTP_REQUESTS)
    # обработка данных
    row_id_data_from_service = extract_row_id_data(raw_row_id_data_from_service)
    print(f"Получено записей для row_id из сервиса: {len(row_id_data_from_service)}")

    # 5. Сбор данных в единую структуру
    aggregation_result = union_data_dicts(data_from_dataprovider, tasks_data_from_service, row_id_data_from_service)
    # проверка, что все данные из сервиса вошли в итоговый словарь
    if tasks_data_from_service or row_id_data_from_service:
        print("Проблема! Словари не пустые.")
        print(f"tasks_data_from_service: {tasks_data_from_service}")
        print(f"row_id_data_from_service: {row_id_data_from_service}")

    # 6. Запись данных в файл
    file_path = generate_file(aggregation_result)
    print(f"Файл с проверкой: {file_path}")

    # 6. Проверка данных
    check_res = check_task_data(aggregation_result)

    if not check_res["invalid_tasks"]:
        print("Проблем с задачами не выявлено.")
    else:
        print("В следующих задачах выявлена проблема:")
        print(check_res["errors"])

    # 7. Отметка в Датапровайдере данных как "проверенные"
    if MARK_CHECKED_IN_DATAPROVIDER:
        mark_res = mark_checked_rows_in_dataprovider_by_row_id(DATAPROVIDER_API_URL, TABLE_NAME, [k for k in aggregation_result.keys()])

        print(mark_res.get("message"))
        if not mark_res.get("success"):
            print(mark_res.get("error"))

    # 8. Генерация SQL запроса в БД Датапровайдера для удаления данных
    delete_sql_template = f"delete from api.{TABLE_NAME} where 1=1 {between_date_sql_template} and (read_line = true);"
    print(f"Запрос для удаления: {delete_sql_template}")

if __name__ == "__main__":

    # Интервал времени теста для проверки если не нужно, установить None
    START_DATE = "1776696816117"
    END_DATE = "1776762643345"

    # Настройки запуска
    MAX_TASK_ID = 300
    MAX_CONCURRENT_HTTP_REQUESTS = 10
    MARK_CHECKED_IN_DATAPROVIDER = True

    DATAPROVIDER_API_URL = settings.DATAPROVIDER_API_URL
    TABLE_NAME = settings.TABLE_NAME_CHECK_UPDATE

    TOPS_URL=settings.TOPS_URL
    TOKEN=settings.TOKEN

    asyncio.run(main())