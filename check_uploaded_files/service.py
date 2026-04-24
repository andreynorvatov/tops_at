import asyncio
import urllib.parse

import aiohttp
from typing import List, Dict, Any, Optional
import json
import os
from dotenv import load_dotenv

async def fetch_data(
        session: aiohttp.ClientSession,
        task_id: str,
        url: str,
        auth_token: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 10
) -> Dict[str, Any]:
    """
    Асинхронный запрос к API с параметрами

    Args:
        session: aiohttp сессия
        task_id:
        url: URL запроса
        auth_token:
        params: Параметры запроса (GET параметры)
        timeout: Таймаут в секундах

    Returns:
        Словарь с данными ответа
    """
    async with session.get(url, params=params, timeout=timeout, headers = {"x-lamb-auth-token": auth_token}) as response:
        response.raise_for_status()  # Поднимет исключение при HTTP ошибке
        row_response = await response.json()
        # print("-----------------------")
        # print(url)
        # print(params)
        # print(row_response)
        # print("-----------------------")
        return {task_id: row_response}


async def execute_request_with_limit(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    task_id: str,
    url: str,
    auth_token: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 10
) -> Optional[Dict[str, Any]]:
    """
    Выполняет один HTTP запрос с ограничением параллельности через семафор
    """
    async with semaphore:
        try:
            result = await fetch_data(session, task_id, url, auth_token, params, timeout)
            return result
        except Exception as e:
            print(f"Ошибка запроса {url}: {e}")
            return None


async def parallel_requests_with_concurrency_limit(
        requests_url: dict[str, str],
        auth_token: str,
        params: Optional[Dict[str, Any]] = None,
        max_concurrent_requests: int = 5
) -> List[Dict[str, Any]]:
    """
    Выполняет множество HTTP запросов параллельно с ограничением
    на максимальное количество одновременных запросов

    Args:
        requests_url:
        auth_token:
        params:
        max_concurrent_requests: Максимальное количество одновременно выполняемых запросов

    Returns:
        Список успешных ответов (ответы с ошибками игнорируются)
    """
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for task_id, url in requests_url.items():
            task = execute_request_with_limit(
                session,
                semaphore,
                task_id,
                url,
                auth_token,
                params,
                10
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks)

    return [r for r in results if r is not None]

async def get_data_from_service(
        requests_url: dict[str, str],
        auth_token: str,
        max_concurrent_requests: int = 5) -> List[Dict[str, Any]]:

    results = await parallel_requests_with_concurrency_limit(
        requests_url,
        auth_token,
        max_concurrent_requests=max_concurrent_requests
    )

    return results

def prepare_url(url: str, api_path: str, data: dict) -> dict[str, str]:

    base_url = url.rstrip('/')
    full_api_path_dict = {}

    if "background_tasks" in api_path:

        for task_id in data.keys():
            full_path = api_path.format(task_id=task_id)
            full_api_path_dict[task_id] = full_path
    else:
        for task_id, task_data in data.items():
            projection_id = task_data.get("projection_id")
            row_id = task_data.get("row_id")

            if not row_id:
                modified_api_path = api_path.replace("/{row_id}", "")
                mob = task_data.get("mob")
                filters = [[{
                    "column": "mobil_nyj_telefon_1",
                    "to_lower": True,
                    "operator": "EQUAL",
                    "value": mob
                }]]
                filters_json = json.dumps(filters, separators=(',', ':'))
                filters_encoded = urllib.parse.quote(filters_json)
                full_path = f"{modified_api_path.format(projection_id=projection_id)}/?filters={filters_encoded}"

            else:
                full_path = api_path.format(projection_id=projection_id, row_id=row_id)

            full_api_path_dict[task_id] = full_path

    return {row_id: f"{base_url}{api_path}" for row_id, api_path in full_api_path_dict.items()}



async def main():
    # Пример списка ID
    t = {'c67ce9bb-a7da-462e-9231-919d192f932a': {'id': 6, 'row_id': '5ef1055a-822c-4dc4-a9fd-06851cf4f1f5', 'mob': '+7 909 578-24-52', 'email': 'aiooyjqq@zlxbe.com', 'start_time': '1776696977192', 'file_name': 'Зябликово_200.csv', 'projection_id': 'defaf623-9370-4567-9df3-ef8ca0240091', 'read_line': False}, '90416f65-e49b-49b6-810a-aff1cf7f522a': {'id': 7, 'row_id': '20dce5d3-3ea7-4ab4-b03e-2c558b948b9d', 'mob': '+7 973 187-25-21', 'email': 'vhrhvxsc@skekm.com', 'start_time': '1776755585423', 'file_name': 'Москворечье-Сабурово_200.csv', 'projection_id': 'fa5551db-5b23-417d-b2fc-84017e9019d1', 'read_line': False}, '724bd96a-1f08-4c7d-87c6-8d21ffbdf3d4': {'id': 8, 'row_id': 'e37169ba-b20a-4fc4-8894-b02fd3370a95', 'mob': '+7 925 476-36-42', 'email': 'ryudiwhp@ihfcd.com', 'start_time': '1776755645055', 'file_name': 'Мещанский_200.csv', 'projection_id': '433fa1ba-1a12-4d0a-a682-820a12bbc789', 'read_line': False}, 'd718478c-2f12-4941-8aa5-899ad4a0087e': {'id': 9, 'row_id': '416b64c0-23a6-4ad0-8690-f40e72ed179b', 'mob': '+7 969 450-24-34', 'email': 'dltaxuxt@gwqso.com', 'start_time': '1776755703448', 'file_name': 'Преображенское_200.csv', 'projection_id': 'c97b9da9-30f6-429d-a537-25490ed5b53d', 'read_line': False}, '0d1542e5-d3e5-45a1-a12a-61f4e57b312d': {'id': 10, 'row_id': '3ee243f2-6c44-4c8c-b053-43c357dd77bb', 'mob': '+7 982 625-17-61', 'email': 'nztquiqp@trjpb.com', 'start_time': '1776755764137', 'file_name': 'Строгино_200.csv', 'projection_id': 'c50cbb78-a8e7-49b9-9968-0704c57bbdfe', 'read_line': False}, '5a31ba81-948c-4df2-82e1-c14cccb9cf51': {'id': 11, 'row_id': '4c8a6ce8-172c-4f87-8235-7382a23bcda0', 'mob': '+7 974 155-84-90', 'email': 'bticlbsp@bikes.com', 'start_time': '1776755811641', 'file_name': 'Ховрино_200.csv', 'projection_id': '98971663-199b-4d4e-91ed-7dc4463f31ac', 'read_line': False}, '76fba598-8858-4673-a037-14d98c47d333': {'id': 12, 'row_id': '6d80af11-0303-4047-ad89-30c7e577e161', 'mob': '+7 930 752-57-59', 'email': 'foniflnq@mstvy.com', 'start_time': '1776755873548', 'file_name': 'Восточное Измайлово_200.csv', 'projection_id': 'cac486d8-ddf0-47e6-b4b9-29fe10c869c1', 'read_line': False}, 'fbabc496-d192-44f6-a046-38179c54a00e': {'id': 13, 'row_id': '9f8490f3-7de3-49fc-8d2d-ac84dda0cf3e', 'mob': '+7 901 820-74-86', 'email': 'xhxufzrc@kgift.com', 'start_time': '1776755940460', 'file_name': 'Гольяново_200.csv', 'projection_id': '11c00259-4257-4792-8625-73ecf1a337a0', 'read_line': False}, '75ba4e6e-62c1-45b7-90c1-e841c822e04b': {'id': 14, 'row_id': 'f2d1f7b6-9279-400e-9241-ceb4b8cf2d99', 'mob': '+7 911 138-88-63', 'email': 'ttgzohwj@lumns.com', 'start_time': '1776756007567', 'file_name': 'Чертаново Центральное_200.csv', 'projection_id': 'c67055d9-da4d-48d5-86e9-2ae7b72b23eb', 'read_line': False}, 'fdc57ae7-ca1b-44f6-91dd-990f7c9fe169': {'id': 15, 'row_id': None, 'mob': '+7 997 209-55-20', 'email': 'pvbatbmu@ognim.com', 'start_time': '1776762643345', 'file_name': 'Молжаниновский_50.csv', 'projection_id': '9f7d9e02-6543-4e94-923b-8a1d66de4b11', 'read_line': False}}

    # Выполняем запросы
    # results = await fetch_tasks_batch(
    #     task_ids=task_ids,
    #     auth_token=TOKEN,
    #     max_concurrent=5  # максимум 5 одновременных запросов
    # )

    # results = await get_data_from_service(URL, task_ids=task_ids, auth_token=TOKEN)

    api_path_task_id = "/api/mona/background_tasks/{task_id}"
    api_path_row_id = "/api/chiara/projection_data/{projection_id}/{row_id}"


    # print(prepare_url(TOPS_URL, api_path_row_id, t))
    # results = await get_data_from_service_v2(URL, )
    background_tasks_urls = prepare_url(TOPS_URL, api_path_row_id, t)
    print(background_tasks_urls)

    results = await get_data_from_service(background_tasks_urls, TOKEN)
    print(results)
    print(len(results))

    # print(results)
    # print(len(results))
    # Выводим результаты
    # for result in results:
    #     pprint(result)
    #     if result["success"]:
    #         print(f"✅ Task {result['task_id']}: {result['data']}")
    #     else:
    #         print(f"❌ Task {result['task_id']}: {result['error']}")


if __name__ == "__main__":
    load_dotenv()

    TOKEN = os.getenv('TOKEN')

    # URL = os.getenv('URL')
    TOPS_URL = "https://tops-stage.mos.ru"

    asyncio.run(main())

