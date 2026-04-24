import requests


def _sync_request_to_dataprovider(dp_host: str, table_name: str, params: list[tuple[str, str]], headers: dict[str, str] | None) -> requests.Response:

    res = requests.get(
        f'{dp_host}/{table_name}',
        headers=headers,
        params=params
    )

    return res

def count_data_size(dp_host: str, table_name: str, params: list[tuple[str, str|int]], headers: dict[str, str] | None) -> int:

    params_updated = (params or []).copy()
    params_updated.append(("limit", 0))

    headers = headers or {}
    headers["Prefer"] = "count=exact"

    res = _sync_request_to_dataprovider(dp_host, table_name, params_updated, headers)

    content_range_header = res.headers.get("Content-Range")

    if content_range_header and '/' in content_range_header:
        return int(content_range_header.split('/')[-1])
    return 0

def get_data_from_dataprovider(dp_host: str, table_name: str, params: list[tuple[str, str]]) -> dict:
    row_data = _sync_request_to_dataprovider(dp_host=dp_host, table_name=table_name, params=params, headers=None).json()
    return {item['task_id']: {k: v for k, v in item.items() if k != 'task_id'} for item in row_data}

def mark_checked_rows_in_dataprovider_by_row_id(dp_host: str, table_name: str, task_ids: list[str]):
    if not task_ids:
        return {"success": False, "message": "Список task_id пуст", "updated_count": 0}

    url = f"{dp_host}/{table_name}"

    # Данные для обновления
    update_data = {
        "read_line": True
    }

    # Формируем фильтр для IN-условия
    # Используем оператор in.(value1,value2,value3)
    task_ids_str = ",".join(str(t_id) for t_id in task_ids)
    params = {"task_id": f"in.({task_ids_str})"}

    # Заголовки для получения информации об обновленных строках
    headers = {
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

    try:
        response = requests.patch(
            url,
            json=update_data,
            params=params,
            headers=headers
        )

        if response.status_code == 200:
            updated_rows = response.json()
            return {
                "success": True,
                "message": f"Успешно обновлено {len(updated_rows)} строк в Датапровайдере",
                "updated_count": len(updated_rows),
                "updated_data": updated_rows
            }
        else:
            return {
                "success": False,
                "message": f"Ошибка API при попытке обновления данных в Датапровайдере: {response.status_code}",
                "error": response.text,
                "updated_count": 0
            }

    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "message": f"Ошибка соединения с Датапровайдером: {str(e)}",
            "updated_count": 0
        }


if __name__ == "__main__":

    DP_HOST = "http://10.126.145.36:3000"
    TABLE_NAME = 'tops_checkupdate_pp'

    # PARAMS_DATA = [("read_line", "eq.false")]
    # #
    # # row_count = count_data_size(DP_HOST, TABLE_NAME, PARAMS_DATA, headers=None)
    # # print(row_count)
    # #
    # START_DATE = "1776696816117"
    # END_DATE = "1776762643345"
    # PARAMS_DATA_2 = [
    #     ("read_line", "eq.false"),
    #     ("start_time", f"gte.{START_DATE}"),
    #     ("start_time", f"lte.{END_DATE}")
    # ]
    # data = get_data_from_dataprovider(DP_HOST, TABLE_NAME, PARAMS_DATA_2)
    # print(data)
    # print(len(data))

    r = mark_checked_rows_in_dataprovider_by_row_id(DP_HOST, TABLE_NAME, ["e676b35b-dc26-4b41-a5bc-26e39b825da0", '76fba598-8858-4673-a037-14d98c47d333'])

    print(r)