from datetime import datetime

def extract_task_data(tasks_data: list[dict]) -> dict[str, dict[str, str]]:
    """
    Извлекает выбранные данные из списка словарей задач.

    Args:
        tasks_data: Словарь словарей с данными задач

    Returns:
        Список словарей с извлеченными полями:
        task_id, task_type, projection_title, status, error_info, meta_info, success
    """
    result = {}

    for data in tasks_data:
        for task_id, task_data in data.items():

            time_created = datetime.fromisoformat(task_data.get('time_created').replace('Z', '+00:00'))
            time_updated = datetime.fromisoformat(task_data.get('time_updated').replace('Z', '+00:00'))
            meta_info = task_data.get('meta_info').get("success_upload_context")
            if meta_info:
                uploaded_rows, created_attr = meta_info.split('\n')
            else:
                uploaded_rows,created_attr = None, None

            extracted = {
                'status': task_data.get('status'),
                "duration_seconds": (time_updated - time_created).total_seconds(),
                "time_created": task_data.get('time_created'),
                "time_updated": task_data.get('time_updated'),
                'projection_title': task_data.get('context', {}).get('projection_title'),
                'task_type': task_data.get('task_type'),
                'error_info': task_data.get('error_info'),
                'uploaded_rows': uploaded_rows,
                'created_attr': created_attr,
            }
            result[task_id]= extracted

    return result

def extract_row_id_data(rows_id_data: list[dict]) -> dict[str, dict[str, str]]:

    result = {}

    for data in rows_id_data:
        for task_id, row_id_data in data.items():

            if row_id_data.get("items"):
                extracted = {
                    "mob": row_id_data.get('items')[0].get("mobil_nyj_telefon_1"),
                    "email": row_id_data.get('items')[0].get("elektronnaja_pochta")
                }

                if len(row_id_data.get("items")) > 1:
                    print(f"Внимание! Запрос поиска по телефону вернул более 1 результата. task_id: {task_id}")

            else:
                extracted = {
                    "mob": row_id_data.get('mobil_nyj_telefon_1'),
                    "email": row_id_data.get('elektronnaja_pochta')
                }

            result[task_id]= extracted

    return result

def union_data_dicts(dataprovider_dict: dict, tasks_dict, rows_id_dict) -> dict:

    result = {}
    for task_id, task_data in dataprovider_dict.items():

        t = tasks_dict.pop(task_id)
        r = rows_id_dict.pop(task_id)

        mob = task_data.get("mob").replace(" ", "").replace("-", "")

        result[task_id] = {
            "row_id": task_data.get("row_id"),
            "mob": mob,
            "email": task_data.get("email"),
            "start_time_msk": datetime.fromtimestamp(int(task_data.get("start_time")) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            "start_time": task_data.get("start_time"),
            "file_name": task_data.get("file_name"),
            "projection_id": task_data.get("projection_id"),

            "task_status": t.get("status"),
            "task_duration_seconds": t.get("duration_seconds"),
            "task_time_created": t.get("time_created"),
            "task_time_updated": t.get("time_updated"),
            "task_projection_title": t.get("projection_title"),
            "task_type": t.get("task_type"),
            "task_error_info": t.get("error_info"),
            "task_uploaded_rows": t.get("uploaded_rows"),
            "task_created_attr": t.get("created_attr"),

            "row_id_mob": r.get("mob"),
            "row_id_email": r.get("email"),

            "mob_check": r.get("mob") == mob,
            "email_check": r.get("email") == task_data.get("email"),
        }

    return result


if __name__ == '__main__':

    r = [{'75ba4e6e-62c1-45b7-90c1-e841c822e04b': {'time_created': '2026-04-21T07:20:07.219370Z', 'time_updated': '2026-04-21T07:20:08.343906Z', 'task_id': '75ba4e6e-62c1-45b7-90c1-e841c822e04b', 'user_id': '9537de14-9028-4f21-8d44-94e05d04f70c', 'task_type': 'FILE_IMPORT_PROJECTION', 'status': 'COMPLETED', 'meta_info': {'success_upload_context': 'Загружено строк: 200\nСоздано атрибутов: 0'}, 'error_info': None, 'context': {'table_id': '3b603b70-f816-4f31-9871-8bfd50fd4504', 'database_id': '84a1993a-c55c-4e7f-9577-5d6199934056', 'projection_id': 'c67055d9-da4d-48d5-86e9-2ae7b72b23eb', 'projection_title': 'Чертаново Центральное_актив'}, 'target_object': {'object_kind': 'PROJECTION', 'object_id': 'c67055d9-da4d-48d5-86e9-2ae7b72b23eb'}, 'user': {'time_created': '2026-03-03T07:50:06.468112Z', 'time_updated': '2026-03-03T07:50:36.233639Z', 'user_id': '9537de14-9028-4f21-8d44-94e05d04f70c', 'role': 'OPERATOR', 'is_blocked': False, 'last_name': 'Тестовый НТ2', 'first_name': 'Тестовый НТ2', 'middle_name': 'Тестовый НТ2', 'phone_number': None, 'email': '', 'sudir_snils': '00009990108', 'sudir_guid': 'b33e7d9e-9c9f-4cfb-a6ea-d6b83a8fa3f1', 'spaces': []}}}]

    res = extract_task_data(r)

    for i in res:
        print(i)