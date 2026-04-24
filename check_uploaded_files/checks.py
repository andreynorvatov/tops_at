
def check_task_data(data_dict):
    """
    Проверяет словарь с данными задач на соответствие условиям:
    - email_check и mob_check не должны быть False
    - task_status должен быть 'COMPLETED'
    - task_duration_seconds не более 20 секунд

    Args:
        data_dict: словарь с данными задач

    Returns:
        dict: Словарь с результатами проверки, содержащий:
              - 'valid_tasks': список ID задач, прошедших проверку
              - 'invalid_tasks': список ID задач, не прошедших проверку
              - 'errors': детали ошибок для каждой задачи
    """

    results = {
        'valid_tasks': [],
        'invalid_tasks': [],
        'errors': {}
    }

    for task_id, task_data in data_dict.items():
        errors = []

        # Проверка email_check
        if not task_data.get('email_check', False):
            errors.append("email_check is False")

        # Проверка mob_check
        if not task_data.get('mob_check', False):
            errors.append("mob_check is False")

        # Проверка task_status
        if task_data.get('task_status') != 'COMPLETED':
            errors.append(f"task_status is '{task_data.get('task_status')}', expected 'COMPLETED'")

        # Проверка task_duration_seconds
        duration = task_data.get('task_duration_seconds')
        if duration is not None and duration > 20:
            errors.append(f"task_duration_seconds is {duration} seconds, exceeds 20 seconds")

        # Запись результатов
        if errors:
            results['invalid_tasks'].append(task_id)
            results['errors'][task_id] = errors
        else:
            results['valid_tasks'].append(task_id)

    return results


if "__main__" == __name__:
    d = {'75ba4e6e-62c1-45b7-90c1-e841c822e04b': {'row_id': 'f2d1f7b6-9279-400e-9241-ceb4b8cf2d99', 'mob': '+79111388863',
                                              'email': 'ttgzohwj@lumns.com', 'start_time_msk': '2026-04-21 11:20:07',
                                              'start_time': '1776756007567',
                                              'file_name': 'Чертаново Центральное_200.csv',
                                              'projection_id': 'c67055d9-da4d-48d5-86e9-2ae7b72b23eb',
                                              'task_status': 'COMPLETED', 'task_duration_seconds': 21.124536,
                                              'task_time_created': '2026-04-21T07:20:07.219370Z',
                                              'task_time_updated': '2026-04-21T07:20:08.343906Z',
                                              'task_projection_title': 'Чертаново Центральное_актив',
                                              'task_type': 'FILE_IMPORT_PROJECTION', 'task_error_info': None,
                                              'task_uploaded_rows': 'Загружено строк: 200',
                                              'task_created_attr': 'Создано атрибутов: 0', 'row_id_mob': '+79111388863',
                                              'row_id_email': 'ttgzohwj@lumns.com', 'mob_check': True,
                                              'email_check': True, 'task_id': '75ba4e6e-62c1-45b7-90c1-e841c822e04b'},
     'fdc57ae7-ca1b-44f6-91dd-990f7c9fe169': {'row_id': None, 'mob': '+79972095520', 'email': 'pvbatbmu@ognim.com',
                                              'start_time_msk': '2026-04-21 13:10:43', 'start_time': '1776762643345',
                                              'file_name': 'Молжаниновский_50.csv',
                                              'projection_id': '9f7d9e02-6543-4e94-923b-8a1d66de4b11',
                                              'task_status': 'COMPLETED', 'task_duration_seconds': 0.862345,
                                              'task_time_created': '2026-04-21T09:10:39.340456Z',
                                              'task_time_updated': '2026-04-21T09:10:40.202801Z',
                                              'task_projection_title': 'Молжаниновский_актив',
                                              'task_type': 'FILE_IMPORT_PROJECTION', 'task_error_info': None,
                                              'task_uploaded_rows': 'Загружено строк: 50',
                                              'task_created_attr': 'Создано атрибутов: 0', 'row_id_mob': '+79972095520',
                                              'row_id_email': 'pvbatbmu@ognim.com', 'mob_check': True,
                                              'email_check': True, 'task_id': 'fdc57ae7-ca1b-44f6-91dd-990f7c9fe169'}}

    r = check_task_data(d)
    print(r)