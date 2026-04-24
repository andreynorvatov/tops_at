import os
# import pandas as pd
import requests

from datetime import datetime

HOST = "https://tops-stage.mos.ru"
DP_HOST = "http://10.126.145.36:3000"
Token = "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZTA1ZmNkZTEtMGUyMi00ZDI3LTgxM2ItYmM2NWJjY2E4N2MzIiwidG9rZW5faWQiOiI0NGE1YzgyMS0zYmYzLTQ1ZDEtYjg1YS04ZWZkNmY4NTQ2NjgiLCJyb2xlIjoiT1BFUkFUT1IiLCJleHAiOjE3NzcwMTgzOTl9.0IdXEi9UT2w1AQ9YEnKWgoJr_t2wx9JTW7nBkymgzCBS4Sgxor4wbpjX09JC09d-PpdTVgNIpEQAYB1zFCorgnEzB3Mr1NzbLUJMF9yfr1AAMfx8wiPQS4-bE5i57n_o"

class Row_BD:
    def __init__(self, data_dict):
        self.id = data_dict.get('id')
        self.row_id = data_dict.get('row_id')
        self.mob = data_dict.get('mob')
        self.email = data_dict.get('email')
        self.start_time = data_dict.get('start_time')
        self.file_name = data_dict.get('file_name')
        self.task_id = data_dict.get('task_id')
        self.projection_id = data_dict.get('projection_id')
        self.read_line = data_dict.get('read_line')

    def get_task(self, Token):
        headers = {'x-lamb-auth-token': Token}
        response = requests.get(f'{HOST}/api/mona/background_tasks/{self.task_id}', headers=headers)
        print(response.request.url)
        print(response.request.headers)
        print(response.json())
        return response.json()
    
    def get_raw(self, Token):
        headers = {'x-lamb-auth-token': Token}
        response = requests.get(f'{HOST}/api/chiara/projection_data/{self.projection_id}/{self.row_id}', headers=headers)
        return response.json()
    
    def get_raw_filter(self, Token):
        headers = {'x-lamb-auth-token': Token}
        params = {
        "filters": f"[[{{\"column\":\"mobil_nyj_telefon_1\",\"to_lower\":true,\"operator\":\"EQUAL\",\"value\":\"{self.mob}\"}}]]"
        }
        response = requests.get(f'{HOST}/api/chiara/projection_data/{self.projection_id}/', headers=headers, params=params)
        return response.json()
    
    def change_status(self):
        params = {'p_id': self.id}
        response = requests.post(f'{DP_HOST}/rpc/tops_checkupdate_set_read_line', json=params)
        return response.text

    def get_object(self, Token):
        if self.row_id is not None:
            objects = self.get_raw(Token)
            mob = objects["mobil_nyj_telefon_1"]
            email = objects["elektronnaja_pochta"]
        else:
            objects = self.get_raw_filter(Token=Token)
            mob = objects['items'][0]['mobil_nyj_telefon_1']
            email = objects['items'][0]['elektronnaja_pochta']
        result = {
            'mob': mob,
            'email': email
        }
        return result
    
    def check_object(self, object):
        result_mob = self.extract_digits(object.get('mob'))
        result_email = object.get('email')
        mob = self.extract_digits(self.mob)
        print(self.row_id)
        if result_mob != mob:
            print(f'Ошибка, не совпадение моб.номера. Task_id= {self.task_id}. Где {result_mob}!={mob}')
            return f'Ошибка {result_mob}!={mob}'
        elif result_email != self.email:
            print(f'Ошибка, не совпадение email. Task_id= {self.task_id}. Где {result_email}!={self.email}')
            return f'Ошибка {result_mob}!={mob}'
        else:
            print(f'Проверка task_id = {self.task_id} прошла успешно')
            return f'OK'


    def check_task(self, task):

        status = task.get("status")
        if status == 'COMPLETED':
            result = self.time_exec(task=task)
        else:
            result = f'Ошибка, Task status = {status}'
        return result

    def extract_digits(self, phone_str):
        return ''.join(ch for ch in phone_str if ch.isdigit())
    
    def time_exec(self, task):
        time_created = datetime.fromisoformat(task.get("time_created").replace('Z', '+00:00'))
        time_updated = datetime.fromisoformat(task.get("time_updated").replace('Z', '+00:00'))
        delta = time_updated - time_created
        result_second = delta.total_seconds()
        result = f'{result_second:.3f} секунд'
        return result

def save_result_to_csv(data):
    result = {
        'check_status': data['status_row'],
        'task_id': data['task_id'],
        'time_result': data['status'],
        'start_time': data['start_time'],
        'meta_info': data['meta_info'],
        'file_name': data['file_name'],
        'row_id': data['row_id']
    }
    df = pd.DataFrame(result)
    csv_file = 'result_check_import.csv'
    file_exists = os.path.isfile(csv_file)
    df.to_csv(csv_file, sep=';', mode='a', header=not file_exists, index=False, encoding='utf-8')

def main():
    response = requests.post(f'{DP_HOST}/rpc/tops_checkupdate_get_all')
    result = response.json()
    start = datetime.now()
    for item in result:
        row_obj = Row_BD(item)
        if row_obj.read_line is True:
            pass
        else:
            data = {}
            task = row_obj.get_task(Token=Token)
            status = row_obj.check_task(task=task)
            status_row = row_obj.check_object(row_obj.get_object(Token=Token))
    #
    #         data['status_row'] = status_row
    #         data['task_id'] = row_obj.task_id
    #         data['status'] = status
    #         data['start_time'] = row_obj.start_time
    #         data['meta_info'] = task.get('meta_info')
    #         data['file_name'] = row_obj.file_name
    #         data['row_id'] = row_obj.row_id
    #
    #         row_obj.change_status()
    #         if data:
    #             save_result_to_csv(data=data)
    # print(f'Время выполненения- {datetime.now() - start}')


if __name__ == "__main__":
    main()