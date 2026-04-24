import requests
import csv

URL = "tops-stage.mos.ru"
DATABASES_ID = "84a1993a-c55c-4e7f-9577-5d6199934056"
TABLE_ID = "3b603b70-f816-4f31-9871-8bfd50fd4504"
TOKEN = "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiOTUzN2RlMTQtOTAyOC00ZjIxLThkNDQtOTRlMDVkMDRmNzBjIiwidG9rZW5faWQiOiIxMzU1ZDY1ZC00MWYxLTRjMTktOGNiYi1mNmFlMDUxMTliZDIiLCJyb2xlIjoiT1BFUkFUT1IiLCJleHAiOjE3NzM5ODgxNzZ9.wKmKMj7vOIIQ9pbMXJ7w0uz7Ix7xSk5nnhg4n_4xwBWq6iE8IauPF68Em-7wJHnMlBnmcjbtKDgPJ_4YTaxjEBQC0PdiaPyZ1js-ZRA_oUHHxe1MTUZpsyyNpTrWgfvQ"

# URL с параметрами запроса
url = f"https://{URL}/api/chiara/table_data/{TABLE_ID}"

# Параметры запроса
params = {
    "limit": 1,
    "offset": 2,
    "sorting": "",
    "filters": '[[{"column":"rajon_razvitija","to_lower":true,"operator":"EQUAL","value":"00b23989-43bd-4420-a61d-0d79c90016e2"}]]'
}

# Куки (обязательно)
cookies = {
    "session-cookie": "189deb34d7fe391cff26030abeb261f53290c41419132b64ad6e907523531e8464e61f2088bdb438b7d10264735b54bf"
}

# Минимальный набор заголовков
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": f"https://{URL}/databases/{DATABASES_ID}/{TABLE_ID}",
    "x-lamb-auth-token": TOKEN
}


# Функция для получения всех _id и записи в CSV
def save_ids_to_csv(data, filename="row_ids.csv"):
    """
    Извлекает все _id из items ответа и ДОЗАПИСЫВАЕТ их в CSV файл
    """
    items = data.get('items', [])
    if not items:
        print("Нет элементов для обработки")
        return

    # Проверяем, существует ли файл и не пустой ли он
    file_exists = False
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            file_exists = f.read(1) != ''  # Проверяем, что файл не пустой
    except FileNotFoundError:
        file_exists = False

    # Открываем файл в режиме дозаписи
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Если файл новый или пустой, записываем заголовок
        if not file_exists:
            writer.writerow(['row_id'])
            print(f"Создан новый файл {filename} с заголовком")

        # Дозаписываем все _id
        for item in items:
            writer.writerow([item.get('_id', '')])

    print(f"Дозаписано {len(items)} ID в файл {filename}")


response = requests.get(url, params=params, cookies=cookies, headers=headers)
print(f"Статус код: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print("Полученные данные:")
    # print(data)

    # Сохраняем все _id в CSV
    save_ids_to_csv(data, "data/row_ids.csv")
else:
    print(f"Ошибка при выполнении запроса: {response.status_code}")