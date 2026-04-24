import requests
from pprint import pprint
from settings import settings

TOPS_URL = settings.TOPS_URL
TOKEN = settings.TOKEN


headers = {'x-lamb-auth-token': TOKEN}
# response = requests.get(f'{TOPS_URL}/api/mona/background_tasks/75ba4e6e-62c1-45b7-90c1-e841c822e04b', headers=headers)

mob = '+7 997 209-55-20'
projection_id = '9f7d9e02-6543-4e94-923b-8a1d66de4b11'
params = {
    "filters": f"[[{{\"column\":\"mobil_nyj_telefon_1\",\"to_lower\":true,\"operator\":\"EQUAL\",\"value\":\"{mob}\"}}]]"
}
response = requests.get(f'{TOPS_URL}/api/chiara/projection_data/{projection_id}/', headers=headers, params=params)

pprint(response.json())
print(response.url)
