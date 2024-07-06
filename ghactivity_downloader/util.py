from datetime import datetime, timedelta
import requests

next_file ='2024-07-06-0.json.gz'

while True:
    res = requests.get(f'https://data.gharchive.org/{next_file}')
    if res.status_code != 200:
        break
    print(f'The status code for {next_file} is {res.status_code}')
    date_part = next_file.split('.')[0]
    next_file = f"{datetime.strftime(datetime.strptime(date_part, '%Y-%M-%d-%H')+timedelta(hours=1), '%Y-%M-%d-%-H')}.json.gz"
