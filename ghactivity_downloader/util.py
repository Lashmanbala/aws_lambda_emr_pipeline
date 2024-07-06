from datetime import datetime, timedelta

next_file ='2024-07-06-0.json.gz'

for i in range(24):
    date_part = next_file.split('.')[0]
    next_file = f"{datetime.strftime(datetime.strptime(date_part, '%Y-%M-%d-%H')+timedelta(hours=1), '%Y-%M-%d-%-H')}.json.gz"
    print(next_file)