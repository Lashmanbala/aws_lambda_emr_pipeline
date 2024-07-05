import requests

def download_file(file):
    res = requests.get(f'https://data.gharchive.org/{file}')
    return res

op = download_file('2015-01-01-15.json.gz')
print(op.status_code)