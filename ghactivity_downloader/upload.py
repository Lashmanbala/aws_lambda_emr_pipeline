import boto3
import requests

file = '2015-01-01-15.json.gz'
res = requests.get(f'https://data.gharchive.org/{file}')

s3_client = boto3.client('s3')
upload_res = s3_client.put_object(Bucket='gh-raw-bucket',
                                  Key = file,
                                  Body = res.content)
print(upload_res)