from datetime import datetime, timedelta
import requests, boto3, os
from botocore.errorfactory import ClientError

s3_client = boto3.client('s3')

baseline_file ='2024-07-06-0.json.gz'

while True:
    try:
        bookmark_file = s3_client.get_object(Bucket='gh-raw-bucket',
                                    Key='sandbox/bookmark')
        prev_file = bookmark_file['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code']=='NoSuchKey':
            prev_file = baseline_file
        else:
            raise

    date_part = prev_file.split('.')[0]
    next_file = f"{datetime.strftime(datetime.strptime(date_part, '%Y-%M-%d-%H')+timedelta(hours=1), '%Y-%M-%d-%-H')}.json.gz"

    res = requests.get(f'https://data.gharchive.org/{next_file}')

    if res.status_code != 200:
        break
    print(f'The status code for {next_file} is {res.status_code}')

    bookmark_content = next_file
    s3_client.put_object(Bucket='gh-raw-bucket',
                                    Key='sandbox/bookmark',
                                    Body=bookmark_content.encode('utf-8'))

