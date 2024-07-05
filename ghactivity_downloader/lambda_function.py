import json
from download import download_file

def lambda_handler(event, context):
    download_res = download_file('2015-01-01-15.json.gz')
    return {
        'statusCode': download_res.status_code,
        'body': json.dumps('downloaded the file')
    }
print(lambda_handler(None, None))