import json
import os
from download import download_file
from upload import upload_s3

def lambda_handler(event, context):
    file = '2015-01-01-15.json.gz'

    bucket = os.environ.get('BUCKET_NAME')
    file_prefix = os.environ.get('FILE_PREFIX')

    download_res = download_file(file)
    upload_res = upload_s3(bucket,f'{file_prefix}/{file}',download_res.content)

    return {
        'statusCode': upload_res,
        'body': json.dumps('downloaded the file')
    }

print(lambda_handler(None, None))