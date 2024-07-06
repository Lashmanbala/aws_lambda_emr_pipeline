import json
import os
from download import download_file
from upload import upload_s3

def lambda_handler(event, context):
    file = '2015-01-01-17.json.gz'

    bucket = os.environ.get('BUCKET_NAME')
    download_res = download_file(file)
    upload_res = upload_s3(bucket,file,download_res.content)

    return {
        'statusCode': upload_res,
        'body': json.dumps('downloaded the file')
    }

print(lambda_handler(None, None))