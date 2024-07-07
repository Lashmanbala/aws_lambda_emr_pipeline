from datetime import datetime, timedelta
import boto3
from botocore.errorfactory import ClientError

def get_client():
    return boto3.client('s3')


def get_prev_file_name(bucket_name,file_prefix,bookmark_file,baseline_file):
    s3_client = get_client()
    try:
        bookmark_file = s3_client.get_object(Bucket=bucket_name,
                                    Key=f'{file_prefix}/{bookmark_file}')
        prev_file = bookmark_file['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code']=='NoSuchKey':
            prev_file = baseline_file
        else:
            raise
    return prev_file

def get_next_file_name(prev_file):
    date_part = prev_file.split('.')[0]
    next_file = f"{datetime.strftime(datetime.strptime(date_part, '%Y-%M-%d-%H')+timedelta(hours=1), '%Y-%M-%d-%-H')}.json.gz"
    return next_file

def upload_bookmark(bucket_name,file_prefix,bookmark_file,bookmark_content):
    s3_client = get_client()
    s3_client.put_object(Bucket=bucket_name,
                        Key=f'{file_prefix}/{bookmark_file}',
                        Body=bookmark_content.encode('utf-8'))

