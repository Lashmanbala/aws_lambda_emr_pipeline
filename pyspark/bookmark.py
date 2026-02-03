import boto3
from botocore.errorfactory import ClientError
from datetime import datetime, timedelta


def get_pattern(bucket_name,file_prefix,bookmark_file,baseline_file):
    s3_client = boto3.client('s3')
    try:
        bookmark_file = s3_client.get_object(Bucket=bucket_name,
                                    Key=f'{file_prefix}/{bookmark_file}')
        prev_day = bookmark_file['Body'].read().decode('utf-8')
        next_day = f"{datetime.strftime(datetime.strptime(prev_day, '%Y-%m-%d')+timedelta(days=1), '%Y-%m-%d')}" # only the date part
        pattern = next_day
    except ClientError as e:
        if e.response['Error']['Code']=='NoSuchKey':
            pattern = baseline_file.split('.')[0][:10] # only the date part
        else:
            raise
    return pattern


def upload_bookmark(bucket_name,file_prefix,bookmark_file,bookmark_content):
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket_name,
                        Key=f'{file_prefix}/{bookmark_file}',
                        Body=bookmark_content.encode('utf-8'))

