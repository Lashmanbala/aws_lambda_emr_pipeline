import boto3
from botocore.errorfactory import ClientError
from datetime import datetime, timedelta

bucket_name = 'github-bkt1'
file_prefix = 'zipfiles1'
bookmark_file = 'bookmark'
baseline_file = '2024-07-26-0.json.gz'
def get_prev_day(bucket_name,file_prefix,bookmark_file,baseline_file):
    s3_client = boto3.client('s3')
    try:
        bookmark_file = s3_client.get_object(Bucket=bucket_name,
                                    Key=f'{file_prefix}/{bookmark_file}')
        prev_day = bookmark_file['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code']=='NoSuchKey':
            prev_day = baseline_file.split('.')[0][:-2]
        else:
            raise
    return prev_day

def get_next_day(prev_day):
    next_day = f"{datetime.strftime(datetime.strptime(prev_day, '%Y-%M-%d')+timedelta(days=1), '%Y-%M-%d')}"
    return next_day

def upload_bookmark(bucket_name,file_prefix,bookmark_file,bookmark_content):
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket_name,
                        Key=f'{file_prefix}/{bookmark_file}',
                        Body=bookmark_content.encode('utf-8'))

while True:
    prev_day = get_prev_day(bucket_name,file_prefix,bookmark_file,baseline_file)
    print(prev_day)
    print(type(prev_day))
    nxt_day = get_next_day(prev_day)
    print(nxt_day)
    print(type(nxt_day))
    nd = datetime.strptime(nxt_day, '%Y-%m-%d').date()
    print(nd)
    print(type(nd))
    if datetime.strptime(nxt_day, '%Y-%m-%d').date() == datetime.today().date():
        print('its today')
        break
    upload_bookmark(bucket_name,file_prefix,bookmark_file,nxt_day)
