# import boto3
# from botocore.errorfactory import ClientError
from datetime import datetime, timedelta

# bucket_name = 'github-bkt1'
# file_prefix = 'zipfiles1'
bookmark_file = 'bookmark'
baseline_file = '2015-01-01-0.json.gz'
# def get_prev_day(bucket_name,file_prefix,bookmark_file,baseline_file):
#     s3_client = boto3.client('s3')
#     try:
#         bookmark_file = s3_client.get_object(Bucket=bucket_name,
#                                     Key=f'{file_prefix}/{bookmark_file}')
#         prev_day = bookmark_file['Body'].read().decode('utf-8')
#     except ClientError as e:
#         if e.response['Error']['Code']=='NoSuchKey':
#             prev_day = baseline_file.split('.')[0][:-2]
#         else:
#             raise
#     return prev_day

# def get_next_day(prev_day):
#     next_day = f"{datetime.strftime(datetime.strptime(prev_day, '%Y-%M-%d')+timedelta(days=1), '%Y-%M-%d')}"
#     return next_day

# def upload_bookmark(bucket_name,file_prefix,bookmark_file,bookmark_content):
#     s3_client = boto3.client('s3')
#     s3_client.put_object(Bucket=bucket_name,
#                         Key=f'{file_prefix}/{bookmark_file}',
#                         Body=bookmark_content.encode('utf-8'))

import os
from datetime import datetime, timedelta

def get_prev_day(local_directory, bookmark_file, baseline_file):
    bookmark_path = os.path.join(local_directory, bookmark_file)
    try:
        with open(bookmark_path, 'r') as f:
            prev_day = f.read().strip()
    except FileNotFoundError:
        prev_day = baseline_file.split('.')[0][:-2]
    return prev_day

def get_next_day(prev_day):
    next_day = f"{datetime.strftime(datetime.strptime(prev_day, '%Y-%m-%d') + timedelta(days=1), '%Y-%m-%d')}"
    return next_day

def upload_bookmark(local_directory, bookmark_file, bookmark_content):
    bookmark_path = os.path.join(local_directory, bookmark_file)
    with open(bookmark_path, 'w') as f:
        f.write(bookmark_content)

# Example usage:
# local_directory = '/home/bala/code/projects/github_activity_project/pyspark/'
# bookmark_file = 'bookmark.txt'
# baseline_file = '2015-01-01-0.json.gz'

# prev_day = get_prev_day(local_directory, bookmark_file, baseline_file)
# next_day = get_next_day(prev_day)
# upload_bookmark(local_directory, bookmark_file, next_day)
