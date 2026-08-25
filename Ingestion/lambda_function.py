import os
import boto3
from download import download_file
from upload import upload_s3
from util import get_prev_file_name, get_next_file_name, upload_bookmark


sns_client = boto3.client('sns')

def publish_alert(topic_arn, subject, message):
    try:
        sns_client.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    except Exception as e:
        print(f'Failed to publish SNS alert: {e}')  # not letting alerting itself crash the handler

def lambda_handler(event, context):
    bucket_name = os.environ.get('BUCKET_NAME')
    file_prefix = os.environ.get('FILE_PREFIX')
    bookmark_file = os.environ.get('BOOKMARK_FILE')
    baseline_file = os.environ.get('BASELINE_FILE')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    try:
        while True:
            prev_file_name = get_prev_file_name(bucket_name,file_prefix,bookmark_file,baseline_file)
            next_file_name = get_next_file_name(prev_file_name)

            download_res = download_file(next_file_name)
            
            if download_res.status_code == 404:
                print(f'Caught up till {prev_file_name}')
                break
            if download_res.status_code != 200:
                msg = f'Unexpected status {download_res.status_code} downloading {next_file_name}'
                print(msg)
                publish_alert(sns_topic_arn, 'GH Ingestion: download failed', msg)
                break
            
            upload_res = upload_s3(bucket_name,f'{file_prefix}/{next_file_name}',download_res.content)
            
            print(f'File {next_file_name} is succssfully processed')

            upload_bookmark(bucket_name,file_prefix,bookmark_file,next_file_name)

        return upload_res
    
    except Exception as e:
        msg = f'Ingestion Lambda failed: {e}'
        print(msg)
        publish_alert(sns_topic_arn, 'GH Ingestion: Lambda error', msg)
        raise  # re-raise so it still shows as a Lambda failure in CloudWatch too