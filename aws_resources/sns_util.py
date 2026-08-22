import boto3
import json

def create_sns_topic(topic_name):
    sns_client = boto3.client('sns')
    try:
        response = sns_client.create_topic(Name=topic_name)
        print(f'Topic "{topic_name}" created with ARN: {response["TopicArn"]}')
        return response
    except Exception as e:
        print(e)