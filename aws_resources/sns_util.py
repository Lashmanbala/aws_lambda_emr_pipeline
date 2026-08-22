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

def subscribe_email(topic_arn, email):
    sns_client = boto3.client('sns')
    try:
        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email
        )
        print(f'Subscription pending confirmation for {email} — check inbox to confirm')
        return response
    except Exception as e:
        print(e)

def allow_eventbridge_to_publish(topic_arn, rule_arn):
    sns_client = boto3.client('sns')
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowEventBridgePublish",
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "SNS:Publish",
            "Resource": topic_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": rule_arn}}
        }]
    }
    try:
        response = sns_client.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName='Policy',
            AttributeValue=json.dumps(policy)
        )
        print(f'EventBridge granted publish permission on {topic_arn}')
        return response
    except Exception as e:
        print(e)