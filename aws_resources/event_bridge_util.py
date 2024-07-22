def create_event_bridge_rule(rule_name):
    event_bridge_client = boto3.client('events')
    try:
        event_rule_response = event_bridge_client.put_rule(
                Name=rule_name,
                ScheduleExpression='rate(60 minutes)',
                State='ENABLED',
                Description='Trigger ghactivity-download-function hourly'
                )
        return event_rule_response
    except Exception as e:
        print(e)

def add_target_to_rule(rule_name, lambda_arn, rule_arn):
    eventbridge_client = boto3.client('events')
    lambda_client = boto3.client('lambda')
    try:
        put_targets_response = eventbridge_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                'Id': '1',
                'Arn': lambda_arn,
            }
        ]
        )
        # Add permission for EventBridge to invoke the Lambda function
        lambda_client.add_permission(
            FunctionName=lambda_arn,
            StatementId='EventBridgeInvokeLambda',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=rule_arn
        )
        
        return put_targets_response
    except Exception as e:
        print(e)