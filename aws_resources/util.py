import boto3
import json

def create_bucket(bkt_name):
    s3_client = boto3.client('s3')
    res = s3_client.create_bucket(Bucket=bkt_name)
        
    return res

def upload_s3(bucket,folder,file_name,body):
    s3_client = boto3.client('s3')
    res = s3_client.put_object(
            Bucket=bucket,
            Key=f'{folder}/{file_name}',
            Body=body
            )
    return res


def create_iam_role(role_name, trusted_service, policy_arn_list):
    # Initialize the IAM client
    iam_client = boto3.client('iam')

    # Trust policy that allows the specified service to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": trusted_service
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        # Create the role
        create_role_response = iam_client.create_role(
                                            RoleName=role_name,
                                            AssumeRolePolicyDocument=json.dumps(trust_policy),
                                            Description="AWS Lambda role"
                                        )
        
        role_arn = create_role_response['Role']['Arn']
        print(f'Role "{role_name}" created successfully with ARN: {role_arn}')

        # Attach the specified policies to the role
        for policy_arn in policy_arn_list:
            iam_client.attach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy_arn
                        )
            print(f'Policy {policy_arn} attached to role "{role_name}".')

        return role_arn

    except Exception as e:
        print(f'Error: {e}')

def create_lambda_function(bucket, folder, file_name, role_arn, env_variables_dict):
    lambda_client = boto3.client('lambda')
    try:
        lambda_res = lambda_client.create_function(
                                    Code={
                                        'S3Bucket': bucket,
                                        'S3Key': f'{folder}/{file_name}'
                                        },
                                    Description='Downloading ghactivity',
                                    Environment={
                                        'Variables': env_variables_dict
                                                },
                                    
                                    FunctionName='ghactivity-download-function',
                                    Handler='lambda_function.lambda_handler',
                                    MemorySize=256,
                                    Role=role_arn,
                                    Runtime='python3.10',      
                                    Timeout=60
                                )
        return lambda_res
    except Exception as e:
        print(e)
