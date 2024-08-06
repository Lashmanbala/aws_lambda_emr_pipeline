import boto3
import json

def create_iam_role(role_name, policy_arn_list):
    # Initialize the IAM client
    iam_client = boto3.client('iam')

    # Trust policy that allows the specified service to assume this role
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": 'lambda.amazonaws.com' 
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        # Create the role
        create_role_response = iam_client.create_role(
                                            RoleName=role_name,
                                            AssumeRolePolicyDocument=json.dumps(policy_document)
                                        )
        
        role_arn = create_role_response['Role']['Arn']
        print(f'Role "{role_name}" created successfully with ARN: {role_arn}')

    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f'Role "{role_name}" already exists.')
        create_role_response = iam_client.get_role(RoleName=role_name)

        # Attach the specified policies to the role
        for policy_arn in policy_arn_list:
            iam_client.attach_role_policy(
                                        RoleName=role_name,
                                        PolicyArn=policy_arn
                                    )
            print(f'Policy {policy_arn} attached to role "{role_name}".')

        return create_role_response

    except Exception as e:
        print(f'Error: {e}')

def create_lambda_function(bucket, folder, file_name, role_arn, env_variables_dict,func_name, handler):
    lambda_client = boto3.client('lambda')
    
    try:
        lambda_res = lambda_client.create_function(
                                    Code={
                                        'S3Bucket': bucket,
                                        'S3Key': f'{folder}/{file_name}'
                                        },
                                    Environment={
                                        'Variables': env_variables_dict
                                                },
                                    
                                    FunctionName=func_name,
                                    Handler=handler,
                                    MemorySize=512,
                                    Role=role_arn,
                                    Runtime='python3.10',      # as per the development environment
                                    Timeout=300
                                )
        print(f"successfully created {lambda_res['FunctionName']}")
        arn = lambda_res['FunctionArn']
        return arn
    except Exception as e:
        print(e)

def invoke_lambda_funtion(func_name):
    lambda_client = boto3.client('lambda')
    try:
        response = lambda_client.invoke(
                                        FunctionName=func_name,
                                        InvocationType='RequestResponse'
                                        )
        return response
    except Exception as e:
        print(e)

