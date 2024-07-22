import boto3
import json

def create_emr_ec2_role():
    iam_client = boto3.client('iam')

    # Define the trust relationship policy
    role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # Create the IAM role
    try:
        role_response = iam_client.create_role(
            RoleName='EMR_EC2_Role',
            AssumeRolePolicyDocument=json.dumps(role_policy_document),
            Description='Role for EMR EC2 instances'
        )
        role_arn = role_response['Role']['Arn']
        print(f'Role "EMR_EC2_Role" created successfully with ARN: {role_arn}')
    except iam_client.exceptions.EntityAlreadyExistsException:
        print('Role "EMR_EC2_Role" already exists.')
        role_arn = iam_client.get_role(RoleName='EMR_EC2_Role')['Role']['Arn']

    # Attach the AmazonElasticMapReduceforEC2Role and the s3 fullaccess policy
    emr_policy_arn = 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'
    s3_policy_arn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess'
    arn_list = [emr_policy_arn, s3_policy_arn]

    for arn in arn_list:    
        iam_client.attach_role_policy(
            RoleName='EMR_EC2_Role',
            PolicyArn=arn
        )
        print(f'Policy {arn} attached to role "EMR_EC2_Role".')


    return role_arn


def create_emr_service_role():
    iam_client = boto3.client('iam')

    # Define the trust relationship policy
    role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "elasticmapreduce.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # Create the IAM role
    try:
        role_response = iam_client.create_role(
            RoleName='EMR_Service_Role',
            AssumeRolePolicyDocument=json.dumps(role_policy_document),
            Description='Role for EMR service'
        )
        role_arn = role_response['Role']['Arn']
        print(f'Role "EMR_Service_Role" created successfully with ARN: {role_arn}')
    except iam_client.exceptions.EntityAlreadyExistsException:
        print('Role "EMR_Service_Role" already exists.')
        role_arn = iam_client.get_role(RoleName='EMR_Service_Role')['Role']['Arn']

    # Attach the AmazonElasticMapReduceRole policy
    emr_policy_arn = 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
    iam_client.attach_role_policy(
        RoleName='EMR_Service_Role',
        PolicyArn=emr_policy_arn
    )
    print(f'Policy {emr_policy_arn} attached to role "EMR_Service_Role".')

    return role_arn

# Create the EMR_DefaultRole
role_arn = create_emr_service_role()
print(f'IAM role created with ARN: {role_arn}')
