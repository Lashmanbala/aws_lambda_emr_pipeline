import boto3
import json
import time

def create_emr_ec2_instance_profile():
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
        time.sleep(5)
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


    # Create instance profile
    try:
        iam_client.create_instance_profile(InstanceProfileName='EMR_EC2_InstanceProfile')
        time.sleep(5)
        print('Instance profile "EMR_EC2_InstanceProfile" created successfully.')

    except iam_client.exceptions.EntityAlreadyExistsException:
        print('Instance profile "EMR_EC2_InstanceProfile" already exists.')

    # Check if the role is already attached to the instance profile
    response = iam_client.get_instance_profile(InstanceProfileName='EMR_EC2_InstanceProfile')
    attached_roles = [role['RoleName'] for role in response['InstanceProfile']['Roles']]

    if 'EMR_EC2_Role' not in attached_roles:
        iam_client.add_role_to_instance_profile(
            InstanceProfileName='EMR_EC2_InstanceProfile',
            RoleName='EMR_EC2_Role'
        )
        print('Role "EMR_EC2_Role" added to instance profile "EMR_EC2_InstanceProfile".')
    else:
        print('Role "EMR_EC2_Role" is already attached to instance profile "EMR_EC2_InstanceProfile".')

    time.sleep(5)
    return response["InstanceProfile"]["InstanceProfileName"]
    

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
        role_name = role_response['Role']['RoleName']
        print(f'Role "EMR_Service_Role" created successfully with ARN: {role_name}')

    except iam_client.exceptions.EntityAlreadyExistsException:
        print('Role "EMR_Service_Role" already exists.')
        role_name = iam_client.get_role(RoleName='EMR_Service_Role')['Role']['RoleName']

    # Attach the AmazonElasticMapReduceRole policy
    emr_policy_arn = 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
    iam_client.attach_role_policy(
                                RoleName='EMR_Service_Role',
                                PolicyArn=emr_policy_arn
                            )
    print(f'Policy {emr_policy_arn} attached to role "{role_name}".')
    return role_name

def create_emr_cluster(bucket_name, instance_type, core_instance_count, bootstrap_file_path):
        
    emr_client = boto3.client('emr')

    bootstrap_actions = [
        {
            'Name': 'Install boto3',
            'ScriptBootstrapAction': {
                'Path': bootstrap_file_path
            }
        }
    ]
    # Define the cluster
    cluster_config = {
        'Name': 'ghactivity_cluster',
        'LogUri': f's3://{bucket_name}/logs/emr_logs/',
        'ReleaseLabel': 'emr-7.12.0',  # Replace with your desired EMR release version
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': core_instance_count
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        'Applications': [
            {'Name': 'Hadoop'},
            {'Name': 'Spark'}
        ],
        'JobFlowRole': 'EMR_EC2_InstanceProfile',  # Ensure this role exists or create it 
        'ServiceRole': 'EMR_Service_Role',  # Ensure this role exists or create it
        'AutoTerminationPolicy': {
            'IdleTimeout': 300
        },
        'BootstrapActions': bootstrap_actions
    }

    # Create the cluster with the step
    emr_response = emr_client.run_job_flow(**cluster_config)
    return emr_response


def add_spark_step(cluster_id, env_vars_dict, zip_file_path, app_file_path):
    emr_client = boto3.client('emr')

    env_vars = env_vars_dict    
    env_conf_args = []
    for key, value in env_vars.items():
        # env_conf_args.extend(['--conf', f'spark.yarn.appMasterEnv.{key}={value}'])
        # Driver (ApplicationMaster)
        env_conf_args.extend([
            '--conf', f'spark.yarn.appMasterEnv.{key}={value}'
        ])

        # Executors (MANDATORY)
        env_conf_args.extend([
            '--conf', f'spark.executorEnv.{key}={value}'
        ])

    # Define the Spark submit step
    step_config = {
        'Name': 'Spark submit step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                "--conf", "spark.executor.instances=3",
                "--conf", "spark.executor.cores=1",
                "--conf", "spark.executor.memory=2g",
                "--conf", "spark.executor.memoryOverhead=1g",
                "--conf", "spark.driver.memory=2g",
                "--conf", "spark.sql.shuffle.partitions=8",
                "--conf", "spark.sql.adaptive.enabled=false"
                ] + env_conf_args +[
                '--py-files', zip_file_path,
                app_file_path
            ]
        }
    }
    try:
        # Add the step to the cluster
        step_response = emr_client.add_job_flow_steps(
                                                    JobFlowId=cluster_id,
                                                    Steps=[step_config]
                                                )

        return step_response
    except Exception as e:
        print(e)

def test():
    env_vars = {"ENVIRON":"PROD", 
            "SRC_DIR":"s3://github-activity-bucket-123/landing/", 
            "SRC_FILE_FORMAT":"json", 
            "TGT_DIR":"s3://github-activity-bucket-123/raw/", 
            "TGT_FILE_FORMAT":"parquet",
            "BUCKET_NAME":"github-activity-bucket-123",
            "FILE_PREFIX":"raw",
            "BOOKMARK_FILE":"bookmark",
            "BASELINE_FILE":"2026-01-27-0.json.gz"}

    env_conf_args = []
    for key, value in env_vars.items():
        env_conf_args.extend(['--conf', f'spark.yarn.appMasterEnv.{key}={value}'])

    step_config = {
            'Name': 'Spark submit step',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster'
                    ] + env_conf_args +[
                    '--py-files', 'zip_file_path',
                    'app_file_path'
                ]
            }
        }

    print(step_config['HadoopJarStep']['Args'])

# test()

# ['spark-submit', '--deploy-mode', 'cluster', '--conf', 'spark.yarn.appMasterEnv.ENVIRON=PROD', '--conf', 'spark.yarn.appMasterEnv.SRC_DIR=s3://github-activity-bucket/landing/', '--conf', 'spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json', '--conf', 'spark.yarn.appMasterEnv.TGT_DIR=s3://github-activity-bucket/raw/', '--conf', 'spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet', '--conf', 'spark.yarn.appMasterEnv.BUCKET_NAME=github-activity-bucket', '--conf', 'spark.yarn.appMasterEnv.FILE_PREFIX=raw', '--conf', 'spark.yarn.appMasterEnv.BOOKMARK_FILE=bookmark', '--conf', 'spark.yarn.appMasterEnv.BASELINE_FILE=2024-09-12-0.json.gz', '--py-files', 'zip_file_path', 'app_file_path']

# ['spark-submit', '--deploy-mode', 'cluster', '--conf', 'spark.yarn.appMasterEnv.ENVIRON=PROD', '--conf', 'spark.yarn.appMasterEnv.SRC_DIR=s3://github-activity-bucket-123/landing/', '--conf', 'spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json', '--conf', 'spark.yarn.appMasterEnv.TGT_DIR=s3://github-activity-bucket-123/raw/', '--conf', 'spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet', '--conf', 'spark.yarn.appMasterEnv.BUCKET_NAME=github-activity-bucket-123', '--conf', 'spark.yarn.appMasterEnv.FILE_PREFIX=raw', '--conf', 'spark.yarn.appMasterEnv.BOOKMARK_FILE=bookmark', '--conf', 'spark.yarn.appMasterEnv.BASELINE_FILE=2026-01-27-0.json.gz', '--py-files', 'zip_file_path', 'app_file_path']
