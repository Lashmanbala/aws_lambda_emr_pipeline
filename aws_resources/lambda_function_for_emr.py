import boto3
import json
from emr_util import create_emr_ec2_instance_profile, create_emr_service_role, create_emr_cluster, add_spark_step

def lambda_handler(event, context):
    emr_ec2_instance_profile = create_emr_ec2_instance_profile()
    print(f' instance profile {emr_ec2_instance_profile} is created')

    emr_service_role = create_emr_service_role()
    print(f'IAM role {emr_service_role} is created')

    bucket_name= 'github-bkt'
    instance_type='m4.xlarge'
    core_instance_count=1

    emr_cluster_id = create_emr_cluster(bucket_name, instance_type, core_instance_count)
    print(f'cluster is created with the id {emr_cluster_id}')

    env_vars_dict= {
            'ENVIRON':'PROD',
            'SRC_DIR':'s3://github-bkt/landing/',
            'SRC_FILE_FORMAT':'json',
            'TGT_DIR':'s3://github-bkt/raw/',
            'TGT_FILE_FORMAT':'parquet',
            'SRC_FILE_PATTERN':'2024-07-21',
    }
    zip_file_path = 's3://github-bkt/zipfiles/github_spark_app.zip'
    app_file_path = 's3://github-bkt/zipfiles/app.py'

    spark_step = add_spark_step(emr_cluster_id,env_vars_dict, zip_file_path, app_file_path)
    print(f'Successfully step is added to cluster. step id is {spark_step}')