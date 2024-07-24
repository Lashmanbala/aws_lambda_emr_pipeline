import boto3
import json
import os
from emr_util import create_emr_ec2_instance_profile, create_emr_service_role, create_emr_cluster, add_spark_step

def lambda_handler(event, context):
    emr_ec2_instance_profile = create_emr_ec2_instance_profile()
    print(f' instance profile {emr_ec2_instance_profile} is created')

    emr_service_role = create_emr_service_role()
    print(f'IAM role {emr_service_role} is created')

    bucket_name= os.environ.get('BUCKET_NAME')
    instance_type=os.environ.get('INSTANCE_TYPE')
    core_instance_count= int(os.environ.get('CORE_INSTANCE_COUNT'))

    emr_response = create_emr_cluster(bucket_name, instance_type, core_instance_count)
    emr_cluster_id = emr_response['JobFlowId']
    print(f'cluster is created with the id {emr_cluster_id}')

    env_vars_dict= os.environ.get('SPARK_ENV_DICT')
    zip_file_path = os.environ.get('ZIP_FILE_PATH')
    app_file_path = os.environ.get('APP_FILE_PATH')

    step_response = add_spark_step(emr_cluster_id,env_vars_dict, zip_file_path, app_file_path)
    print(f"Successfully step is added to cluster. step id is {step_response['StepIds'][0]}")