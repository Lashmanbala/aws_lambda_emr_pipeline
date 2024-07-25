from s3_util import create_bucket, upload_s3
from lambda_util import create_iam_role, create_lambda_function, invoke_lambda_funtion
from event_bridge_util import create_event_bridge_rule, add_target_to_rule

def create_and_upload_s3():
    print('Creating bucket')
    bucket='github-bkt1'
    bucket_res = create_bucket(bucket)

    if bucket_res['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f'{bucket} created successfully')

    ghactivity_lambda_zipfile = '/home/bala/code/projects/github_activity_project/ghactivity_downloader/ghactivity_downloader_for_lambda.zip'
    emr_lambda_zipfile = '/home/bala/code/projects/github_activity_project/aws_resources/lambda_for_emr.zip'
    spark_app_zipfile = '/home/bala/code/projects/github_activity_project/pyspark/github_spark_app.zip'
    spark_app_file = '/home/bala/code/projects/github_activity_project/pyspark/app.py'
    file_path_list = [ghactivity_lambda_zipfile, emr_lambda_zipfile, spark_app_zipfile, spark_app_file]

    folder='zipfiles'

    print('Uploading files')
    for file_path in file_path_list:
        file_name = file_path.split('/')[-1]

        if file_name[-3:] == 'zip':
            body=open(file_path, 'rb').read()
        else:
            body=open(file_path, 'r').read()

        upload_res = upload_s3(bucket,folder,file_name,body)
        if upload_res['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f'{file_name} uploded successfully')

def create_downloder_lambda():
    print('Creating iam role for downloder_lambda')

    role_name = 'lambda-s3-full-access-role'
    bucket='github-bkt1'
    folder='zipfiles'
    ghactivity_lambda_zipfile = '/home/bala/code/projects/github_activity_project/ghactivity_downloader/ghactivity_downloader_for_lambda.zip'
    file_name = ghactivity_lambda_zipfile.split('/')[-1]

    lambda_basic_execution_arn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
    s3_full_access_arn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess'
    policy_arn_list = [lambda_basic_execution_arn, s3_full_access_arn]

    create_role_response = create_iam_role(role_name, policy_arn_list)
    lambda_s3_role_arn = create_role_response['Role']['Arn']
    print(f'IAM role created with ARN: {lambda_s3_role_arn}')

    env_variables_dict = {'BUCKET_NAME' : bucket,
                        'FILE_PREFIX' : 'landing',
                        'BOOKMARK_FILE' : 'bookmark',
                        'BASELINE_FILE' : '2024-07-21-0.json.gz'
                        }
    func_name='ghactivity-download-function'
    handler = 'lambda_function.lambda_handler'

    print(f'Creating lambda function {func_name}')
    lambda_arn = create_lambda_function(bucket, folder, file_name, lambda_s3_role_arn, env_variables_dict,func_name,handler)
    return lambda_arn


def shedule_downloder_lambda(lambda_arn):
    print('Scheduling downloader lambda')

    rate = 'rate(60 minutes)'
    rule_name = 'HourlyGhactivityDownloadRule'

    event_rule_response = create_event_bridge_rule(rule_name, rate)
    print(f"Successfully event rule created for ghactivity_downloader function with arn: {event_rule_response['RuleArn']}")

    rule_arn = event_rule_response['RuleArn']
    
    add_target_to_rule(rule_name, lambda_arn, rule_arn)
    print("Successfully lambda target added to event rule")


def create_emr_lambda():
    print('Creating iam role for emr_lambda')

    role_name = 'lambda-s3-emr-iam-access-role'
    lambda_basic_execution_arn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
    s3_full_access_arn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess'
    iam_full_access_arn = 'arn:aws:iam::aws:policy/IAMFullAccess'
    emr_full_access_arn = 'arn:aws:iam::aws:policy/AmazonElasticMapReduceFullAccess'
    policy_arn_list = [lambda_basic_execution_arn, s3_full_access_arn, iam_full_access_arn, emr_full_access_arn]

    create_role_response = create_iam_role(role_name, policy_arn_list)

    lambda_s3_iam_emr_role_arn = create_role_response['Role']['Arn']
    print(f'IAM role created with ARN: {lambda_s3_iam_emr_role_arn}')

    bucket = 'github-bkt1'
    folder = 'zipfiles'
    emr_lambda_zipfile = '/home/bala/code/projects/github_activity_project/aws_resources/lambda_for_emr.zip'
    file_name = emr_lambda_zipfile.split('/')[-1]
    env_variables_dict = {
        'BUCKET_NAME': 'github-bkt1',
        'INSTANCE_TYPE': 'm4.xlarge',
        'CORE_INSTANCE_COUNT': '1',
        'SPARK_ENV_DICT': '{"ENVIRON":"PROD", "SRC_DIR":"s3://github-bkt1/landing/", "SRC_FILE_FORMAT":"json", "TGT_DIR":"s3://github-bkt1/raw/", "TGT_FILE_FORMAT":"parquet", "SRC_FILE_PATTERN":"2024-07-21"}',
        'ZIP_FILE_PATH': 's3://github-bkt1/zipfiles/github_spark_app.zip',
        'APP_FILE_PATH': 's3://github-bkt1/zipfiles/app.py'
    }

    func_name = 'lambda_function_for_emr'
    handler = 'lambda_function_for_emr.lambda_handler'

    print(f'Creating lambda function {func_name}')
    lambda_arn = create_lambda_function(bucket,folder,file_name,lambda_s3_iam_emr_role_arn,env_variables_dict,func_name,handler)
    return lambda_arn

def schedule_emr_lambda(lambda_arn):
    print('Scheduling emr lambda')

    rate = 'cron(0 0 * * ? *)'
    rule_name = 'DailyEmrRule'
    
    event_rule_response = create_event_bridge_rule(rule_name, rate)
    print('Successfully event rule created for lambda_for_emr function')

    rule_arn = event_rule_response['RuleArn']
    
    add_target_to_rule(rule_name, lambda_arn, rule_arn)
    print('Successfully lambda target added to event rule')

if __name__ == '__main__':
    create_and_upload_s3()

    downloader_lambda_arn = create_downloder_lambda()
    shedule_downloder_lambda(downloader_lambda_arn)

    emr_lambda_arn = create_emr_lambda()
    schedule_emr_lambda(emr_lambda_arn)
