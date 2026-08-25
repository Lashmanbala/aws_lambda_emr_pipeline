from s3_util import create_bucket, upload_s3
from lambda_util import create_iam_role, create_lambda_function, invoke_lambda_funtion
from event_bridge_util import create_event_bridge_rule, add_target_to_rule,  create_event_pattern_rule, add_sns_target_to_rule
from sns_util import create_sns_topic, subscribe_email, allow_eventbridge_to_publish
import dotenv
import os

def create_and_upload_s3():
    print('Creating bucket')
    bucket='github-activity-bucket-123'
    bucket_res = create_bucket(bucket)

    if bucket_res['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f'{bucket} created successfully')

    ghactivity_lambda_zipfile = os.environ.get('ghactivity_lambda_zipfile') # local file path
    emr_lambda_zipfile = os.environ.get('emr_lambda_zipfile')
    spark_app_zipfile = os.environ.get('spark_app_zipfile')
    spark_app_file = os.environ.get('spark_app_file')
    bootstrap_file = os.environ.get('bootstrap_file')
    file_path_list = [ghactivity_lambda_zipfile, emr_lambda_zipfile, spark_app_zipfile, spark_app_file, bootstrap_file]

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

def create_ingestion_failure_alert():
    print('Creating SNS topic for ingestion Lambda failure alerts')
    topic_res = create_sns_topic('ghactivity-ingestion-failure-alerts')
    topic_arn = topic_res['TopicArn']

    alert_email = os.environ.get('ALERT_EMAIL')
    subscribe_email(topic_arn, alert_email)

    print(f'Ingestion failure alerts wired to {alert_email}')
    return topic_arn

def create_downloder_lambda(sns_topic_arn):
    print('Creating iam role for downloder_lambda')

    role_name = 'lambda-s3-full-access-role'
    lambda_basic_execution_arn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
    s3_full_access_arn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess'
    sns_full_access_arn = 'arn:aws:iam::aws:policy/AmazonSNSFullAccess'
    policy_arn_list = [lambda_basic_execution_arn, s3_full_access_arn, sns_full_access_arn]

    create_role_response = create_iam_role(role_name, policy_arn_list)
    lambda_s3_role_arn = create_role_response['Role']['Arn']
    print(f'IAM role created with ARN: {lambda_s3_role_arn}')

    bucket='github-activity-bucket-123'
    folder='zipfiles'
    ghactivity_lambda_zipfile = os.environ.get('ghactivity_lambda_zipfile')
    file_name = ghactivity_lambda_zipfile.split('/')[-1]

    env_variables_dict = {'BUCKET_NAME' : bucket,
                        'FILE_PREFIX' : 'landing',
                        'BOOKMARK_FILE' : 'bookmark',
                        'BASELINE_FILE' : '2026-01-27-0.json.gz',  # update it
                        'SNS_TOPIC_ARN' : sns_topic_arn
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

    bucket = 'github-activity-bucket-123'
    folder = 'zipfiles'
    emr_lambda_zipfile = os.environ.get('emr_lambda_zipfile')
    file_name = emr_lambda_zipfile.split('/')[-1]
    env_variables_dict = {
        'BUCKET_NAME': 'github-activity-bucket-123',
        'INSTANCE_TYPE': 'm4.xlarge', # 4vcpu, 16 gb memory
        'CORE_INSTANCE_COUNT': '1',
        'BOOTSTRAP_FILE_PATH': 's3://github-activity-bucket-123/zipfiles/install_boto3.sh',
        'SPARK_ENV_DICT': '{"ENVIRON":"PROD", "SRC_DIR":"s3://github-activity-bucket-123/landing/", "SRC_FILE_FORMAT":"json", "TGT_DIR":"s3://github-activity-bucket-123/processed/","BUCKET_NAME":"github-activity-bucket-123","FILE_PREFIX":"raw","BOOKMARK_FILE":"bookmark","BASELINE_FILE":"2026-01-27-0.json.gz"}',
        'ZIP_FILE_PATH': 's3://github-activity-bucket-123/zipfiles/github_spark_app.zip',
        'APP_FILE_PATH': 's3://github-activity-bucket-123/zipfiles/app.py'
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

def create_emr_failure_alert():
    print('Creating SNS topic for EMR step failure alerts')
    topic_res = create_sns_topic('emr-step-failure-alerts')
    topic_arn = topic_res['TopicArn']

    alert_email = os.environ.get('ALERT_EMAIL')
    subscribe_email(topic_arn, alert_email)

    rule_name = 'EMRStepFailureRule'
    event_pattern = {
        "source": ["aws.emr"],
        "detail-type": ["EMR Step Status Change"],
        "detail": {"state": ["FAILED"]}
    }
    rule_response = create_event_pattern_rule(rule_name, event_pattern)
    rule_arn = rule_response['RuleArn']

    add_sns_target_to_rule(rule_name, topic_arn)
    allow_eventbridge_to_publish(topic_arn, rule_arn)

    print(f'EMR failure alerts wired to {alert_email}')


def deploy():
    create_and_upload_s3()

    ingestion_topic_arn = create_ingestion_failure_alert()
    downloader_lambda_arn = create_downloder_lambda(ingestion_topic_arn)
    shedule_downloder_lambda(downloader_lambda_arn)

    emr_lambda_arn = create_emr_lambda()
    schedule_emr_lambda(emr_lambda_arn)

    create_emr_failure_alert() 

if __name__ == '__main__':
    dotenv.load_dotenv()
    deploy()