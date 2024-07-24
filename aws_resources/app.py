from util import create_bucket, upload_s3, create_iam_role, create_lambda_function, invoke_lambda_funtion
from event_bridge_util import create_event_bridge_rule, add_target_to_rule

bkt_name='github-bkt'
# bkt_res = create_bucket(bkt_name)

# if bkt_res['ResponseMetadata']['HTTPStatusCode'] == 200:
#         print('Bucket created successfully')

# file_path = '/home/bala/code/projects/github_activity_project/ghactivity_downloader/ghactivity_downloader_for_lambda.zip'
folder='zipfiles'
# file_name=file_path.split('/')[-1]
# body=open(file_path, 'rb').read()

# upload_res = upload_s3(bkt_name,folder,file_name,body)
        
# print('zipfile uploded successfully')

# role_name = 'lambda-s3-full-access-role'
# lambda_basic_execution_arn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
# s3_full_access_arn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess'
# policy_arn_list = [lambda_basic_execution_arn,s3_full_access_arn]

# create_role_response = create_iam_role(role_name, policy_arn_list)
# lambda_s3_role_arn = create_role_response['Role']['Arn']
# print(f'IAM role created with ARN: {lambda_s3_role_arn}')

bucket = bkt_name
# role_arn = 'arn:aws:iam::891376967063:role/lambda-s3-full-access-role'
# env_variables_dict = {'BUCKET_NAME' : bkt_name,
#                     'FILE_PREFIX' : 'landing',
#                     'BOOKMARK_FILE' : 'bookmark',
#                     'BASELINE_FILE' : '2024-07-21-0.json.gz'
#                     }
# func_name='ghactivity-download-function'
# handler = 'lambda_function.lambda_handler'

# lambda_res = create_lambda_function(bucket, folder, file_name, lambda_s3_role_arn, env_variables_dict,func_name,handler)
# print('successfully created lambda function')
# print(lambda_res)

# invoke_res = invoke_lambda_funtion(func_name)
# print('successfully invoked lambda function')
# print(invoke_res)

# rate = 'rate(60 minutes)'
# rule_name = 'HourlyGhactivityDownloadRule'
# event_rule_response = create_event_bridge_rule(rule_name, rate)
# print('Successfully event rule created for ghactivity_downloader function')
# print(event_rule_response)

# lambda_arn = lambda_res['FunctionArn']
# rule_arn = event_rule_response['RuleArn']
# put_targets_response = add_target_to_rule(rule_name, lambda_arn, rule_arn)
# print('Successfully lambda target added to event rule')
# print(put_targets_response)


# role_name = 'lambda-s3-emr-iam-access-role'
# lambda_basic_execution_arn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
# s3_full_access_arn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess'
# iam_full_access_arn = 'arn:aws:iam::aws:policy/IAMFullAccess'
# emr_full_access_arn = 'arn:aws:iam::aws:policy/AmazonElasticMapReduceFullAccess'
# policy_arn_list = [lambda_basic_execution_arn, s3_full_access_arn, iam_full_access_arn, emr_full_access_arn]

# create_role_response = create_iam_role(role_name, policy_arn_list)
# lambda_s3_iam_emr_role_arn = create_role_response['Role']['Arn']
# print(f'IAM role created with ARN: {lambda_s3_iam_emr_role_arn}')

# file_name = 'lambda_for_emr.zip'
# env_variables_dict = {
#     'BUCKET_NAME': 'github-bkt',
#     'INSTANCE_TYPE': 'm4.xlarge',
#     'CORE_INSTANCE_COUNT': '1',
#     'SPARK_ENV_DICT': '{"ENVIRON":"PROD", "SRC_DIR":"s3://github-bkt/landing/", "SRC_FILE_FORMAT":"json", "TGT_DIR":"s3://github-bkt/raw/", "TGT_FILE_FORMAT":"parquet", "SRC_FILE_PATTERN":"2024-07-21"}',
#     'ZIP_FILE_PATH': 's3://github-bkt/zipfiles/github_spark_app.zip',
#     'APP_FILE_PATH': 's3://github-bkt/zipfiles/app.py'
# }

# func_name = 'lambda_function_for_emr'
# handler = 'lambda_function_for_emr.lambda_handler'

# create_lambda_response = create_lambda_function(bucket,folder,file_name,lambda_s3_iam_emr_role_arn,env_variables_dict,func_name,handler)
# print(f"Successfully created {create_lambda_response['FunctionName']}")

# res = invoke_lambda_funtion('lambda_function_for_emr')
# print(f"Successfully invoked {create_lambda_response['FunctionName']}")

rate = 'cron(0 0 * * ? *)' # every day at 12:00am
rule_name = 'DailyEmrRule'
event_rule_response = create_event_bridge_rule(rule_name, rate)
print('Successfully event rule created for lambda_for_emr function')
print(event_rule_response)

# lambda_arn = create_lambda_response['FunctionArn']
lambda_arn = 'arn:aws:lambda:us-east-1:891376967063:function:lambda_function_for_emr'
rule_arn = event_rule_response['RuleArn']
put_targets_response = add_target_to_rule(rule_name, lambda_arn, rule_arn)
print('Successfully lambda target added to event rule')
print(put_targets_response)