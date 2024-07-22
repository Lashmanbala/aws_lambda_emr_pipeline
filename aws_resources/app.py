from util import create_bucket, upload_s3, create_iam_role, create_lambda_function, invoke_lambda_funtion
from event_bridge_util import create_event_bridge_rule, add_target_to_rule

bkt_name='github-bkt'
bkt_res = create_bucket(bkt_name)

if bkt_res['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Bucket created successfully')

file_path = '/home/bala/code/projects/github_activity_project/ghactivity_downloader/ghactivity_downloader_for_lambda.zip'
folder='zipfiles'
file_name=file_path.split('/')[-1]
body=open(file_path, 'rb').read()

upload_res = upload_s3(bkt_name,folder,file_name,body)
        
print('zipfile uploded successfully')

role_name = 'lambda-s3-full-access-role'
trusted_service = 'lambda.amazonaws.com'  # The service that will assume the role
policy_arn_list = ['arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole','arn:aws:iam::aws:policy/AmazonS3FullAccess']

role_arn = create_iam_role(role_name, trusted_service, policy_arn_list)
print(f'IAM role created with ARN: {role_arn}')

bucket = bkt_name
role_arn = 'arn:aws:iam::891376967063:role/lambda-s3-full-access-role'
env_variables_dict = {'BUCKET_NAME' : bkt_name,
                    'FILE_PREFIX' : 'landing',
                    'BOOKMARK_FILE' : 'bookmark',
                    'BASELINE_FILE' : '2024-07-21-0.json.gz'
                    }
func_name='ghactivity-download-function'
lambda_res = create_lambda_function(bucket, folder, file_name, role_arn, env_variables_dict,func_name)
print('successfully created lambda function')
print(lambda_res)

invoke_res = invoke_lambda_funtion(func_name)
print('successfully invoked lambda function')
print(invoke_res)

rule_name = 'HourlyGhactivityDownloadRule'
event_rule_response = create_event_bridge_rule(rule_name)
print('Successfully event rule created')
print(event_rule_response)

lambda_arn = lambda_res['FunctionArn']
# lambda_arn = 'arn:aws:lambda:us-east-1:891376967063:function:ghactivity-download-function'
rule_arn = event_rule_response['RuleArn']
# rule_arn = 'arn:aws:events:us-east-1:891376967063:rule/HourlyGhactivityDownloadRule'
put_targets_response = add_target_to_rule(rule_name, lambda_arn, rule_arn)
print('Successfully target added to event rule')
print(put_targets_response)