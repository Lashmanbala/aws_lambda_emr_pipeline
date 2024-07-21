from util import create_bucket, upload_s3, create_iam_role, create_lambda_function, invoke_lambda_funtion

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