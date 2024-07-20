from util import create_bucket, upload_s3, create_iam_role

bkt_name='github-bkt'
bkt_res = create_bucket(bkt_name)

if bkt_res['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Bucket created successfully')

        bucket='github-bkt'
        folder='zipfiles'
        file_name='zip_file_for_lambda.zip'
        body='/home/bala/code/projects/github_activity_project/ghactivity_downloader/ghactivity_downloader_for_lambda.zip'

        upload_res = upload_s3(bucket,folder,file_name,body)
                
        print('zipfile uploded successfully')

role_name = 'lambda-s3-full-access-role'
trusted_service = 'lambda.amazonaws.com'  # The service that will assume the role
policy_arn_list = ['arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole','arn:aws:iam::aws:policy/AmazonS3FullAccess']
role_arn = create_iam_role(role_name, trusted_service, policy_arn_list)
print(f'IAM role created with ARN: {role_arn}')