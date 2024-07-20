from util import create_bucket, upload_s3

bkt_res = create_bucket(bkt_name='github-bkt')

if bkt_res['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Bucket created successfully')

        upload_res = upload_s3(bucket='github-bkt',
                folder='zipfiles',
                file_name='zip_file_for_lambda.zip',
                body='/home/bala/code/projects/github_activity_project/ghactivity_downloader/ghactivity_downloader_for_lambda.zip')
                
        print('zipfile uploded successfully')
