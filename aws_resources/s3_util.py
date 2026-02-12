import boto3

def create_bucket(bucket_name):
    s3_client = boto3.client('s3')

    res = s3_client.create_bucket(Bucket=bucket_name)
        
    return res


def upload_s3(bucket_name,folder,file_name,body):

    s3_client = boto3.client('s3')
    
    res = s3_client.put_object(
                            Bucket=bucket_name,
                            Key=f'{folder}/{file_name}',
                            Body=body
                            )
    return res