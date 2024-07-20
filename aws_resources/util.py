import boto3

def create_bucket(bkt_name):
    s3_client = boto3.client('s3')
    res = s3_client.create_bucket(Bucket=bkt_name)
        
    return res

