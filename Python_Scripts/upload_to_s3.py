import boto3

access_key = 'XXXXX'
secret_key = 'XXXXX'
s3_bucket_name = 'erho2'
s3_filename = 'late_orders.csv'
s3 = boto3.client('s3',
                      aws_access_key_id = access_key,
                      aws_secret_access_key = secret_key)

s3.upload_file(
    Filename=s3_filename, Bucket=s3_bucket_name,
    Key=s3_filename,)