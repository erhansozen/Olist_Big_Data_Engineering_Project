from boto3.s3.transfer import S3Transfer
import boto3

# S3 access, bucket and file configurations
aws_access_key_id = 'XXXXX'
aws_secret_access_key = 'XXXXX'
s3 = boto3.resource('s3')
s3_bucket_name = 'erho2'
file_downloaded = "Olist.zip"

# Download dataset from S3
s3 = boto3.client('s3')
s3.download_file(s3_bucket_name, file_downloaded, file_downloaded)