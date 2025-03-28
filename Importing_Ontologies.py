import boto3
from botocore.exceptions import NoCredentialsError

# AWS credentials TODO: Add your credentials
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
AWS_BUCKET_NAME = ""
AWS_REGION = ""

# File details
tt = "<Path to the ttl file generated in the previous step>" #TODO
s3_key =  '<todo>.ttl' #S3 key (path) where the file will be stored #TODO make sure you change this filename

# Initialize a session using Amazon S3
s3 = boto3.client('s3', 
                  aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY,
                  region_name=AWS_REGION)
try:
    s3.upload_file(tt, AWS_BUCKET_NAME, s3_key)
    print(f"File {tt} uploaded to {AWS_BUCKET_NAME}/{s3_key}")
except FileNotFoundError:
    print("The file was not found")
except NoCredentialsError:
    print("Credentials not available")