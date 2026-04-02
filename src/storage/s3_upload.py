import boto3
from src.config.paths import (
   S3_BUCKET_NAME, S3_KEYS, s3_LOCAL_PATH
)
from botocore.exceptions import ClientError


def upload_parquet_files():
    s3 = boto3.client("s3")

    for fp, key in zip(s3_LOCAL_PATH, S3_KEYS):
        try:
            s3.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code != "NoSuchKey":
                raise
    
        
        s3.upload_file(fp, S3_BUCKET_NAME, key)
    
    



