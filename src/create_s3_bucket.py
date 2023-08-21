from os import environ

import boto3

MINIO_ENDPOINT_URL =  environ['MINIO_ENDPOINT_URL'] 
MINIO_ACCESS_KEY = environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = environ['MINIO_SECRET_ACCESS_KEY']
BUCKET_NAME = environ['S3_BUCKET']

# Initialize the MinIO client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT_URL,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Create a bucket
try:
    s3_client.create_bucket(Bucket=BUCKET_NAME, ACL="private")
    print(f"Bucket '{BUCKET_NAME}' created successfully.")
except Exception as e:
    print(f"Error creating bucket: {e}")


# List buckets to verify
response = s3_client.list_buckets()
print("Buckets:")
for bucket in response["Buckets"]:
    print(f"- {bucket['Name']}")


