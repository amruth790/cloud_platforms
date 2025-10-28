"""
AWS Cloud Basics 

Description:
  Simple script to connect to AWS S3, create a bucket (if not exists),
  upload a local file, and list all objects inside the bucket.
"""

import boto3
import os

def main():
    #AWS configuration
    bucket_name = "my-bigdata-bucket-demo"
    region = "eu-west-2"  # London region
    file_to_upload = "sample_data.csv"

    # Create a sample file
    with open(file_to_upload, "w") as f:
        f.write("id,name,score\n1,Aravind,90\n2,Adam,88")

    #Connect to S3
    s3 = boto3.client("s3")

    #  Create bucket if not exists
    try:
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
        print(f" Bucket '{bucket_name}' created successfully.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f" Bucket '{bucket_name}' already exists.")

    #  Upload file
    s3.upload_file(file_to_upload, bucket_name, file_to_upload)
    print(f" Uploaded {file_to_upload} to {bucket_name}")

    #  List objects in bucket
    print("\n Files in bucket:")
    for obj in s3.list_objects(Bucket=bucket_name).get('Contents', []):
        print("-", obj['Key'])

if __name__ == "__main__":
    main()
