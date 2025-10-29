# cloud_platforms

# 1

#  AWS Cloud Basics - S3, EC2, Lambda

This project demonstrates how to use AWS services for data storage and basic cloud computing tasks

##  Overview
- **Amazon S3** → Upload and list data files using Python (Boto3)
- **Amazon EC2** → Run compute instances for data processing
- **AWS Lambda** → Automate file processing with serverless functions

##  Key Features
- Create S3 buckets programmatically  
- Upload and list files using `boto3`  
- Example Python script (`aws_s3_basics.py`)  
- Prepares for integration with AWS Glue and Redshift


# 2
# AWS Glue → Redshift ETL (sample)

This folder contains a sample ETL pipeline that demonstrates how to:
1. Place raw CSV data in Amazon S3.
2. Catalog it with AWS Glue (Crawler).
3. Run an AWS Glue job (PySpark) to transform data.
4. Load transformed records into Amazon Redshift for analytics.
   
 **Important:** This repo contains example code and a small sample dataset. 
 The Glue job script (`glue_etl_job.py`) is intended to run inside AWS Glue, not locally, unless you follow the local testing guidance.

