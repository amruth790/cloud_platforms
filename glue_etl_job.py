# glue_etl_job.py
# AWS Glue ETL (PySpark) example


# NOTE:
# - This script is intended to run as an AWS Glue Job (Glue 3.0+ runtime).
# - It uses AWS Glue libraries (awsglue.*) that are available in the Glue environment.
# - For local testing use the "Local testing" guidance in README.md.

import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Expected job args: --JOB_NAME
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters (edit for your environment)
GLUE_DATABASE = "my_glue_db"                # Glue Data Catalog database name created by crawler
GLUE_TABLE = "sales_data"                   # Glue table name created by crawler
REDSHIFT_JDBC_CONNECTION = "redshift-conn"  # Glue connection name to Redshift (created in Glue console)
REDSHIFT_DB_TABLE = "public.sales_data"     # Redshift target table (schema.table)
REDSHIFT_TMP_DIR = "s3://my-bucket/tmp/"    # S3 temp dir for Glue -> Redshift COPY

# 1) Read data from Glue Data Catalog (crawler must create the table first)
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name=GLUE_TABLE,
    transformation_ctx="datasource"
)

# 2) Basic data cleaning / transformations (convert to DynamicFrame -> DataFrame for some ops)
df = datasource.toDF()

# Example transformations:
# - Trim strings
# - Drop rows with NULL product or price
# - Cast types
df = df.withColumn("product", F.trim(F.col("product"))) \
       .withColumn("price", F.col("price").cast("double")) \
       .withColumn("quantity", F.col("quantity").cast("int")) \
       .na.drop(subset=["product", "price", "quantity"])

# Add a derived column
df = df.withColumn("total", F.col("price") * F.col("quantity"))

# Convert back to DynamicFrame
from awsglue.dynamicframe import DynamicFrame
transformed_dyf = DynamicFrame.fromDF(df, glueContext, "transformed_dyf")

# Optional: show a few rows in Glue job logs
print("Sample transformed rows:")
df.show(5, truncate=False)

# 3) Write to Redshift using Glue's JDBC writer
# This uses the Glue connection (which wraps Redshift credentials and network config)
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=transformed_dyf,
    catalog_connection=REDSHIFT_JDBC_CONNECTION,
    connection_options={"dbtable": REDSHIFT_DB_TABLE, "database": "dev"},
    redshift_tmp_dir=REDSHIFT_TMP_DIR,
    transformation_ctx="redshift_write"
)

job.commit()
print("Glue job complete.")
