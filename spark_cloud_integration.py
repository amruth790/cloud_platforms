from pyspark.sql import SparkSession

# --- Spark session with S3 access ---
spark = SparkSession.builder \
    .appName("CloudIntegrationExample") \
    .config("spark.hadoop.fs.s3a.access.key", "<AWS_ACCESS_KEY>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<AWS_SECRET_KEY>") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# --- Read CSV directly from S3 ---
df = spark.read.csv("s3a://my-bucket/sales.csv", header=True, inferSchema=True)

# --- Perform transformations ---
df_summary = df.groupBy("Category").sum("Revenue").orderBy("sum(Revenue)", ascending=False)

# --- Show results ---
df_summary.show()

# --- Save processed data back to S3 ---
df_summary.write.mode("overwrite").csv("s3a://my-bucket/output/sales_summary")

spark.stop()
