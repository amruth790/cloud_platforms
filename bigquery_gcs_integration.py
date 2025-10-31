from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Define dataset and table
dataset_id = "my_dataset"
table_id = f"{dataset_id}.sales_data"

# Create dataset if not exists
dataset = bigquery.Dataset(client.dataset(dataset_id))
dataset.location = "US"
client.create_dataset(dataset, exists_ok=True)

# Load data from GCS
uri = "gs://my-bucket/sales.csv"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
load_job.result()

# Query the data
query_job = client.query("""
    SELECT product, SUM(price * quantity) AS total_revenue
    FROM `my_dataset.sales_data`
    GROUP BY product
    ORDER BY total_revenue DESC
""")

for row in query_job.result():
    print(f"{row.product}: {row.total_revenue}")
