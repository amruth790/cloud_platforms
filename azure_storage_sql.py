from azure.storage.blob import BlobServiceClient
import pyodbc
import pandas as pd
from io import StringIO

# --- Upload data to Azure Blob Storage ---
connect_str = "<your_connection_string>"
container_name = "data"
blob_name = "sales.csv"

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

with open("sales.csv", "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

print("✅ CSV uploaded to Blob Storage.")

# --- Connect to Azure SQL Database ---
server = '<your_server>.database.windows.net'
database = 'salesdb'
username = '<your_username>'
password = '<your_password>'
driver = '{ODBC Driver 18 for SQL Server}'

conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# --- Query Data ---
query = "SELECT Product, SUM(Quantity * Price) AS TotalRevenue FROM Sales GROUP BY Product ORDER BY TotalRevenue DESC"
df = pd.read_sql(query, conn)

print(df)

conn.close()
