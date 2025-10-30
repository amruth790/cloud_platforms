import psycopg2

# Connection parameters
conn = psycopg2.connect(
    dbname='mydb',
    user='awsuser',
    password='mypassword',
    host='redshift-cluster-name.region.redshift.amazonaws.com',
    port='5439'
)
cur = conn.cursor()

# Create a sample table
cur.execute("""
    CREATE TABLE IF NOT EXISTS sales_data (
        id INT,
        product VARCHAR(50),
        price FLOAT,
        quantity INT
    );
""")

# Load data from S3 into Redshift
cur.execute("""
    COPY sales_data
    FROM 's3://my-bucket/sales.csv'
    IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
    CSV IGNOREHEADER 1;
""")

# Run a sample query
cur.execute("""
    SELECT product, SUM(price * quantity) AS total_revenue
    FROM sales_data
    GROUP BY product
    ORDER BY total_revenue DESC;
""")

for row in cur.fetchall():
    print(row)

conn.commit()
cur.close()
conn.close()
