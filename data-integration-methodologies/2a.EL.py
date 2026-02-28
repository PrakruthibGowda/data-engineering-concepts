# MSSQL Orders table to BigQuery simple and straight forward example, written to get last one hour order data in batches and load it into BQ table
import pyodbc
import pandas as pd
from google.cloud import bigquery

# -------------------------------
# STEP 1: EXTRACT FROM MSSQL
# -------------------------------

# MSSQL connection details
server = 'your_server.database.windows.net'
database = 'your_database'
username = 'your_username'
password = 'your_password'
driver = '{ODBC Driver 17 for SQL Server}'

connection_string = f"""
DRIVER={driver};
SERVER={server};
DATABASE={database};
UID={username};
PWD={password};
"""

# Connect to MSSQL
conn = pyodbc.connect(connection_string)

# Extract query
query = """
SELECT *
FROM orders where order_date >= DATEADD(HOUR, -1, GETDATE())
"""

# Load into pandas DataFrame
df = pd.read_sql(query, conn)
conn.close()

print("Extracted rows:", len(df))


# -------------------------------
# STEP 2: LOAD INTO BIGQUERY
# -------------------------------

# BigQuery configuration
project_id = "your-gcp-project"
dataset_id = "analytics"
table_id = "orders_raw"

client = bigquery.Client(project=project_id)

table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Load DataFrame into BigQuery
job = client.load_table_from_dataframe(df, table_ref)

job.result()  # Wait for job to complete

print("Data successfully loaded into BigQuery!")
