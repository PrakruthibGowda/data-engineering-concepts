#Simple example for extracting some sample data and transform it to add timestamp and load to BQ table
from google.cloud import bigquery
from datetime import datetime


def etl_to_bigquery():
    """Simple ETL: hardcoded data → BigQuery"""
    
    # EXTRACT: Sample sales data
    sales_data = [
        {"order_id": "ORD001", "customer": "Alice", "amount": 150.00},
        {"order_id": "ORD002", "customer": "Bob", "amount": 200.50},
        {"order_id": "ORD003", "customer": "Alice", "amount": 75.25},
    ]
    
    # TRANSFORM: Add timestamp
    for row in sales_data:
        row["loaded_at"] = datetime.utcnow().isoformat()
    
    # LOAD: Insert into BigQuery
    client = bigquery.Client(project="your-project-id")
    table_id = "your-project-id.sales_data.sales"
    
    job = client.load_table_from_json(sales_data, table_id)
    job.result()  # Wait for completion
    
    print(f"✅ Loaded {len(sales_data)} rows into BigQuery")


if __name__ == "__main__":
    etl_to_bigquery()
