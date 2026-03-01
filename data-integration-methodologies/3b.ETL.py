#Extract sales data from a CSV file, transform it to calculate metrics, and load it into a Bigquery table for reporting.
import csv
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def extract_sales_data(csv_path: str):
    """Extract sales transactions from CSV."""
    print(f"[EXTRACT] Reading from {csv_path}...")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data = list(reader)
    print(f"[EXTRACT] Extracted {len(data)} records")
    return data


def transform_sales_data(raw_data: list) -> list:
    """Transform raw sales into structured records with calculations."""
    print("[TRANSFORM] Cleaning and calculating metrics...")
    transformed = []
    
    for row in raw_data:
        try:
            # Parse and validate
            order_id = row.get("order_id", "").strip()
            customer = row.get("customer_name", "Unknown").strip().title()
            product = row.get("product", "").strip()
            quantity = int(row.get("quantity", "0"))
            price = float(row.get("price", "0"))
            order_date_str = row.get("order_date", "").strip()
            
            # Skip invalid rows
            if not order_id or quantity <= 0 or price <= 0:
                print(f"[TRANSFORM] Skipping invalid row: {row}")
                continue
            
            # Parse date
            order_date = datetime.strptime(order_date_str, "%Y-%m-%d").date()
            
            # Calculate metrics
            total_amount = round(quantity * price, 2)
            discount = 0.1 if total_amount > 1000 else 0.0  # 10% discount for orders > $1000
            final_amount = round(total_amount * (1 - discount), 2)
            
            # Categorize
            category = "High Value" if final_amount >= 500 else "Standard"
            
            transformed.append({
                "order_id": order_id,
                "customer": customer,
                "product": product,
                "quantity": quantity,
                "price": price,
                "order_date": order_date.isoformat(),
                "total_amount": total_amount,
                "discount_rate": discount,
                "final_amount": final_amount,
                "category": category
            })
        except (ValueError, KeyError) as e:
            print(f"[TRANSFORM] Error processing row {row}: {e}")
            continue
    
    print(f"[TRANSFORM] Transformed {len(transformed)} valid records")
    return transformed


def load_to_bigquery(data: list, project_id: str, dataset_id: str, table_id: str):
    """Load transformed data into BigQuery table."""
    print(f"[LOAD] Writing to BigQuery {project_id}.{dataset_id}.{table_id}...")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Create dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"[LOAD] Dataset {dataset_id} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"[LOAD] Created dataset {dataset_id}")
    
    # Define table schema
    schema = [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("total_amount", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("discount_rate", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("final_amount", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    # Create or get table
    table_ref = dataset_ref.table(table_id)
    try:
        table = client.get_table(table_ref)
        print(f"[LOAD] Table {table_id} already exists")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"[LOAD] Created table {table_id}")
    
    # Add loaded_at timestamp to each row
    loaded_at = datetime.utcnow().isoformat()
    for row in data:
        row["loaded_at"] = loaded_at
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append data
        # Use WRITE_TRUNCATE to replace all data
        # Use WRITE_EMPTY to only write if table is empty
    )
    
    # Load data
    job = client.load_table_from_json(data, table_ref, job_config=job_config)
    job.result()  # Wait for job to complete
    
    print(f"[LOAD] Loaded {len(data)} records into BigQuery")
    
    # Run a sample query to verify
    query = f"""
        SELECT 
            customer, 
            SUM(final_amount) as total_sales, 
            COUNT(*) as order_count
        FROM `{project_id}.{dataset_id}.{table_id}`
        GROUP BY customer
        ORDER BY total_sales DESC
        LIMIT 5
    """
    
    print("\n[REPORT] Top 5 customers by sales:")
    query_job = client.query(query)
    for row in query_job:
        print(f"  {row.customer}: ${row.total_sales:,.2f} ({row.order_count} orders)")


def run_etl_pipeline(csv_path: str, project_id: str, dataset_id: str, table_id: str):
    """Run the complete ETL pipeline."""
    print("=" * 60)
    print("Starting ETL Pipeline to BigQuery")
    print("=" * 60)
    
    # ETL steps
    raw_data = extract_sales_data(csv_path)
    transformed_data = transform_sales_data(raw_data)
    load_to_bigquery(transformed_data, project_id, dataset_id, table_id)
    
    print("=" * 60)
    print("ETL Pipeline Complete!")
    print("=" * 60)


if __name__ == "__main__":
    # Create sample data
    sample_csv = "sales_data.csv"
    
    with open(sample_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "order_id", "customer_name", "product", "quantity", "price", "order_date"
        ])
        writer.writeheader()
        writer.writerows([
            {"order_id": "ORD001", "customer_name": "john doe", "product": "Laptop", 
             "quantity": "2", "price": "899.99", "order_date": "2026-02-15"},
            {"order_id": "ORD002", "customer_name": "JANE SMITH", "product": "Mouse", 
             "quantity": "5", "price": "25.50", "order_date": "2026-02-16"},
            {"order_id": "ORD003", "customer_name": "alice brown", "product": "Monitor", 
             "quantity": "3", "price": "450.00", "order_date": "2026-02-17"},
            {"order_id": "ORD004", "customer_name": "bob wilson", "product": "Keyboard", 
             "quantity": "10", "price": "75.00", "order_date": "2026-02-18"},
            {"order_id": "ORD005", "customer_name": "john doe", "product": "Desk", 
             "quantity": "1", "price": "599.99", "order_date": "2026-02-20"},
            {"order_id": "INVALID", "customer_name": "test", "product": "Bad", 
             "quantity": "-1", "price": "0", "order_date": "2026-02-21"},  # Invalid
        ])
    
    # Configure your BigQuery settings
    PROJECT_ID = "your-project-id"  # Replace with your GCP project ID
    DATASET_ID = "sales_data"
    TABLE_ID = "sales"
    
    # Run ETL
    run_etl_pipeline(sample_csv, PROJECT_ID, DATASET_ID, TABLE_ID)
