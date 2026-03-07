# Here’s a simple ELT example in Python: Extract: read raw sales data, Load: load raw data into a database table, Transform: run SQL on the database after loading

import sqlite3
import csv


def extract(csv_file: str):
    with open(csv_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)


def load(raw_data: list, conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_sales (
            order_id TEXT,
            customer TEXT,
            amount REAL
        )
    """)

    for row in raw_data:
        cursor.execute("""
            INSERT INTO raw_sales (order_id, customer, amount)
            VALUES (?, ?, ?)
        """, (
            row["order_id"],
            row["customer"],
            float(row["amount"])
        ))

    conn.commit()


def transform(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_summary AS
        SELECT
            customer,
            COUNT(*) AS total_orders,
            SUM(amount) AS total_amount
        FROM raw_sales
        GROUP BY customer
    """)

    conn.commit()


def main():
    csv_file = "sales.csv"

    # sample input file
    with open(csv_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["order_id", "customer", "amount"])
        writer.writerow(["1", "Alice", "100.50"])
        writer.writerow(["2", "Bob", "200.00"])
        writer.writerow(["3", "Alice", "50.00"])

    conn = sqlite3.connect("example.db")

    raw_data = extract(csv_file)
    load(raw_data, conn)
    transform(conn)

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sales_summary")
    for row in cursor.fetchall():
        print(row)

    conn.close()


if __name__ == "__main__":
    main()
