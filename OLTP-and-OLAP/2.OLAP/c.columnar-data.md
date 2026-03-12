# Columnar Data — Explained Simply

## What is it?

Most databases you interact with daily (like PostgreSQL or MySQL) store data row by row. Imagine a spreadsheet — each row is a complete record saved together on disk. This is great when you need to grab one full record, like looking up a customer by ID.

Columnar storage flips this on its head. Instead of storing entire rows together, it stores all values of a single column together. So all the "names" live next to each other, all the "ages" live next to each other, and so on.

## Why does that matter?

Think about an analytics query:

> "What's the average order value across 500 million orders?"

You only care about the **`order_value`** column.

In a **row store**, the database has to read through every single row — including the customer name, address, email, shipping method, and 30 other columns you don't need — just to get to that one number. That's a lot of **wasted I/O**.

In a **columnar store**, the engine reads only the **`order_value`** column and skips everything else.

For analytical workloads where you're scanning **millions of rows but only touching a few columns**, this approach is **dramatically faster**.

## Example

**Row-oriented (traditional):**

>text Row 1: [Alice, 30, London]

>Row 2: [Bob, 25, Paris ]

>Row 3: [Carol, 35, Berlin]

**Stored on disk as:**

```
Alice, 30, London, Bob, 25, Paris, Carol, 35, Berlin 
```

**Column-oriented:**

text Names: [Alice, Bob, Carol ] Ages: [30, 25, 35 ] Cities: [London, Paris, Berlin] 

Stored on disk as:
```
Alice, Bob, Carol, 30, 25, 35, London, Paris, Berlin
```

The fundamental difference: **row stores keep all fields of a record together;
column stores keep all values of a field together.**

---

## How Columnar Storage Works Internally

### 1. Column Chunks

Data is split into column chunks (also called column stripes). Each chunk holds values for a
single column across many rows.


### 2. Encoding Techniques

Columnar formats exploit the homogeneity of data within a column:

| Technique | How It Works | Best For |
|-----------|-------------|----------|
| **Run-Length Encoding (RLE)** | Stores value + count of consecutive repeats | Low-cardinality columns (e.g., `country`) |
| **Dictionary Encoding** | Maps each unique value to an integer ID | String columns with repeated values |
| **Bit Packing** | Uses the minimum number of bits per value | Integer columns with small ranges |
| **Delta Encoding** | Stores the difference between consecutive values | Sorted or sequential data (e.g., timestamps) |

**Dictionary Encoding example:**

>text Original: [London, Paris, London, London, Berlin, Paris]

>Dictionary: {0: London, 1: Paris, 2: Berlin}

>Encoded: [0, 1, 0, 0, 2, 1] <-- much smaller

### 3. Compression

**How does compression help?**

When you line up all values of a single column, they tend to look very similar — same data type, similar values, and often lots of repetition.

For example, a **`country`** column might contain `"UK"` repeated a million times.

Columnar formats exploit this pattern using efficient encoding techniques such as:

- **Run-length encoding (RLE)**  
  Example: `"UK"` appears **1,000,000 times in a row**.

- **Dictionary encoding**  
  Example: replace `"United Kingdom"` with an integer key like **`3`**.

Because of these compression techniques, **columnar files can often be 10–30× smaller** than their row-oriented equivalents.
Smaller files mean less data to read from disk, less data to transfer over the network, and lower cloud storage bills.

| Storage Type | Typical Compression Ratio |
|-------------|--------------------------|
| Row-oriented | 3:1 to 5:1 |
| Column-oriented | **10:1 to 30:1** (sometimes higher) |

### 4. Metadata & Statistics

Columnar formats store per-column metadata:

- **Min / Max values** per chunk — enables predicate pushdown (skip irrelevant chunks)
- **Null counts**
- **Distinct counts**
- **Bloom filters** (for membership checks)

text Column: age 

`Chunk 0: min=18, max=35, nulls=0` ← skip if `WHERE age > 50`

`Chunk 1: min=40, max=72, nulls=2` ← read this chunk

## What are the popular columnar formats?

**Parquet**

Parquet is the **most widely used columnar file format** in modern data systems. It is supported across the ecosystem, including tools such as **Spark, AWS Athena, BigQuery, Snowflake, and dbt**.

Key features include:

- Efficient **columnar storage** for analytical workloads
- Support for **nested and complex data structures**
- Rich **metadata** (such as min/max values per column chunk) that enables query engines to skip unnecessary data
- Excellent **compression efficiency**

Because of these capabilities, Parquet is often the **default storage format for large-scale analytics pipelines**.

---

**ORC (Optimized Row Columnar)**

ORC is another columnar file format originally developed for the **Apache Hive ecosystem**.

It shares many similarities with Parquet but is particularly optimized for **Hive-based workloads**.

Key characteristics include:

- Highly efficient **columnar compression**
- Advanced **predicate pushdown**
- Built-in **ACID transaction support** in Hive environments
- Strong performance for **large Hive queries**

ORC is commonly used in **Hive, Presto, and some Hadoop-based analytics stacks**.

---

**Apache Arrow**

Apache Arrow is different from Parquet and ORC because it is an **in-memory columnar format**, not a storage format.

Arrow is designed to allow **different programming languages and data systems to share data in memory without serialization or copying**.

Key benefits include:

- Zero-copy **data sharing between systems**
- High-performance **in-memory analytics**
- Cross-language interoperability

Many modern data tools use Arrow internally, including:

- **Pandas (2.0+)**
- **Polars**
- **DuckDB**
- **PySpark**

### Format Comparison

| Feature | Parquet | ORC | Arrow |
|---------|---------|-----|-------|
| **Storage** | On disk | On disk | In memory |
| **Compression** | Snappy, GZIP, ZSTD, LZ4 | ZLIB, Snappy, LZ4, ZSTD | Optional |
| **Nested Data** | Excellent | Limited | Excellent |
| **Ecosystem** | Broad (Spark, AWS, GCP) | Hive-centric | Cross-language |
| **ACID Support** | No | Yes (with Hive) | N/A |
| **Best For** | Data lakes, analytics | Hive workloads | In-memory processing, IPC |

---

## Key Advantages

### 1. Read Only What You Need

A query like `SELECT AVG(price) FROM sales` reads **only the price column**, skipping all other
columns entirely.

### 2. Superior Compression

Homogeneous data within a column compresses far better. This means less disk I/O, less network
transfer, and lower storage costs.

### 3. Vectorized Execution

Modern CPUs can process arrays of the same type using SIMD (Single Instruction, Multiple Data)
instructions. Columnar layouts feed directly into this, allowing the CPU to process batches of
values in a single operation.

### 4. Predicate Pushdown

Using min/max statistics, the engine skips entire chunks without reading them:

text Query: 
`WHERE year = 2025`

Chunk `metadata: min_year=2020, max_year=2022` → SKIP Chunk 

`metadata: min_year=2024, max_year=2026` → READ this chunk


### 5. Cost Efficiency

Less data read = less compute = lower cloud bills, especially in systems that charge per byte
scanned (like BigQuery or Athena).

---

## Key Disadvantages & Problems

### 1. Slow Row-Level Operations

Writes, updates, and deletes are expensive. To update a single row, the engine must locate and
modify values across multiple column files. Row stores handle this in a single operation.

### 2. Poor Write Performance

Inserts require appending to every column file separately. This makes high-frequency OLTP writes
impractical and real-time inserts more complex (requires buffering).

### 3. Row Reconstruction Cost

Queries that need entire rows (e.g., `SELECT *`) must stitch columns back together, which is
slower than a row store where the full row is already contiguous.

### 4. Schema Evolution Complexity

Adding, removing, or renaming columns can be complex depending on the format. Parquet handles it
reasonably well (by column name), but it's still more involved than `ALTER TABLE` in a
traditional database.

### 5. Small Dataset Overhead

For small datasets (fewer than thousands of rows), the overhead of columnar encoding, metadata,
and chunk management can make it slower than just reading rows.

### 6. Limited Transaction Support

Most columnar systems have limited or no ACID transaction support. They are not designed for
concurrent read/write workloads, row-level locking, or rollback scenarios.

### 7. The Small Files Problem

If your pipeline produces thousands of tiny Parquet files instead of a few large ones, query performance tanks. The engine spends more time opening files and reading metadata than actually processing data. Compaction (merging small files into larger ones) is a common maintenance task.

When working with data lakes, having **too many small Parquet or ORC files** can create serious performance issues.

Small files introduce **excessive metadata overhead**, slow down **file listing operations on object stores** (such as Amazon S3), and often result in **poor compression efficiency**.

Because many query engines must open and read metadata from **each file individually**, thousands or millions of tiny files can significantly slow down analytical queries.

To mitigate this issue, a common maintenance practice is **file compaction** — merging many small files into fewer, larger files.

**Typical recommended file sizes:**

- **128 MB – 1 GB per file**

Maintaining files within this range helps improve:

- Query performance
- Compression efficiency
- Object store listing speed
- Overall data lake health

---

## Where Columnar Storage Is Used

Pretty much **every modern analytics system** relies on columnar storage because it is optimized for large-scale analytical queries.

### Cloud Data Warehouses
Cloud data warehouses such as **Snowflake, BigQuery, Amazon Redshift, and Azure Synapse** store data internally in a columnar format.  
This architecture allows them to scan only the relevant columns needed for a query, making analytical workloads significantly faster.

---

### Data Lakes
Data lakes built on **Amazon S3, Google Cloud Storage (GCS), or Azure Data Lake Storage (ADLS)** typically store curated datasets in **Parquet files**.

Query engines such as:

- **AWS Athena**
- **Trino**
- **Apache Spark SQL**

can read these files directly, enabling large-scale analytics without loading data into a traditional database.

---

### Lakehouse Table Formats
Modern **table formats** like:

- **Delta Lake**
- **Apache Iceberg**
- **Apache Hudi**

are built on top of **Parquet**.

They extend raw data lake files with capabilities typically found in databases, including:

- **ACID transactions**
- **Time travel**
- **Schema evolution**
- **Data versioning**

These features make data lakes behave more like traditional warehouses.

---

### Time-Series Databases
Time-series databases such as:

- **InfluxDB**
- **QuestDB**
- **Apache Druid**

often use columnar storage because time-series queries typically involve **aggregations across large time ranges** but only require a few columns (for example, timestamp and metric values).

Columnar layouts make these queries extremely efficient.

---

### Embedded Analytics Engines
Modern embedded analytics tools like:

- **DuckDB** (often described as *"SQLite for analytics"*)
- **Polars**

allow you to run analytical queries directly against **Parquet files on your local machine**, without needing a dedicated server.

This enables fast, interactive data analysis on laptops or small environments.

---

### Business Intelligence (BI) Tools
BI and dashboarding platforms such as:

- **Tableau**
- **Looker**
- **Power BI**

typically query **columnar data warehouses** behind the scenes.

---

## When should you use columnar vs. row storage?

### Use Columnar Storage When

Columnar storage works best for **analytical workloads** where queries scan large datasets but only access a few columns.

Typical use cases include:

- Aggregations and reporting
- Business intelligence dashboards
- Data science and machine learning analysis
- Large analytical queries over millions or billions of rows

Because columnar systems read only the necessary columns, they significantly reduce **I/O and query time**.

---

### Use Row-Oriented Storage When

Row-oriented storage is ideal for **transactional workloads** where applications frequently read or modify individual records.

Common examples include:

- User registrations and profile updates
- Order processing
- Payment transactions
- Inventory updates

Row stores allow fast **single-row reads and writes** and typically provide strong **ACID guarantees**, making them suitable for operational systems.

---

### How Modern Data Architectures Use Both

In practice, most modern data architectures combine both approaches:

- A **row-oriented database** for the application layer (**OLTP**)  
- A **columnar warehouse or data lake** for analytics (**OLAP**)

Data is typically moved from OLTP systems to OLAP systems using **ETL or ELT pipelines**, enabling fast transactional processing while still supporting large-scale analytics.
