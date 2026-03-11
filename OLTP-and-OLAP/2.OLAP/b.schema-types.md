## OLAP Schema types
In OLAP (Online Analytical Processing) systems, data is organized using dimensional schemas optimized for analytics, aggregations, and reporting. The goal is to make complex analytical queries fast and easy to write.

### The three main schema types used in OLAP are:

  * Star Schema
  * Snowflake Schema
  * Galaxy Schema (Fact Constellation)

### 1.Star Schema
The Star Schema is the most common OLAP schema. It has a central fact table connected directly to dimension tables.

  * Fact Table - Contains numeric measures and foreign keys to dimensions.
     > Example: Fact_Sales(sale_id,	product_id,	customer_id,	date_id,	store_id,	revenue)
    
  * Dimension Tables - Contain descriptive attributes.
     > Example: Dim_Product(product_id, product_name, category, brand);
                Dim_Customer(customer_id, name, city, country)

  * Characteristics:
      * Denormalized
      * Few joins required
      * Fast queries
      * Easy to understand

  * Advantages:
      * High query performance
      * Simple design
      * Ideal for BI dashboards

  * Disadvantages:
      * Data redundancy
      * arger storage

  Example Query:
  Total sales by product category:
  > SELECT p.category, SUM(f.revenue)
     FROM Fact_Sales f
     JOIN Dim_Product p
     ON f.product_id = p.product_id
     GROUP BY p.category;

### 2.Snowflake Schema
The Snowflake Schema is a normalized version of the Star Schema. Dimension tables are split into multiple related tables.

  Instead of one large product table:
  > Dim_Product(product_id, product_name, category)

  We split it into: 
  > Dim_Product(product_id, product_name, category_id);
  > Dim_Category(category_id, category_name)

  * Characteristics:
      * Normalized dimensions
      * More joins
      * Less redundancy

  * Advantages:
      * Reduced data duplication
      * Better data integrity

  * Disadvantages:
      * More complex queries
      * Slightly slower analytics queries
   
### 3. Galaxy Schema (Fact Constellation)
A Galaxy Schema contains multiple fact tables sharing dimension tables.
Used when analyzing multiple business processes.

  * Example:
    Fact_Sales:
    >| sale_id | product_id | customer_id | date_id | revenue |

    Fact_Returns:
    >| return_id | product_id | customer_id | date_id | refund |

    Shared dimensions:
    * Product
    * Customer
    * Date

  * Advantages:
      * Supports multiple analytical workflows
      * Flexible
      * Reusable dimensions

  * Disadvantages:
      * More complex design
      * Requires careful modeling

### Real-World Example
  * Star schema : One fact table and denormalised dim tables
    >Fact_Orders -> Dim_Customer, Dim_Product, Dim_Date, Dim_Store
  
    Used for: Sales dashboard, Revenue trends, Customer segmentation

  * Snowflake schema : One fact table, dim tables have further normalised dim tables
    >Fact_Orders : Dim_Product → Dim_Category, Dim_Customer → Dim_Geography

    Used when: Dimension hierarchy exists, data duplication must be minimized
    
  * Galaxy schema : All fact tables share dimention tables
    >Fact_Orders, Fact_Returns, Fact_Inventory -> Dim_Product, Dim_Date, Dim_Customer

    Used for: Full enterprise analytics, Supply chain + sales analysis
