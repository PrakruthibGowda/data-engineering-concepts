
# Slowly Changing Dimensions (SCD)

Slowly Changing Dimensions is a key concept in **data warehousing** that deals with how dimension tables manage and track changes to data over time. Since dimension data (e.g., customer addresses, product names, employee titles) doesn't stay static forever, we need strategies to handle these changes.

## Why Handle Slowly Changing Dimensions?

Dimension data — such as customer addresses, product categories, and employee roles — inevitably changes over time. Without a strategy, these changes silently corrupt your data warehouse.

### Key Reasons

#### 1. Historical Accuracy
Without SCD handling, reports always reflect the *current* state. A customer who moved from London to Manchester would have all past orders attributed to Manchester, making historical regional analysis unreliable.

#### 2. Audit & Compliance
Regulated industries require proof of what data looked like at a specific point in time. SCD strategies (especially Type 2) provide a defensible audit trail.

#### 3. Accurate Analytics & Attribution
If a product changes category or a sales rep switches territory, analytics must reflect the context that was true *when the event occurred* — not today's context.

#### 4. Data Integrity
Fact tables reference dimension keys. Silently overwriting dimensions breaks the relationship between facts and their original context, leading to inconsistent results.

#### 5. Change Is Inevitable
Customers move, products get reclassified, employees change roles. Ignoring these changes doesn't prevent them — it just means your warehouse drifts from reality.

## Summary

  | Without SCD Handling          | With SCD Handling                   |
  |-------------------------------|-------------------------------------|
  | Reports show only current state | Reports reflect the truth *at the time* |
  | No audit trail                | Full traceability of changes        |
  | Broken historical analysis    | Accurate trend analysis             |
  | Silent data corruption        | Explicit, controlled change management |

**Bottom line:** SCD handling ensures your data warehouse tells the *true* story, not just the *latest* one.

## Common SCD Types

#### Type 0 — Retain Original

  - **No changes** are made. The dimension value is fixed at the time of initial load.
  - Use case: Data that should never change (e.g., original signup date).

#### Type 1 — Overwrite

  - The old value is simply **overwritten** with the new value.
  - **No history** is preserved.
  - Simple but you lose the ability to track what changed.

  | customer_id | name  | city     |
  |-------------|-------|----------|
  | 101         | Alice | New York |

  After Alice moves:

  | customer_id | name  | city    |
  |-------------|-------|---------|
  | 101         | Alice | Chicago |

#### Type 2 — Add New Row (Most Common)

  - A **new row** is inserted for each change, preserving full history.
  - Typically uses `effective_date`, `expiry_date`, and/or an `is_current` flag.
  - This is the **most widely used** approach for tracking historical changes.

  | surrogate_key | customer_id | city     | effective_date | expiry_date | is_current |
  |---------------|-------------|----------|----------------|-------------|------------|
  | 1             | 101         | New York | 2023-01-01     | 2025-06-15  | N          |
  | 2             | 101         | Chicago  | 2025-06-15     | 9999-12-31  | Y          |

#### Type 3 — Add New Column

  - Adds a **new column** to store the previous value (e.g., `previous_city`).
  - Only tracks **one level** of history.

  | customer_id | city    | previous_city |
  |-------------|---------|---------------|
  | 101         | Chicago | New York      |

#### Type 4 — History Table

  - Current data lives in the **main dimension table**, and all historical records are stored in a **separate history table**.
  - Keeps the main table lean and fast for queries.


#### SCD Type 5 — Mini-Dimension + Type 1 Outrigger

  SCD Type 5 combines a **Type 4 mini-dimension** with a **Type 1 overwrite** reference on the base dimension. It is designed for attributes that change too frequently for Type 2 to be practical.

  **How It Works**

  1. Frequently changing attributes are moved into a separate **mini-dimension** table.
  2. The **fact table** links to the mini-dimension, preserving the historical value at the time of the event.
  3. The **base dimension** holds an overwritten (Type 1) foreign key pointing to the current mini-dimension row for quick access to the latest profile.

  **Example**

  **Customer Dimension (base):**

  | customer_key | customer_id | name  | current_demo_key |
  |--------------|-------------|-------|------------------|
  | 1            | 101         | Alice | 3                |

  **Demographic Mini-Dimension:**

  | demo_key | income_band | age_band |
  |----------|-------------|----------|
  | 1        | Medium      | 25-34    |
  | 2        | Medium      | 35-44    |
  | 3        | High        | 35-44    |

  **Fact Table:**

  | order_key | customer_key | demo_key | amount |
  |-----------|--------------|----------|--------|
  | 1001      | 1            | 1        | 50.00  |
  | 1002      | 1            | 3        | 75.00  |

  - `demo_key` in the fact table captures the demographic **at the time** of the transaction.
  - `current_demo_key` in the base dimension always reflects the **current** demographic.

  ##### When to Use

  - Attributes change **too frequently** for Type 2 (which would cause excessive row growth).
  - You need **both** historical and current views of rapidly changing attributes.
  - Common in retail and marketing for demographic or behavioural segmentation.

#### Type 6 — Hybrid (1 + 2 + 3)

  - Combines Types 1, 2, and 3 (1 + 2 + 3 = 6, hence the name).
  - New rows are added (Type 2), a `current_value` column is overwritten on all rows (Type 1), and a `previous_value` column is maintained (Type 3).
  - Offers maximum flexibility but adds complexity.

## When to Use What?

  | Type | History?   | Complexity  | Best For                                                              |
  |------|------------|-------------|-----------------------------------------------------------------------|
  | 0    | None       | Very Low    | Immutable attributes                                                  |
  | 1    | None       | Low         | Corrections, non-critical changes                                     |
  | 2    | Full       | Medium      | Audit trails, regulatory compliance                                   |
  | 3    | Limited    | Low-Medium  | Tracking only the last change                                         |
  | 4    | Full       | Medium      | Performance-sensitive queries on current data                         |
  | 5    | Full       | Medium-High | Rapidly changing attributes needing both historical and current views |
  | 6    | Full       | High        | Complex analytical needs                                              |

## Key Takeaway

  **Type 2** is the most common in practice because it preserves full history with a well-understood pattern. It is the approach most frequently encountered when loading dimension tables in ETL pipelines.
