# Gold Layer — Star Schema Dimensional Model

## Purpose

The Gold layer contains a **star schema dimensional model** — the standard analytical data model used in data warehouses. It's designed for fast, flexible querying by BI tools and analysts.

**Tables:**

| Table | Type | Description |
|---|---|---|
| `fact_sales` | Fact | One row per invoice line item — measures + foreign keys |
| `dim_customer` | Dimension (SCD2) | Customer with country change history |
| `dim_product` | Dimension | Products with keyword-derived category |
| `dim_date` | Dimension | Date spine with calendar attributes |
| `dim_country` | Dimension | Countries with derived region |

**Implementation:** [`notebooks/03_build_gold.py`](../notebooks/03_build_gold.py)

---

## Star Schema Design

A star schema has a central fact table surrounded by dimension tables. The fact table stores **measures** (numbers you aggregate) and **foreign keys** to dimensions. Dimensions provide **descriptive context** (who, what, when, where).

```
         dim_date
            |
dim_country — fact_sales — dim_customer
            |
        dim_product
```

**Why star schema over a flat table?**
- Queries are simpler — join fact to one or more dimensions
- Aggregations are fast — pre-computed surrogate keys avoid string joins
- Flexible — you can slice by any combination of dimensions
- Standard — every BI tool understands star schemas

---

## Grain Definition

**`fact_sales` grain: one row per invoice line item (`InvoiceNo` + `StockCode`).**

The grain is the most important design decision for a fact table. It defines exactly what one row represents. At line-item grain:
- Aggregate to order level: `GROUP BY invoice_no`
- Aggregate to customer level: `GROUP BY customer_key`
- Aggregate to product level: `GROUP BY product_key`
- Aggregate to monthly level: `GROUP BY year, month`

---

## Surrogate Key Strategy

All surrogate keys use `xxhash64()` — a deterministic hash function.

**Why not `monotonically_increasing_id()`?**
In distributed Spark execution, `monotonically_increasing_id()` produces values that are unique within a run but non-deterministic across runs. The same row may get a different key on a re-run. This breaks incremental MERGE operations.

**Why `xxhash64()`?**
Given the same input values, `xxhash64()` always produces the same output — making surrogate keys reproducible across pipeline runs. This is essential for MERGE-based upserts.

| Table | Key expression |
|---|---|
| `dim_customer` | `xxhash64(customer_id, effective_date)` |
| `dim_product` | `xxhash64(stock_code)` |
| `dim_country` | `xxhash64(country_name)` |
| `fact_sales` | `xxhash64(invoice_no, stock_code, invoice_date, customer_id)` |
| `dim_date` | `CAST(date_format(full_date, 'yyyyMMdd') AS INT)` — inherently deterministic |

---

## SCD Type 2 — dim_customer

SCD Type 2 tracks historical changes by adding new rows rather than overwriting. Each version has `effective_date`, `expiry_date`, and `current_flag`.

**Trigger:** a new version is created when a customer's `country` changes.

```
customer_id | country        | effective_date | expiry_date | current_flag
12345       | UNITED KINGDOM | 2010-12-01     | 2011-03-15  | False
12345       | GERMANY        | 2011-03-15     | NULL        | True
```

**Why SCD2 over SCD1 (overwrite)?**
SCD1 would lose the history — you'd never know the customer was previously in the UK. SCD2 preserves the full history, enabling accurate historical analysis: "what was this customer's country when they placed this order?"

**Temporal join for fact_sales:**
```python
(col("order_date") >= col("effective_date")) &
((col("order_date") < col("expiry_date")) | col("expiry_date").isNull())
```

This is the correct form. `BETWEEN` fails for NULL `expiry_date` (the current record). The explicit predicate handles both open-ended current versions and closed historical versions.

---

## Incremental MERGE Strategy

Gold tables use MERGE-based upserts rather than full rebuilds. This is the production-grade approach.

**dim_product, dim_country:** MERGE on natural key — insert new rows, update changed attributes.

**dim_customer (SCD2):** Two-step MERGE:
1. Expire old version: UPDATE `expiry_date` and `current_flag = False`
2. Insert new version: APPEND with `current_flag = True`

**fact_sales:** MERGE on `sale_key` — `WHEN NOT MATCHED THEN INSERT *`. Effectively append-only since `sale_key` is deterministic. The MERGE prevents duplicate insertion on Bronze replays.

---

## Delta OPTIMIZE and ZORDER

```python
spark.sql("OPTIMIZE workspace.gold.fact_sales ZORDER BY (customer_key, product_key)")
```

**ZORDER** co-locates rows with the same `customer_key` and `product_key` in the same data files. When a query joins `fact_sales` to `dim_customer` and `dim_product`, Spark reads fewer files (data skipping).

**Free Edition caveat:** OPTIMIZE/ZORDER support may vary in Free Edition serverless environments. The pipeline wraps these calls in `try/except` and degrades gracefully if they fail — data is correct, queries may be slower.

---

## Further Reading

- [Kimball dimensional modelling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Delta MERGE](https://docs.databricks.com/delta/merge.html)
- [Delta OPTIMIZE and ZORDER](https://docs.databricks.com/delta/optimize.html)
- [SCD Type 2 patterns](https://docs.databricks.com/delta/merge.html#slowly-changing-data-scd-type-2-using-merge)
