# Silver Layer — PySpark Transformations

## Purpose

The Silver layer takes raw Bronze data and makes it **trustworthy and usable**. This is where the bulk of the data engineering work happens — and where interviewers want to see PySpark skills.

Six transformations are applied in sequence:

1. **Null handling** — rows missing key columns are dropped
2. **Cancellation removal** — `InvoiceNo` starting with `'C'` are excluded
3. **Type casting** — all columns cast from STRING to correct types
4. **Quarantine isolation** — malformed rows go to `quarantine_transactions`, not silently dropped
5. **Derived columns** — `Revenue = Quantity × UnitPrice`, `OrderDate = to_date(InvoiceDate)`
6. **Deduplication** — composite key dedup using Window functions

**Table:** `workspace.silver.transactions` (partitioned by `order_date`)
**Quarantine:** `workspace.silver.quarantine_transactions`
**Implementation:** [`notebooks/02_transform_silver.py`](../notebooks/02_transform_silver.py)

---

## Why PySpark DataFrame API (Not SQL Strings)

The Silver notebook uses the PySpark DataFrame API throughout — `filter()`, `withColumn()`, `cast()`, `Window` — rather than `spark.sql("SELECT ...")` strings.

**Reasons:**
- **Type safety** — the Catalyst query planner validates column references at compile time
- **Composability** — transformations chain naturally without string concatenation
- **Testability** — individual transformation steps can be unit tested
- **Readability** — the transformation intent is explicit in code, not buried in SQL strings

---

## Null Handling

```python
df = df.filter(
    col("InvoiceNo").isNotNull() &
    col("StockCode").isNotNull() &
    col("CustomerID").isNotNull()
)
```

Three columns are the minimum required keys. `InvoiceNo` identifies the transaction, `StockCode` identifies the product, `CustomerID` is required to join to `dim_customer` in the Gold layer. Rows missing any of these cannot be meaningfully used downstream.

---

## Cancellation Removal

```python
df = df.filter(~col("InvoiceNo").startswith("C"))
```

In the Online Retail dataset, cancelled orders have `InvoiceNo` starting with `'C'` (e.g., `C536379`). These represent reversals of previous transactions. Including them would distort revenue calculations — a cancellation row has a negative quantity, which would reduce total revenue incorrectly.

If you need to analyse cancellations, query `workspace.bronze.raw_online_retail` directly.

---

## Type Casting

```python
df = df.withColumn("quantity_cast",    col("Quantity").cast(IntegerType()))
       .withColumn("unit_price_cast",  col("UnitPrice").cast(DecimalType(10, 2)))
       .withColumn("invoice_date_cast", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
```

The `InvoiceDate` format string `'M/d/yyyy H:mm'` matches the source format (e.g., `12/1/2010 8:26`). If the format doesn't match, `to_timestamp()` returns NULL — these rows are caught by the quarantine step.

---

## Quarantine vs Silent Drop

Instead of silently dropping malformed rows, the Silver notebook isolates them in `workspace.silver.quarantine_transactions` with a `rejection_reason` column.

| Rejection reason | Condition |
|---|---|
| `NEGATIVE_QUANTITY` | `quantity <= 0` after casting |
| `INVALID_TIMESTAMP` | `invoice_date` is NULL after casting |
| `NEGATIVE_PRICE` | `unit_price < 0` after casting |

**Note:** `unit_price = 0` rows are **not** quarantined — they represent valid free or promotional items. Only strictly negative prices indicate data errors.

This approach enables:
- Investigation of rejected rows without data loss
- Reprocessing after fixing the source data
- Audit trail of data quality issues per pipeline run

---

## Deduplication with Window Functions

```python
dedup_window = Window.partitionBy(
    "invoice_no", "stock_code", "invoice_date_cast", "quantity_cast"
).orderBy(col("_ingestion_timestamp").desc())

df = df.withColumn("_rn", row_number().over(dedup_window)).filter(col("_rn") == 1)
```

**Why four columns, not two?**
Using only `(invoice_no, stock_code)` risks collapsing legitimately distinct rows. The same `StockCode` can appear on the same `InvoiceNo` with different quantities (e.g., a correction entry). Including `invoice_date` and `quantity` ensures only true duplicates — identical rows ingested more than once — are collapsed.

`ROW_NUMBER()` assigns rank 1 to the most recently ingested row within each group. Only `rn = 1` rows are kept.

---

## Derived Columns

```python
df = df.withColumn("revenue", (col("quantity_cast") * col("unit_price_cast")).cast(DecimalType(12, 2)))
       .withColumn("order_date", to_date(col("invoice_date_cast")))
```

`Revenue` is the primary measure in `fact_sales`. Calculating it in Silver means all downstream consumers get it without recomputation.

`OrderDate` is the date portion of `InvoiceDate`. It's used as the partition key for the Silver table and as the join key to `dim_date` in the Gold layer.

---

## Partitioning

```python
silver_df.write.format("delta").mode("overwrite").partitionBy("order_date").saveAsTable(...)
```

Partitioning by `order_date` means Spark stores data in separate directories per date. When the Gold notebook reads Silver for a specific date range, Spark only reads the relevant partitions — this is called **partition pruning** and significantly reduces I/O for large tables.

---

## Further Reading

- [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [PySpark Window functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [Delta Lake partitioning](https://docs.databricks.com/delta/best-practices.html#choose-the-right-partition-column)
