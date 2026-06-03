# Databricks notebook source
# =============================================================================
# 02_transform_silver.py — Silver Layer PySpark Transformations
# =============================================================================
# PURPOSE:
#   Reads raw Bronze data and produces a clean, typed, validated Silver table.
#   This notebook demonstrates production-grade PySpark skills:
#     - Null handling
#     - Cancellation removal
#     - Type casting with PySpark DataFrame API (not SQL strings)
#     - Quarantine isolation (malformed rows → silver.quarantine_transactions)
#     - String standardisation
#     - Derived columns (Revenue, OrderDate)
#     - Deduplication with Window functions
#     - Full-refresh and incremental write modes
#
# BATCH READ (NOT STREAMING):
#   Silver is a batch transformation — it reads the entire Bronze table (or
#   a filtered subset for incremental mode) as a static DataFrame, not a stream.
#   This is appropriate because Silver transformations are complex and stateful
#   (deduplication requires seeing all rows for a given key).
#
# WRITE MODES:
#   Full refresh (default): overwrite the Silver table — used for initial load
#   Incremental: append only new records — used for subsequent runs
# =============================================================================

# COMMAND ----------

import uuid
import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    IntegerType, DecimalType, StringType, TimestampType
)

# COMMAND ----------

# =============================================================================
# SECTION 1: Widgets and Setup
# =============================================================================

# pipeline_run_id: read from the ingest_bronze task value.
# debugValue provides a fallback UUID for interactive runs outside a Workflow.
pipeline_run_id = dbutils.jobs.taskValues.get(
    taskKey="ingest_bronze",
    key="pipeline_run_id",
    debugValue=str(uuid.uuid4()),
)

# run_mode: controls whether to do a full overwrite or incremental append.
# In the Databricks Workflow, this is always "full_refresh" for the initial
# load. Set to "incremental" for subsequent runs to append only new records.
dbutils.widgets.dropdown("run_mode", "full_refresh", ["full_refresh", "incremental"])
run_mode = dbutils.widgets.get("run_mode")

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"run_mode        : {run_mode}")
print(f"Started at      : {datetime.utcnow().isoformat()}Z")

run_start_time = time.time()

# COMMAND ----------

# =============================================================================
# SECTION 2: Create Silver Tables (DDL)
# =============================================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver transactions table: clean, typed, deduplicated records.
# MAGIC -- Partitioned by order_date for efficient partition pruning in Gold reads.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.transactions (
# MAGIC   invoice_no          STRING        COMMENT 'Invoice number (cancellations removed)',
# MAGIC   stock_code          STRING        COMMENT 'Product code (trimmed, upper-cased)',
# MAGIC   description         STRING        COMMENT 'Product description (trimmed); NULL allowed',
# MAGIC   quantity            INT           COMMENT 'Units purchased (cast from STRING; <= 0 quarantined)',
# MAGIC   invoice_date        TIMESTAMP     COMMENT 'Transaction timestamp (parsed from M/d/yyyy H:mm)',
# MAGIC   order_date          DATE          COMMENT 'Date portion of invoice_date (partition key)',
# MAGIC   unit_price          DECIMAL(10,2) COMMENT 'Price per unit in GBP (< 0 quarantined; 0 allowed)',
# MAGIC   customer_id         STRING        COMMENT 'Customer identifier (trimmed; NULL rows removed)',
# MAGIC   country             STRING        COMMENT 'Country of the customer (trimmed, upper-cased)',
# MAGIC   revenue             DECIMAL(12,2) COMMENT 'Derived: quantity * unit_price',
# MAGIC   _source_file_name   STRING        COMMENT 'Passed through from Bronze',
# MAGIC   _ingestion_timestamp TIMESTAMP    COMMENT 'Passed through from Bronze',
# MAGIC   _pipeline_run_id    STRING        COMMENT 'Correlation ID',
# MAGIC   _silver_updated_at  TIMESTAMP     COMMENT 'current_timestamp() at Silver write time'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (order_date)
# MAGIC COMMENT 'Silver cleansed transactions. Partitioned by order_date for efficient Gold reads.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver quarantine: malformed rows isolated during transformation.
# MAGIC -- Three rejection reasons: NEGATIVE_QUANTITY, INVALID_TIMESTAMP, NEGATIVE_PRICE.
# MAGIC -- Note: unit_price = 0 rows are NOT quarantined (valid free/promotional items).
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.quarantine_transactions (
# MAGIC   raw_invoice_no      STRING    COMMENT 'Original InvoiceNo value',
# MAGIC   raw_stock_code      STRING    COMMENT 'Original StockCode value',
# MAGIC   raw_quantity        STRING    COMMENT 'Original Quantity value',
# MAGIC   raw_unit_price      STRING    COMMENT 'Original UnitPrice value',
# MAGIC   raw_invoice_date    STRING    COMMENT 'Original InvoiceDate value',
# MAGIC   raw_customer_id     STRING    COMMENT 'Original CustomerID value',
# MAGIC   raw_country         STRING    COMMENT 'Original Country value',
# MAGIC   raw_description     STRING    COMMENT 'Original Description value',
# MAGIC   rejection_reason    STRING    COMMENT 'NEGATIVE_QUANTITY | INVALID_TIMESTAMP | NEGATIVE_PRICE',
# MAGIC   _pipeline_run_id    STRING    COMMENT 'Correlation ID',
# MAGIC   _quarantine_timestamp TIMESTAMP COMMENT 'When the row was quarantined'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Silver quarantine: malformed rows isolated during transformation. Enables investigation and reprocessing.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver quality failures: dbt test failures loaded by 05_load_quality_failures.py
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.quality_failures (
# MAGIC   pipeline_run_id     STRING    COMMENT 'Correlated run ID',
# MAGIC   check_name          STRING    COMMENT 'dbt test name',
# MAGIC   model_name          STRING    COMMENT 'dbt model/source the test ran against',
# MAGIC   severity            STRING    COMMENT 'warn or error',
# MAGIC   failing_row_count   BIGINT    COMMENT 'Number of rows that failed the test',
# MAGIC   sample_failing_values STRING  COMMENT 'JSON string of the dbt test failure message',
# MAGIC   recorded_at         TIMESTAMP COMMENT 'When this failure row was written'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'dbt test failures loaded after each dbt test run.';

# COMMAND ----------

# =============================================================================
# SECTION 3: Read Bronze Table
# =============================================================================
# Batch read — not streaming. Silver transformations are complex and stateful
# (deduplication requires seeing all rows for a given key at once).

if run_mode == "incremental":
    # Incremental mode: only process records from the current pipeline_run_id
    # that haven't been written to Silver yet.
    # This avoids reprocessing the entire Bronze table on every run.
    existing_run_ids = [
        row["_pipeline_run_id"]
        for row in spark.sql(
            "SELECT DISTINCT _pipeline_run_id FROM workspace.silver.transactions"
        ).collect()
    ]
    bronze_df = (
        spark.read.format("delta")
        .table("workspace.bronze.raw_online_retail")
        .filter(~F.col("_pipeline_run_id").isin(existing_run_ids))
    )
    print(f"Incremental mode: filtering to new pipeline_run_ids not yet in Silver")
else:
    # Full refresh mode: read the entire Bronze table
    bronze_df = spark.read.format("delta").table("workspace.bronze.raw_online_retail")

total_bronze_rows = bronze_df.count()
print(f"Bronze rows to process : {total_bronze_rows:,}")

# COMMAND ----------

# =============================================================================
# SECTION 4: Null Handling
# =============================================================================
# Drop rows where the three minimum required key columns are NULL.
# These rows cannot be meaningfully transformed or joined in the Gold layer.
#
# InvoiceNo  — required to identify the transaction
# StockCode  — required to identify the product
# CustomerID — required to join to dim_customer
#
# Note: Description, Country, and other columns may be NULL and are allowed.

df = bronze_df.filter(
    F.col("InvoiceNo").isNotNull() &
    F.col("StockCode").isNotNull() &
    F.col("CustomerID").isNotNull()
)

null_removed = total_bronze_rows - df.count()
print(f"Rows removed (null key columns) : {null_removed:,}")

# COMMAND ----------

# =============================================================================
# SECTION 5: Cancellation Removal
# =============================================================================
# In the Online Retail dataset, cancelled orders have InvoiceNo starting with 'C'
# (e.g., 'C536379'). These represent reversals of previous transactions and
# should not be included in the Silver layer — they would distort revenue
# calculations and product metrics.
#
# If you need to analyse cancellations, query the Bronze table directly.

df_before_cancel = df.count()
df = df.filter(~F.col("InvoiceNo").startswith("C"))

cancellations_removed = df_before_cancel - df.count()
print(f"Cancellation rows removed : {cancellations_removed:,}")

# COMMAND ----------

# =============================================================================
# SECTION 6: Type Casting
# =============================================================================
# Cast all columns from STRING (Bronze) to their correct types.
# We use the PySpark DataFrame API (not SQL strings) for type safety and
# Catalyst optimisation.
#
# Rows that fail casting (produce NULL where NULL is not allowed) are
# identified and routed to the quarantine table in Section 7.

# Cast Quantity to INT
# The Online Retail dataset has some negative quantities (returns/adjustments).
# We cast first, then quarantine negatives in Section 7.
df = df.withColumn("quantity_cast", F.col("Quantity").cast(IntegerType()))

# Cast UnitPrice to DECIMAL(10,2)
# Some rows have UnitPrice = 0 (free/promotional items) — these are valid.
# Negative prices are quarantined in Section 7.
df = df.withColumn("unit_price_cast", F.col("UnitPrice").cast(DecimalType(10, 2)))

# Cast InvoiceDate to TIMESTAMP
# Format: 'M/d/yyyy H:mm' (e.g., '12/1/2010 8:26' → 2010-12-01 08:26:00)
# Rows where the date cannot be parsed produce NULL — quarantined in Section 7.
df = df.withColumn(
    "invoice_date_cast",
    F.to_timestamp(F.col("InvoiceDate"), "M/d/yyyy H:mm")
)

# COMMAND ----------

# =============================================================================
# SECTION 7: Quarantine Malformed Rows
# =============================================================================
# Instead of silently dropping malformed rows, we isolate them in
# workspace.silver.quarantine_transactions with a rejection_reason.
# This enables investigation and reprocessing without data loss.
#
# Three rejection rules:
#   NEGATIVE_QUANTITY  — quantity <= 0 after casting
#   INVALID_TIMESTAMP  — invoice_date is NULL after casting (unparseable)
#   NEGATIVE_PRICE     — unit_price < 0 after casting
#
# Note: unit_price = 0 is NOT quarantined — zero-price rows represent valid
# free or promotional items (e.g., samples, gifts). Only strictly negative
# prices indicate data errors.

def build_quarantine_row(df_source, condition, reason):
    """Extract rows matching condition and format them for the quarantine table."""
    return (
        df_source.filter(condition)
        .select(
            F.col("InvoiceNo").alias("raw_invoice_no"),
            F.col("StockCode").alias("raw_stock_code"),
            F.col("Quantity").alias("raw_quantity"),
            F.col("UnitPrice").alias("raw_unit_price"),
            F.col("InvoiceDate").alias("raw_invoice_date"),
            F.col("CustomerID").alias("raw_customer_id"),
            F.col("Country").alias("raw_country"),
            F.col("Description").alias("raw_description"),
            F.lit(reason).alias("rejection_reason"),
            F.lit(pipeline_run_id).alias("_pipeline_run_id"),
            F.current_timestamp().alias("_quarantine_timestamp"),
        )
    )

# Identify quarantine conditions
negative_qty_condition = (
    F.col("quantity_cast").isNotNull() & (F.col("quantity_cast") <= 0)
)
invalid_ts_condition = F.col("invoice_date_cast").isNull()
negative_price_condition = (
    F.col("unit_price_cast").isNotNull() & (F.col("unit_price_cast") < 0)
)

# Build quarantine DataFrames
quarantine_negative_qty = build_quarantine_row(df, negative_qty_condition, "NEGATIVE_QUANTITY")
quarantine_invalid_ts   = build_quarantine_row(df, invalid_ts_condition,   "INVALID_TIMESTAMP")
quarantine_negative_price = build_quarantine_row(df, negative_price_condition, "NEGATIVE_PRICE")

# Union all quarantine rows and write
from functools import reduce
from pyspark.sql import DataFrame

quarantine_dfs = [quarantine_negative_qty, quarantine_invalid_ts, quarantine_negative_price]
quarantine_all = reduce(DataFrame.union, quarantine_dfs)

quarantine_count = quarantine_all.count()

if quarantine_count > 0:
    (
        quarantine_all.write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.silver.quarantine_transactions")
    )

# Log quarantine counts per reason
print(f"Quarantine rows written : {quarantine_count:,}")
print(f"  NEGATIVE_QUANTITY     : {quarantine_negative_qty.count():,}")
print(f"  INVALID_TIMESTAMP     : {quarantine_invalid_ts.count():,}")
print(f"  NEGATIVE_PRICE        : {quarantine_negative_price.count():,}")
print(f"  (unit_price = 0 rows pass through — valid free/promotional items)")

# Remove quarantine rows from the main DataFrame
# A row is quarantined if it matches ANY of the three conditions
quarantine_condition = (
    negative_qty_condition | invalid_ts_condition | negative_price_condition
)
df = df.filter(~quarantine_condition)

print(f"Rows after quarantine removal : {df.count():,}")

# COMMAND ----------

# =============================================================================
# SECTION 8: String Standardisation
# =============================================================================
# Trim whitespace and upper-case country/stock_code for consistent joins.
# Description is trimmed but not upper-cased (preserves readability).

df = (
    df
    .withColumn("invoice_no",   F.trim(F.col("InvoiceNo")))
    .withColumn("stock_code",   F.upper(F.trim(F.col("StockCode"))))
    .withColumn("description",  F.trim(F.col("Description")))
    .withColumn("customer_id",  F.trim(F.col("CustomerID")))
    .withColumn("country",      F.upper(F.trim(F.col("Country"))))
)

# COMMAND ----------

# =============================================================================
# SECTION 9: Derived Columns
# =============================================================================
# Revenue = Quantity * UnitPrice
#   This is the primary measure in the Gold fact table. Calculated here in
#   Silver so it's available for all downstream consumers without recomputation.
#
# OrderDate = to_date(InvoiceDate)
#   The date portion of the invoice timestamp. Used as the partition key for
#   the Silver table and as the join key to dim_date in the Gold layer.

df = (
    df
    .withColumn(
        "revenue",
        (F.col("quantity_cast") * F.col("unit_price_cast")).cast(DecimalType(12, 2))
    )
    .withColumn("order_date", F.to_date(F.col("invoice_date_cast")))
    .withColumn("_silver_updated_at", F.current_timestamp())
)

# COMMAND ----------

# =============================================================================
# SECTION 10: Deduplication
# =============================================================================
# Deduplication key: (invoice_no, stock_code, invoice_date, quantity)
#
# WHY FOUR COLUMNS, NOT TWO?
#   Using only (invoice_no, stock_code) risks collapsing legitimately distinct
#   rows. In the Online Retail dataset, the same StockCode can appear on the
#   same InvoiceNo with different quantities (e.g., a correction entry).
#   Including invoice_date and quantity ensures only true duplicates — identical
#   rows ingested more than once — are collapsed.
#
# STRATEGY:
#   Use ROW_NUMBER() over a window partitioned by the dedup key, ordered by
#   _ingestion_timestamp DESC. Keep only rn = 1 (the most recently ingested
#   row for each unique combination). This handles the case where the same
#   row was ingested multiple times due to a Bronze replay.

dedup_window = Window.partitionBy(
    "invoice_no", "stock_code", "invoice_date_cast", "quantity_cast"
).orderBy(F.col("_ingestion_timestamp").desc())

df = (
    df
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"Rows after deduplication : {df.count():,}")

# COMMAND ----------

# =============================================================================
# SECTION 11: Final Column Selection and Rename
# =============================================================================
# Select only the final Silver columns in the correct order.
# Rename cast columns to their final snake_case names.

silver_df = df.select(
    F.col("invoice_no"),
    F.col("stock_code"),
    F.col("description"),
    F.col("quantity_cast").alias("quantity"),
    F.col("invoice_date_cast").alias("invoice_date"),
    F.col("order_date"),
    F.col("unit_price_cast").alias("unit_price"),
    F.col("customer_id"),
    F.col("country"),
    F.col("revenue"),
    F.col("_source_file_name"),
    F.col("_ingestion_timestamp"),
    F.col("_pipeline_run_id"),
    F.col("_silver_updated_at"),
)

final_row_count = silver_df.count()
print(f"Final Silver rows to write : {final_row_count:,}")

# COMMAND ----------

# =============================================================================
# SECTION 12: Write to workspace.silver.transactions
# =============================================================================
# Full refresh: overwrite the entire Silver table.
#   Used for initial load and forced replays.
#   overwriteSchema=True allows schema evolution if the Silver DDL changes.
#
# Incremental: append only new records.
#   Used for subsequent runs when only new Bronze data needs to be processed.
#
# Partitioned by order_date: enables partition pruning in Gold reads.
# When the Gold notebook reads Silver for a specific date range, Spark only
# reads the relevant partitions instead of scanning the entire table.

status = "SUCCESS"
write_error = None

try:
    if run_mode == "full_refresh":
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("order_date")
            .saveAsTable("workspace.silver.transactions")
        )
        print(f"✅ Full refresh write complete: {final_row_count:,} rows written.")
    else:
        (
            silver_df.write
            .format("delta")
            .mode("append")
            .partitionBy("order_date")
            .saveAsTable("workspace.silver.transactions")
        )
        print(f"✅ Incremental append complete: {final_row_count:,} rows appended.")

except Exception as e:
    status = "FAILURE"
    write_error = e
    print(f"❌ Silver write failed: {e}")

# COMMAND ----------

# =============================================================================
# SECTION 13: Quality Metrics Summary
# =============================================================================

duration_seconds = time.time() - run_start_time

print("\n" + "="*60)
print("SILVER TRANSFORMATION QUALITY METRICS")
print("="*60)
print(f"Bronze rows read          : {total_bronze_rows:,}")
print(f"Null key rows removed     : {null_removed:,}")
print(f"Cancellation rows removed : {cancellations_removed:,}")
print(f"Quarantine rows           : {quarantine_count:,}")
print(f"Final Silver rows written : {final_row_count:,}")
print(f"Duration                  : {duration_seconds:.1f}s")
print(f"Status                    : {status}")
print("="*60)

# COMMAND ----------

# =============================================================================
# SECTION 14: Audit Log Write
# =============================================================================

audit_row = spark.createDataFrame(
    [(
        pipeline_run_id,
        "SILVER_COMPLETE",
        int(final_row_count),
        None,
        ["transactions"],
        duration_seconds,
        status,
    )],
    schema=(
        "pipeline_run_id STRING, "
        "event_type STRING, "
        "rows_processed BIGINT, "
        "source_file_name STRING, "
        "models_refreshed ARRAY<STRING>, "
        "duration_seconds DOUBLE, "
        "status STRING"
    ),
).withColumn("logged_at", F.current_timestamp())

(
    audit_row.write
    .format("delta")
    .mode("append")
    .saveAsTable("workspace.bronze.pipeline_audit_log")
)

print(f"✅ Audit log written: SILVER_COMPLETE, rows={final_row_count:,}, duration={duration_seconds:.1f}s")

# COMMAND ----------

# Re-raise on failure
if write_error is not None:
    raise write_error
