# Databricks notebook source
# =============================================================================
# 03_build_gold.py — Gold Layer: Star Schema Dimensional Model
# =============================================================================
# PURPOSE:
#   Builds a production-grade star schema dimensional model from the Silver
#   transactions table. This notebook demonstrates:
#     - Star schema design (fact + dimension tables)
#     - Deterministic surrogate keys using xxhash64()
#     - SCD Type 2 on dim_customer (country change tracking)
#     - Keyword-derived product categories
#     - Temporal join correctness for SCD2 fact resolution
#     - Incremental MERGE-based upserts (no full rebuilds)
#     - Delta OPTIMIZE/ZORDER with graceful Free Edition degradation
#
# STAR SCHEMA:
#   fact_sales (grain: one row per invoice line item)
#     → dim_customer (SCD Type 2 on country change)
#     → dim_product  (keyword-derived category)
#     → dim_date     (date spine)
#     → dim_country  (region mapping)
#
# BUILD ORDER:
#   Dimensions must be built BEFORE the fact table because fact_sales
#   resolves surrogate keys by joining to each dimension.
#   Order: dim_date → dim_country → dim_product → dim_customer → fact_sales
#
# SURROGATE KEY STRATEGY:
#   All surrogate keys use xxhash64() — a deterministic hash function.
#   This ensures keys are reproducible across pipeline runs, unlike
#   monotonically_increasing_id() which produces non-deterministic values
#   in distributed execution and breaks incremental MERGE operations.
# =============================================================================

# COMMAND ----------

import uuid
import time
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    IntegerType, LongType, DecimalType, StringType,
    DateType, BooleanType, StructType, StructField
)

# COMMAND ----------

# =============================================================================
# SECTION 1: Setup
# =============================================================================

pipeline_run_id = dbutils.jobs.taskValues.get(
    taskKey="ingest_bronze",
    key="pipeline_run_id",
    debugValue=str(uuid.uuid4()),
)

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"Started at      : {datetime.utcnow().isoformat()}Z")

run_start_time = time.time()

# Read Silver transactions — the single source for all Gold tables
silver = spark.read.format("delta").table("workspace.silver.transactions")
silver.cache()  # Cache since we read it multiple times for different dimensions

total_silver_rows = silver.count()
print(f"Silver rows     : {total_silver_rows:,}")

# COMMAND ----------

# =============================================================================
# SECTION 2: dim_date — Date Spine
# =============================================================================
# Generate a complete date spine from the min to max order_date in Silver.
# dim_date is a static reference table — no SCD, no MERGE needed for existing
# dates. New dates are appended as the dataset grows.
#
# date_key is an integer in YYYYMMDD format (e.g., 20101201 for 2010-12-01).
# This is inherently deterministic — no hash needed.

print("\n--- Building dim_date ---")

# Get date range from Silver
date_range = silver.agg(
    F.min("order_date").alias("min_date"),
    F.max("order_date").alias("max_date")
).collect()[0]

min_date = date_range["min_date"]
max_date = date_range["max_date"]
num_days = (max_date - min_date).days + 1

print(f"Date range: {min_date} → {max_date} ({num_days} days)")

# Generate date spine using spark.range() + date_add()
# spark.range(n) produces a DataFrame with a single column 'id' from 0 to n-1
date_spine = (
    spark.range(num_days)
    .withColumn("full_date", F.date_add(F.lit(min_date), F.col("id").cast(IntegerType())))
    .withColumn("full_date", F.col("full_date").cast(DateType()))
    .withColumn("date_key",  F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("day",       F.dayofmonth(F.col("full_date")))
    .withColumn("month",     F.month(F.col("full_date")))
    .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
    .withColumn("quarter",   F.quarter(F.col("full_date")))
    .withColumn("year",      F.year(F.col("full_date")))
    .withColumn("week_of_year", F.weekofyear(F.col("full_date")))
    # weekend_flag: True if Saturday (7) or Sunday (1) in Spark's dayofweek convention
    .withColumn("weekend_flag", F.dayofweek(F.col("full_date")).isin(1, 7))
    .drop("id")
)

# MERGE: insert new dates, skip existing ones (date_key is the natural key)
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.gold.dim_date (
        date_key     INT       COMMENT 'YYYYMMDD integer key (e.g. 20101201)',
        full_date    DATE      COMMENT 'Calendar date',
        day          INT       COMMENT 'Day of month (1-31)',
        month        INT       COMMENT 'Month number (1-12)',
        month_name   STRING    COMMENT 'Month name (e.g. January)',
        quarter      INT       COMMENT 'Quarter (1-4)',
        year         INT       COMMENT 'Calendar year',
        week_of_year INT       COMMENT 'ISO week number',
        weekend_flag BOOLEAN   COMMENT 'True if Saturday or Sunday'
    )
    USING DELTA
    COMMENT 'Date dimension. Generated from the order_date range in silver.transactions.'
""")

date_spine.createOrReplaceTempView("new_dates")

spark.sql("""
    MERGE INTO workspace.gold.dim_date AS target
    USING new_dates AS source
    ON target.date_key = source.date_key
    WHEN NOT MATCHED THEN INSERT *
""")

dim_date_count = spark.sql("SELECT COUNT(*) AS cnt FROM workspace.gold.dim_date").collect()[0]["cnt"]
print(f"dim_date rows   : {dim_date_count:,}")

# COMMAND ----------

# =============================================================================
# SECTION 3: dim_country — Country with Region Mapping
# =============================================================================
# Distinct countries from Silver with a derived region.
# Surrogate key: xxhash64(country_name) — deterministic, reproducible.
#
# WHY xxhash64() INSTEAD OF monotonically_increasing_id()?
#   monotonically_increasing_id() produces non-deterministic values across
#   Spark runs — the same country may get a different key on a re-run.
#   This breaks MERGE operations because the same natural key would produce
#   a different surrogate key, causing duplicate rows instead of updates.
#   xxhash64() always produces the same output for the same input.

print("\n--- Building dim_country ---")

# Region mapping using PySpark when().when().otherwise() chain
# Countries not in the explicit list fall through to "Other"
def assign_region(country_col):
    europe = [
        "UNITED KINGDOM", "GERMANY", "FRANCE", "NETHERLANDS", "SPAIN",
        "BELGIUM", "PORTUGAL", "SWEDEN", "NORWAY", "DENMARK", "FINLAND",
        "AUSTRIA", "SWITZERLAND", "ITALY", "POLAND", "CZECH REPUBLIC",
        "GREECE", "CYPRUS", "MALTA", "ICELAND", "LITHUANIA", "LATVIA", "ESTONIA",
        "EIRE", "CHANNEL ISLANDS", "EUROPEAN COMMUNITY"
    ]
    north_america = ["USA", "CANADA"]
    asia_pacific  = ["AUSTRALIA", "JAPAN", "SINGAPORE", "HONG KONG", "INDIA", "BAHRAIN"]
    middle_east   = ["SAUDI ARABIA", "UNITED ARAB EMIRATES", "ISRAEL", "LEBANON"]

    return (
        F.when(country_col.isin(europe),        F.lit("Europe"))
         .when(country_col.isin(north_america),  F.lit("North America"))
         .when(country_col.isin(asia_pacific),   F.lit("Asia-Pacific"))
         .when(country_col.isin(middle_east),    F.lit("Middle East"))
         .otherwise(F.lit("Other"))
    )

new_countries = (
    silver
    .select(F.col("country").alias("country_name"))
    .distinct()
    .filter(F.col("country_name").isNotNull())
    .withColumn("country_key", F.xxhash64(F.col("country_name")))
    .withColumn("region", assign_region(F.col("country_name")))
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.gold.dim_country (
        country_key  BIGINT  COMMENT 'Deterministic hash surrogate: xxhash64(country_name)',
        country_name STRING  COMMENT 'Country name (upper-cased)',
        region       STRING  COMMENT 'Derived region: Europe, North America, Asia-Pacific, Middle East, Other'
    )
    USING DELTA
    COMMENT 'Country dimension with derived region mapping.'
""")

new_countries.createOrReplaceTempView("new_countries")

spark.sql("""
    MERGE INTO workspace.gold.dim_country AS target
    USING new_countries AS source
    ON target.country_name = source.country_name
    WHEN MATCHED AND target.region != source.region
        THEN UPDATE SET target.region = source.region
    WHEN NOT MATCHED THEN INSERT *
""")

dim_country_count = spark.sql("SELECT COUNT(*) AS cnt FROM workspace.gold.dim_country").collect()[0]["cnt"]
print(f"dim_country rows: {dim_country_count:,}")

# COMMAND ----------

# =============================================================================
# SECTION 4: dim_product — Products with Keyword-Derived Category
# =============================================================================
# Distinct products from Silver. Description is the most recent non-null value
# per stock_code. Category is derived from description keywords.
# Surrogate key: xxhash64(stock_code)

print("\n--- Building dim_product ---")

# Get most recent non-null description per stock_code
# Window: partition by stock_code, order by _silver_updated_at DESC
# Keep rn=1 (most recent row with a non-null description)
desc_window = Window.partitionBy("stock_code").orderBy(F.col("_silver_updated_at").desc())

product_base = (
    silver
    .filter(F.col("description").isNotNull())
    .withColumn("_rn", F.row_number().over(desc_window))
    .filter(F.col("_rn") == 1)
    .select(
        F.col("stock_code"),
        F.col("description"),
    )
)

# Category derivation using keyword matching on upper-cased description.
# First match wins — order matters.
# Keywords are checked against the upper-cased description for case-insensitive matching.
def assign_category(desc_col):
    desc_upper = F.upper(desc_col)
    return (
        F.when(desc_upper.rlike("CANDLE|LIGHT|LANTERN|LAMP"),          F.lit("Lighting"))
         .when(desc_upper.rlike("CHRISTMAS|XMAS|ADVENT"),              F.lit("Seasonal"))
         .when(desc_upper.rlike("HEART|LOVE|ROSE|VALENTINE"),          F.lit("Gifts"))
         .when(desc_upper.rlike("BAG|TOTE|PURSE|POUCH"),               F.lit("Bags"))
         .when(desc_upper.rlike("FRAME|PHOTO|PICTURE|MIRROR"),         F.lit("Home Decor"))
         .when(desc_upper.rlike("MUG|CUP|PLATE|BOWL|KITCHEN"),         F.lit("Kitchen"))
         .when(desc_upper.rlike("GARDEN|PLANT|FLOWER|BIRD"),           F.lit("Garden"))
         .when(desc_upper.rlike("TOY|GAME|DOLL|CHILDREN|KIDS"),        F.lit("Toys"))
         .otherwise(F.lit("General"))
    )

new_products = (
    product_base
    .withColumn("product_key", F.xxhash64(F.col("stock_code")))
    .withColumn("category", assign_category(F.col("description")))
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.gold.dim_product (
        product_key  BIGINT  COMMENT 'Deterministic hash surrogate: xxhash64(stock_code)',
        stock_code   STRING  COMMENT 'Product code (natural key)',
        description  STRING  COMMENT 'Most recent non-null product description',
        category     STRING  COMMENT 'Keyword-derived category (Lighting, Seasonal, Gifts, Bags, Home Decor, Kitchen, Garden, Toys, General)'
    )
    USING DELTA
    COMMENT 'Product dimension with keyword-derived category.'
""")

new_products.createOrReplaceTempView("new_products")

spark.sql("""
    MERGE INTO workspace.gold.dim_product AS target
    USING new_products AS source
    ON target.stock_code = source.stock_code
    WHEN MATCHED AND (
        target.description != source.description OR
        target.category != source.category
    )
        THEN UPDATE SET
            target.description = source.description,
            target.category    = source.category
    WHEN NOT MATCHED THEN INSERT *
""")

dim_product_count = spark.sql("SELECT COUNT(*) AS cnt FROM workspace.gold.dim_product").collect()[0]["cnt"]
print(f"dim_product rows: {dim_product_count:,}")

# COMMAND ----------

# =============================================================================
# SECTION 5: dim_customer — SCD Type 2 on Country Change
# =============================================================================
# SCD Type 2 tracks historical changes by adding new rows rather than
# overwriting. Each version has effective_date, expiry_date, and current_flag.
#
# TRIGGER: a new version is created when a customer's country changes.
#
# SURROGATE KEY: xxhash64(customer_id, effective_date)
#   Including effective_date in the hash ensures each version gets a unique key.
#
# CUSTOMER SEGMENT: derived from total lifetime revenue across ALL versions.
#   High Value: total revenue >= £1000
#   Mid Value:  total revenue >= £200
#   Low Value:  total revenue < £200
#
# SCD2 LOGIC:
#   1. Compute each customer's first invoice date per country
#   2. Order versions by first invoice date per customer
#   3. effective_date = first invoice date for that country
#   4. expiry_date = effective_date of the next version (NULL for current)
#   5. current_flag = True only for the latest version
#
# INCREMENTAL MERGE (two steps):
#   Step 1: Expire old versions — UPDATE current records where country changed
#   Step 2: Insert new versions — APPEND new version rows

print("\n--- Building dim_customer (SCD Type 2) ---")

# Step A: Compute first invoice date per customer per country
customer_country_versions = (
    silver
    .groupBy("customer_id", "country")
    .agg(F.min("order_date").alias("effective_date"))
)

# Step B: Order versions per customer by effective_date
# Use lead() to compute expiry_date = effective_date of the next version
version_window = Window.partitionBy("customer_id").orderBy("effective_date")

customer_versions = (
    customer_country_versions
    .withColumn(
        "expiry_date",
        F.lead("effective_date").over(version_window)
    )
    # current_flag = True when expiry_date is NULL (no next version exists)
    .withColumn("current_flag", F.col("expiry_date").isNull())
)

# Step C: Compute customer segment from total lifetime revenue
customer_revenue = (
    silver
    .groupBy("customer_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

customer_segment = (
    customer_revenue
    .withColumn(
        "customer_segment",
        F.when(F.col("total_revenue") >= 1000, F.lit("High Value"))
         .when(F.col("total_revenue") >= 200,  F.lit("Mid Value"))
         .otherwise(F.lit("Low Value"))
    )
    .select("customer_id", "customer_segment")
)

# Step D: Join versions with segment
all_versions = (
    customer_versions
    .join(customer_segment, on="customer_id", how="left")
    .withColumn(
        "customer_key",
        F.xxhash64(
            F.col("customer_id"),
            F.col("effective_date").cast(StringType())
        )
    )
    .select(
        "customer_key",
        "customer_id",
        "country",
        "customer_segment",
        "effective_date",
        "expiry_date",
        "current_flag",
    )
)

# Create dim_customer table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.gold.dim_customer (
        customer_key     BIGINT  COMMENT 'Deterministic hash surrogate: xxhash64(customer_id, effective_date)',
        customer_id      STRING  COMMENT 'Natural key — customer identifier',
        country          STRING  COMMENT 'Country for this version',
        customer_segment STRING  COMMENT 'High Value (>=£1000), Mid Value (>=£200), Low Value (<£200)',
        effective_date   DATE    COMMENT 'Date this version became active',
        expiry_date      DATE    COMMENT 'Date this version was superseded (NULL for current record)',
        current_flag     BOOLEAN COMMENT 'True for the current active version'
    )
    USING DELTA
    COMMENT 'Customer dimension with SCD Type 2 tracking country changes.'
""")

# SCD2 MERGE — Step 1: Expire old versions where country has changed
# Match on customer_id AND current_flag=True AND country differs from new data
# UPDATE: set expiry_date and current_flag=False on the old version
changed_customers = (
    all_versions
    .filter(F.col("current_flag") == False)  # noqa: E712 — these are the new versions
    .select(
        F.col("customer_id"),
        F.col("effective_date").alias("new_effective_date"),
        F.col("country").alias("new_country"),
    )
)

changed_customers.createOrReplaceTempView("changed_customers")

spark.sql("""
    MERGE INTO workspace.gold.dim_customer AS target
    USING changed_customers AS source
    ON target.customer_id = source.customer_id
       AND target.current_flag = True
       AND target.country != source.new_country
    WHEN MATCHED THEN UPDATE SET
        target.expiry_date   = source.new_effective_date,
        target.current_flag  = false
""")

# SCD2 MERGE — Step 2: Insert new versions (both new customers and new country versions)
# We insert all versions from all_versions that don't already exist in dim_customer
# (matched on customer_key — the deterministic hash of customer_id + effective_date)
all_versions.createOrReplaceTempView("all_customer_versions")

spark.sql("""
    MERGE INTO workspace.gold.dim_customer AS target
    USING all_customer_versions AS source
    ON target.customer_key = source.customer_key
    WHEN NOT MATCHED THEN INSERT *
""")

dim_customer_count = spark.sql("SELECT COUNT(*) AS cnt FROM workspace.gold.dim_customer").collect()[0]["cnt"]
current_count = spark.sql(
    "SELECT COUNT(*) AS cnt FROM workspace.gold.dim_customer WHERE current_flag = true"
).collect()[0]["cnt"]
print(f"dim_customer rows: {dim_customer_count:,} total, {current_count:,} current")

# COMMAND ----------

# =============================================================================
# SECTION 6: fact_sales — Line-Item Fact Table
# =============================================================================
# GRAIN: one row per invoice line item (InvoiceNo + StockCode).
#
# This is the most important design decision for a fact table. At line-item
# grain you can aggregate to:
#   - Order level:    GROUP BY invoice_no
#   - Customer level: GROUP BY customer_key
#   - Product level:  GROUP BY product_key
#   - Date level:     GROUP BY date_key
#   - Any combination of the above
#
# SURROGATE KEY: xxhash64(invoice_no, stock_code, invoice_date, customer_id)
#   Four columns are used (not two) for collision safety. The same StockCode
#   can appear on the same InvoiceNo with different customers or dates due to
#   data quality issues in the source. Four columns make the key truly unique
#   per transaction event.
#
# TEMPORAL JOIN FOR customer_key (SCD2):
#   We join to dim_customer using:
#     order_date >= effective_date
#     AND (order_date < expiry_date OR expiry_date IS NULL)
#
#   WHY NOT BETWEEN?
#   BETWEEN is inclusive on both ends. When expiry_date IS NULL (the current
#   record), BETWEEN fails unless you use a sentinel date like '9999-12-31'.
#   The explicit predicate handles both open-ended current versions and closed
#   historical versions correctly without sentinel values.

print("\n--- Building fact_sales ---")

# Load dimension tables for surrogate key resolution
dim_customer = spark.read.format("delta").table("workspace.gold.dim_customer")
dim_product  = spark.read.format("delta").table("workspace.gold.dim_product")
dim_country  = spark.read.format("delta").table("workspace.gold.dim_country")

# Resolve customer_key using SCD2 temporal join
# Join condition: customer_id matches AND order_date falls within the version's
# effective period (>= effective_date AND (< expiry_date OR expiry_date IS NULL))
facts_with_customer = (
    silver
    .join(
        dim_customer.select("customer_key", "customer_id", "effective_date", "expiry_date"),
        on=(
            (silver["customer_id"] == dim_customer["customer_id"]) &
            (silver["order_date"] >= dim_customer["effective_date"]) &
            (
                (silver["order_date"] < dim_customer["expiry_date"]) |
                dim_customer["expiry_date"].isNull()
            )
        ),
        how="left"
    )
    .drop(dim_customer["customer_id"])
    .drop("effective_date", "expiry_date")
)

# Resolve product_key
facts_with_product = (
    facts_with_customer
    .join(
        dim_product.select("product_key", "stock_code"),
        on="stock_code",
        how="left"
    )
)

# Resolve country_key
facts_with_country = (
    facts_with_product
    .join(
        dim_country.select("country_key", "country_name"),
        on=(facts_with_product["country"] == dim_country["country_name"]),
        how="left"
    )
    .drop("country_name")
)

# Build fact_sales with all surrogate keys and partition columns
fact_sales = (
    facts_with_country
    .withColumn(
        "sale_key",
        F.xxhash64(
            F.col("invoice_no"),
            F.col("stock_code"),
            F.col("invoice_date").cast(StringType()),
            F.col("customer_id"),
        )
    )
    .withColumn("date_key", F.date_format(F.col("order_date"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("order_count", F.lit(1).cast(IntegerType()))
    # Partition columns: derived from date_key for efficient time-range pruning
    .withColumn("year",  (F.col("date_key") / 10000).cast(IntegerType()))
    .withColumn("month", ((F.col("date_key") % 10000) / 100).cast(IntegerType()))
    .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
    .select(
        "sale_key",
        "invoice_no",
        "customer_key",
        "product_key",
        "date_key",
        "country_key",
        "quantity",
        "unit_price",
        "revenue",
        "order_count",
        "year",
        "month",
        "_pipeline_run_id",
    )
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.gold.fact_sales (
        sale_key        BIGINT        COMMENT 'Deterministic hash surrogate: xxhash64(invoice_no, stock_code, invoice_date, customer_id)',
        invoice_no      STRING        COMMENT 'Invoice number',
        customer_key    BIGINT        COMMENT 'FK → dim_customer (SCD2 version active at invoice date)',
        product_key     BIGINT        COMMENT 'FK → dim_product',
        date_key        INT           COMMENT 'FK → dim_date (YYYYMMDD)',
        country_key     BIGINT        COMMENT 'FK → dim_country',
        quantity        INT           COMMENT 'Units sold',
        unit_price      DECIMAL(10,2) COMMENT 'Price per unit in GBP',
        revenue         DECIMAL(12,2) COMMENT 'quantity × unit_price',
        order_count     INT           COMMENT 'Always 1 at line-item grain; SUM for order-level aggregation',
        year            INT           COMMENT 'Partition column: derived from date_key',
        month           INT           COMMENT 'Partition column: derived from date_key',
        _pipeline_run_id STRING       COMMENT 'Correlation ID'
    )
    USING DELTA
    PARTITIONED BY (year, month)
    COMMENT 'Sales fact table. Grain: one row per invoice line item (InvoiceNo + StockCode). Partitioned by year/month.'
""")

# MERGE on sale_key: effectively append-only since sale_key is deterministic.
# WHEN NOT MATCHED THEN INSERT * prevents duplicate insertion on Bronze replays.
fact_sales.createOrReplaceTempView("new_facts")

spark.sql("""
    MERGE INTO workspace.gold.fact_sales AS target
    USING new_facts AS source
    ON target.sale_key = source.sale_key
    WHEN NOT MATCHED THEN INSERT *
""")

fact_sales_count = spark.sql("SELECT COUNT(*) AS cnt FROM workspace.gold.fact_sales").collect()[0]["cnt"]
print(f"fact_sales rows : {fact_sales_count:,}")

# COMMAND ----------

# =============================================================================
# SECTION 7: Delta Optimisation
# =============================================================================
# OPTIMIZE + ZORDER co-locates data for the most common join patterns,
# reducing the number of files Spark reads for analytical queries.
#
# ZORDER BY (customer_key, product_key):
#   The most common queries on fact_sales join to dim_customer and dim_product.
#   ZORDER ensures rows with the same customer_key and product_key are stored
#   in the same data files, enabling data skipping.
#
# VACUUM RETAIN 168 HOURS:
#   Removes Delta files older than 7 days. Without VACUUM, deleted/updated
#   files accumulate indefinitely. 7 days retains enough history for debugging.
#
# FREE EDITION CAVEAT:
#   OPTIMIZE/ZORDER support may vary in Free Edition serverless environments.
#   The pipeline degrades gracefully if these commands fail — data is correct,
#   queries may just be slower due to suboptimal file layout.
#   Run OPTIMIZE manually from a SQL warehouse if needed.

print("\n--- Delta Optimisation ---")

try:
    spark.sql("OPTIMIZE workspace.gold.fact_sales ZORDER BY (customer_key, product_key)")
    print("✅ OPTIMIZE ZORDER complete.")

    spark.sql("VACUUM workspace.gold.fact_sales RETAIN 168 HOURS")
    print("✅ VACUUM complete.")

    history_df = spark.sql("DESCRIBE HISTORY workspace.gold.fact_sales LIMIT 5")
    history_df.show(truncate=False)

except Exception as e:
    print(f"⚠️  Delta optimisation skipped: {e}")
    print("    OPTIMIZE/ZORDER support may vary in Free Edition serverless environments.")
    print("    The pipeline degrades gracefully — data is correct, queries may be slower.")
    print("    Run OPTIMIZE manually from a SQL warehouse if needed.")

# COMMAND ----------

# =============================================================================
# SECTION 8: Audit Log Write
# =============================================================================

duration_seconds = time.time() - run_start_time

print(f"\n{'='*60}")
print("GOLD BUILD SUMMARY")
print(f"{'='*60}")
print(f"dim_date rows    : {dim_date_count:,}")
print(f"dim_country rows : {dim_country_count:,}")
print(f"dim_product rows : {dim_product_count:,}")
print(f"dim_customer rows: {dim_customer_count:,} ({current_count:,} current)")
print(f"fact_sales rows  : {fact_sales_count:,}")
print(f"Duration         : {duration_seconds:.1f}s")
print(f"{'='*60}")

audit_row = spark.createDataFrame(
    [(
        pipeline_run_id,
        "GOLD_COMPLETE",
        int(fact_sales_count),
        None,
        ["dim_date", "dim_country", "dim_product", "dim_customer", "fact_sales"],
        duration_seconds,
        "SUCCESS",
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

print(f"✅ Audit log written: GOLD_COMPLETE, fact_sales={fact_sales_count:,}, duration={duration_seconds:.1f}s")

# Unpersist cached Silver DataFrame
silver.unpersist()
