# Databricks notebook source
# =============================================================================
# 00_setup_catalog.py — One-Time Unity Catalog Setup
# =============================================================================
# PURPOSE:
#   Creates the schemas and Unity Catalog Volumes needed by the Online Retail
#   Pipeline. Run this notebook ONCE before triggering the pipeline workflow
#   for the first time.
#
# UNITY CATALOG ON FREE EDITION:
#   Databricks Free Edition automatically provisions a catalog named `workspace`.
#   You do NOT need to create this catalog manually — it already exists.
#   This notebook only creates schemas and volumes within that catalog.
#
# THREE-LEVEL NAMESPACE:
#   All objects in this pipeline use the pattern: catalog.schema.table
#   e.g. workspace.bronze.raw_online_retail
#        workspace.silver.transactions
#        workspace.gold.fact_sales
#
# HOW TO RUN:
#   1. Upload this notebook to your Databricks workspace
#   2. Attach it to any serverless compute or SQL warehouse
#   3. Run All cells
#   4. Verify in Catalog Explorer: Catalog > workspace > bronze/silver/gold
# =============================================================================

# COMMAND ----------

# =============================================================================
# SECTION 1: Create Medallion Schemas
# =============================================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze schema: raw ingestion layer
# MAGIC -- Stores data exactly as received from the source CSV, with no transformations.
# MAGIC -- Tables: raw_online_retail, quarantine, pipeline_audit_log
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver schema: cleansed and conformed layer
# MAGIC -- PySpark transformations: null handling, cancellation removal, type casting,
# MAGIC -- deduplication, derived columns (Revenue, OrderDate), quarantine isolation.
# MAGIC -- Tables: transactions, quarantine_transactions, quality_failures
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold schema: dimensional model (star schema)
# MAGIC -- PySpark builds: fact_sales, dim_customer (SCD2), dim_product, dim_date, dim_country
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

# =============================================================================
# SECTION 2: Add Schema Comments (Data Discovery)
# =============================================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON SCHEMA workspace.bronze IS
# MAGIC   'Bronze layer — raw ingestion. All 8 Online Retail source columns stored as STRING. Includes quarantine and audit log tables.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON SCHEMA workspace.silver IS
# MAGIC   'Silver layer — cleansed and conformed. PySpark transformations: null handling, cancellation removal, type casting, deduplication, Revenue and OrderDate derived. Quarantine table isolates malformed rows.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON SCHEMA workspace.gold IS
# MAGIC   'Gold layer — star schema dimensional model. fact_sales (grain: one row per invoice line item), dim_customer (SCD Type 2 on country change), dim_product (keyword-derived category), dim_date (date spine), dim_country (region mapping).';

# COMMAND ----------

# =============================================================================
# SECTION 3: Create Unity Catalog Volumes
# =============================================================================
# WHAT ARE VOLUMES?
#   Unity Catalog Volumes are managed storage locations for non-tabular files
#   (CSVs, JSON, etc.). They are accessed via /Volumes/<catalog>/<schema>/<volume>/
#   paths in notebooks and Auto Loader.
#
# WHY VOLUMES INSTEAD OF DBFS?
#   On Databricks Free Edition with Unity Catalog enabled, DBFS paths (/dbfs/...)
#   are not supported for Auto Loader cloudFiles.schemaLocation or checkpointLocation.
#   Volume paths (/Volumes/...) are the correct approach and are governed by
#   Unity Catalog access controls.
#
# VOLUMES CREATED:
#   - workspace.bronze.landing              → drop zone for online_retail.csv
#   - workspace.bronze.autoloader_checkpoint → Auto Loader tracks processed files here
#   - workspace.bronze.autoloader_schema    → Auto Loader infers and stores schema here

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Landing zone: place online_retail.csv here before running the pipeline
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.landing;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Auto Loader checkpoint: records which files have already been processed.
# MAGIC -- Deleting this directory forces a full replay of all source files.
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.autoloader_checkpoint;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Auto Loader schema location: stores the inferred schema for the CSV source.
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.autoloader_schema;

# COMMAND ----------

# =============================================================================
# SECTION 4: Upload the Dataset
# =============================================================================
# The Online Retail dataset is available from the UCI Machine Learning Repository:
#   https://archive.ics.uci.edu/dataset/352/online+retail
#
# The original file is an XLSX. Convert it to CSV before uploading:
#   python -c "import pandas as pd; pd.read_excel('Online Retail.xlsx').to_csv('online_retail.csv', index=False)"
#
# Then upload to the landing Volume:
#
# OPTION 1 — Databricks CLI (recommended):
#   databricks fs cp online_retail.csv /Volumes/workspace/bronze/landing/online_retail.csv
#
# OPTION 2 — Databricks UI:
#   1. Go to Catalog > workspace > bronze > landing
#   2. Click "Upload to this volume"
#   3. Select online_retail.csv from your local machine
#
# VERIFY the upload:

# COMMAND ----------

landing_path = "/Volumes/workspace/bronze/landing/"
try:
    files = dbutils.fs.ls(landing_path)
    if not files:
        print("⚠️  No files found in the landing Volume.")
        print(f"   Upload online_retail.csv to: {landing_path}")
    else:
        print(f"✅ Files found in {landing_path}:")
        for f in files:
            print(f"   {f.name}  ({f.size:,} bytes)")
except Exception as e:
    print(f"⚠️  Could not list landing Volume: {e}")

# COMMAND ----------

# =============================================================================
# SECTION 5: Verify Setup
# =============================================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.bronze;

# COMMAND ----------

print("✅ Setup complete.")
print()
print("Next steps:")
print("  1. Upload online_retail.csv to /Volumes/workspace/bronze/landing/")
print("  2. Set your warehouse_id in databricks.yml")
print("  3. Run: databricks bundle deploy --target dev")
print("  4. Run: databricks bundle run online_retail_pipeline_workflow --target dev")
