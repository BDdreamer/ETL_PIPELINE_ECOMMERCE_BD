# Databricks notebook source
# =============================================================================
# 01_ingest_bronze.py — Bronze Ingestion via Auto Loader
# =============================================================================
# PURPOSE:
#   Reads the Online Retail CSV from the Unity Catalog landing Volume and
#   writes it to workspace.bronze.raw_online_retail using Spark Structured
#   Streaming with the Auto Loader (cloudFiles) source.
#
# DATASET:
#   UCI Online Retail dataset — ~541,000 UK e-commerce transactions (2010–2011)
#   Columns: InvoiceNo, StockCode, Description, Quantity, InvoiceDate,
#            UnitPrice, CustomerID, Country
#
# WHAT IS AUTO LOADER (cloudFiles)?
#   Auto Loader is Databricks' incremental file ingestion framework built on
#   Spark Structured Streaming. Instead of scanning the entire source directory
#   on every run, it uses a checkpoint to track which files have already been
#   processed. Only new files are read on each execution.
#
# HOW CHECKPOINT-BASED DEDUPLICATION WORKS:
#   Auto Loader writes a checkpoint after each micro-batch recording the file
#   offsets (names, sizes, modification times) that were successfully committed.
#   On the next run, it reads the checkpoint and skips already-processed files.
#   This gives exactly-once ingestion semantics — even if the job is re-run
#   multiple times, each source file is written to Bronze exactly once.
#
# WHY AUTO LOADER INSTEAD OF spark.read.csv?
#   spark.read.csv is a batch read — it scans ALL files every time and has no
#   memory of what was previously processed. Re-running duplicates every row.
#   Auto Loader solves this natively via its checkpoint mechanism.
#
# WHY VOLUME PATHS INSTEAD OF DBFS?
#   On Databricks Free Edition with Unity Catalog enabled, DBFS paths
#   (/dbfs/... or dbfs:/...) are not supported for cloudFiles.schemaLocation
#   or checkpointLocation. Volume paths (/Volumes/...) are required.
# =============================================================================

# COMMAND ----------

import uuid
import time
from datetime import datetime
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# pipeline_run_id widget
# ---------------------------------------------------------------------------
# In a Databricks Workflow, the orchestrator passes pipeline_run_id as a
# notebook widget parameter. When running interactively, we fall back to a
# freshly generated UUID.
dbutils.widgets.text("pipeline_run_id", str(uuid.uuid4()))
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"Run started at  : {datetime.utcnow().isoformat()}Z")

run_start_time = time.time()

# COMMAND ----------

# =============================================================================
# SECTION 1: Create Bronze Tables (DDL)
# =============================================================================
# All 8 source columns are defined as STRING to preserve raw fidelity.
# Type casting happens in the Silver PySpark notebook.
# Three metadata columns are appended by Auto Loader.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Primary Bronze table: one row per source CSV row, all columns as STRING.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.bronze.raw_online_retail (
# MAGIC   InvoiceNo           STRING  COMMENT 'Invoice number; starts with C for cancellations',
# MAGIC   StockCode           STRING  COMMENT 'Product/item code',
# MAGIC   Description         STRING  COMMENT 'Product description; may be NULL',
# MAGIC   Quantity            STRING  COMMENT 'Units purchased (raw string — may be negative for returns)',
# MAGIC   InvoiceDate         STRING  COMMENT 'Transaction date/time (raw string, e.g. 12/1/2010 8:26)',
# MAGIC   UnitPrice           STRING  COMMENT 'Price per unit in GBP (raw string)',
# MAGIC   CustomerID          STRING  COMMENT 'Customer identifier; may be NULL for guest transactions',
# MAGIC   Country             STRING  COMMENT 'Country of the customer',
# MAGIC   _source_file_name   STRING  COMMENT 'Full path of the source file from _metadata.file_path',
# MAGIC   _ingestion_timestamp TIMESTAMP COMMENT 'current_timestamp() at the time of the write micro-batch',
# MAGIC   _pipeline_run_id    STRING  COMMENT 'Correlation ID shared across all tasks in this job run'
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC )
# MAGIC COMMENT 'Bronze raw ingestion table. All source columns stored as STRING. Three metadata columns appended by Auto Loader.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quarantine table: rows that Auto Loader could not parse are written here.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.bronze.quarantine (
# MAGIC   _raw_data             STRING    COMMENT 'Raw row content that could not be parsed',
# MAGIC   _error_message        STRING    COMMENT 'Parse or schema error description',
# MAGIC   _source_file_name     STRING    COMMENT 'Source file path',
# MAGIC   _quarantine_timestamp TIMESTAMP COMMENT 'When the row was quarantined',
# MAGIC   _pipeline_run_id      STRING    COMMENT 'Correlated run ID'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Bronze quarantine: rows that failed Auto Loader schema validation or parsing.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Audit log: one row per pipeline event (INGESTION_COMPLETE, SILVER_COMPLETE,
# MAGIC -- GOLD_COMPLETE, PIPELINE_COMPLETE). Replaces Premium-only system tables for
# MAGIC -- run observability on Free Edition.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.bronze.pipeline_audit_log (
# MAGIC   pipeline_run_id  STRING         COMMENT 'Unique run identifier shared across all tasks',
# MAGIC   event_type       STRING         COMMENT 'INGESTION_COMPLETE | SILVER_COMPLETE | GOLD_COMPLETE | PIPELINE_COMPLETE',
# MAGIC   rows_processed   BIGINT         COMMENT 'Rows ingested or transformed in this event',
# MAGIC   source_file_name STRING         COMMENT 'Source file path (ingestion events only)',
# MAGIC   models_refreshed ARRAY<STRING>  COMMENT 'Table names refreshed (transformation events only)',
# MAGIC   duration_seconds DOUBLE         COMMENT 'Wall-clock duration of the task in seconds',
# MAGIC   status           STRING         COMMENT 'SUCCESS or FAILURE',
# MAGIC   logged_at        TIMESTAMP      COMMENT 'Timestamp when this audit row was written'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Pipeline audit log. Replaces Premium-only system tables for run observability on Free Edition.';

# COMMAND ----------

# =============================================================================
# SECTION 2: Auto Loader Read Stream
# =============================================================================
# KEY OPTIONS:
#   cloudFiles.format        — underlying file format (csv)
#   cloudFiles.schemaLocation — where Auto Loader stores the inferred schema.
#                               Must be a Volume path on Free Edition.
#   header = true            — use the CSV header row as column names
#   inferSchema = false      — read all columns as STRING (Bronze raw fidelity)
#   badRecordsPath           — malformed rows written here instead of failing stream
#
# METADATA COLUMNS:
#   _source_file_name    — full path of the file being ingested (_metadata.file_path)
#   _ingestion_timestamp — current_timestamp() at write time
#   _pipeline_run_id     — correlation ID from the widget

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    # Schema location: MUST be a Volume path on Free Edition — DBFS not supported
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/autoloader_schema/")
    .option("header", "true")
    # inferSchema=false: read every column as STRING (Bronze raw fidelity).
    # Type casting happens in the Silver PySpark notebook.
    .option("inferSchema", "false")
    # badRecordsPath: malformed rows written here as JSON instead of crashing the stream
    .option("badRecordsPath", "/Volumes/workspace/bronze/autoloader_schema/bad_records/")
    # Source path: the Unity Catalog Volume where online_retail.csv was uploaded
    .load("/Volumes/workspace/bronze/landing/")
    # _metadata.file_path is a hidden Spark metadata column available on all file sources
    .withColumn("_source_file_name", F.col("_metadata.file_path"))
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
)

print("✅ Auto Loader read stream defined.")

# COMMAND ----------

# =============================================================================
# SECTION 3: Write Stream to workspace.bronze.raw_online_retail
# =============================================================================
# trigger(availableNow=True):
#   Processes all files that have arrived since the last checkpoint, then stops.
#   Behaves like a batch job but uses streaming checkpoint for deduplication.
#   This is the recommended pattern for scheduled pipeline jobs.
#
# checkpointLocation:
#   Records which files have been processed. Deleting this forces a full replay.
#   MUST be a Volume path on Free Edition.
#
# mergeSchema=false:
#   Prevents Auto Loader from silently adding new columns if the CSV gains new fields.
#   Schema changes must be handled explicitly.

status = "SUCCESS"
rows_written = 0
stream_error = None

try:
    query = (
        bronze_stream.writeStream
        .format("delta")
        # trigger(availableNow=True): process all pending files, then stop
        .trigger(availableNow=True)
        # Checkpoint: records processed file offsets — prevents re-ingestion
        .option("checkpointLocation", "/Volumes/workspace/bronze/autoloader_checkpoint/")
        .option("mergeSchema", "false")
        .toTable("workspace.bronze.raw_online_retail")
    )

    # awaitTermination() blocks until all available files are processed and the stream stops
    query.awaitTermination()

    print("✅ Stream completed successfully.")

except Exception as e:
    status = "FAILURE"
    stream_error = e
    print(f"❌ Stream failed: {e}")

# COMMAND ----------

# =============================================================================
# SECTION 4: Post-Ingestion Row Count and task_values
# =============================================================================

rows_written = spark.sql(
    "SELECT COUNT(*) AS cnt FROM workspace.bronze.raw_online_retail"
).collect()[0]["cnt"]

print(f"Rows in workspace.bronze.raw_online_retail : {rows_written:,}")

# ---------------------------------------------------------------------------
# task_values API
# ---------------------------------------------------------------------------
# dbutils.jobs.taskValues.set() stores a key-value pair that downstream tasks
# in the same Databricks Workflow job run can read via:
#   dbutils.jobs.taskValues.get(taskKey="ingest_bronze", key="pipeline_run_id")
#
# This is how pipeline_run_id flows from the ingestion notebook to all
# downstream tasks (transform_silver, build_gold, audit_log).
# task_values are scoped to a single job run — not persisted across runs.
dbutils.jobs.taskValues.set(key="pipeline_run_id", value=pipeline_run_id)

print(f"task_value set  : pipeline_run_id = {pipeline_run_id}")

# COMMAND ----------

# =============================================================================
# SECTION 5: Audit Log Write
# =============================================================================
# Write INGESTION_COMPLETE event to the audit log.
# This runs whether the stream succeeded or failed (try/except above captures
# the error and sets status="FAILURE"). Every run has a traceable audit record.

duration_seconds = time.time() - run_start_time

audit_row = spark.createDataFrame(
    [(
        pipeline_run_id,
        "INGESTION_COMPLETE",
        int(rows_written),
        "/Volumes/workspace/bronze/landing/online_retail.csv",
        None,           # models_refreshed is NULL for ingestion events
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

print(f"✅ Audit log written: status={status}, rows_processed={rows_written:,}, duration={duration_seconds:.1f}s")

# COMMAND ----------

# =============================================================================
# SECTION 6: Re-raise on Failure
# =============================================================================
# Re-raise the stream exception now that the audit log has been written.
# This causes the Databricks Workflow task to be marked as FAILED, which
# prevents downstream tasks (transform_silver, build_gold) from running.

if stream_error is not None:
    raise stream_error
