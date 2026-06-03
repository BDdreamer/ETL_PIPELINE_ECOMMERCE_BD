# Databricks notebook source
# =============================================================================
# 04_audit_log.py — Pipeline Completion Audit Log
# =============================================================================
# PURPOSE:
#   Writes a PIPELINE_COMPLETE event to workspace.bronze.pipeline_audit_log
#   after all pipeline tasks have finished successfully. This is the final
#   task in the workflow.
#
# TASK_VALUES API:
#   Databricks Workflows provides a key-value store scoped to a single job run
#   called "task values". Any notebook task can write a value using:
#     dbutils.jobs.taskValues.set(key="my_key", value="my_value")
#   Any downstream task reads it using:
#     dbutils.jobs.taskValues.get(taskKey="<upstream_task_key>", key="my_key")
#
#   In this pipeline:
#     - notebooks/01_ingest_bronze.py sets pipeline_run_id
#     - This notebook reads it with taskKey="ingest_bronze"
#
#   The taskKey argument must match the task_key in resources/workflow.yml.
#   Task values are NOT persisted across job runs.
#   The debugValue argument provides a fallback for interactive runs.
# =============================================================================

# COMMAND ----------

import uuid
import time
from pyspark.sql import functions as F

notebook_start_time = time.time()

# COMMAND ----------

pipeline_run_id = dbutils.jobs.taskValues.get(
    taskKey="ingest_bronze",
    key="pipeline_run_id",
    debugValue=str(uuid.uuid4()),
)

print(f"pipeline_run_id : {pipeline_run_id}")

# COMMAND ----------

# Count total rows in the Bronze table as the rows_processed metric
rows_processed = spark.sql(
    "SELECT COUNT(*) AS cnt FROM workspace.bronze.raw_online_retail"
).collect()[0]["cnt"]

print(f"Total rows in workspace.bronze.raw_online_retail : {rows_processed:,}")

# COMMAND ----------

duration_seconds = time.time() - notebook_start_time

# List of all tables refreshed by this pipeline run
models_refreshed = [
    "transactions",
    "quarantine_transactions",
    "dim_date",
    "dim_country",
    "dim_product",
    "dim_customer",
    "fact_sales",
]

audit_row = spark.createDataFrame(
    [(
        pipeline_run_id,
        "PIPELINE_COMPLETE",
        int(rows_processed),
        None,
        models_refreshed,
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

print(
    f"✅ Audit log written: PIPELINE_COMPLETE, "
    f"rows={rows_processed:,}, "
    f"models={models_refreshed}, "
    f"duration={duration_seconds:.1f}s"
)
