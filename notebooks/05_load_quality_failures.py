# Databricks notebook source
# =============================================================================
# 05_load_quality_failures.py — Load dbt Test Failures into Silver
# =============================================================================
# PURPOSE:
#   Reads dbt/target/run_results.json written by the dbt_test task, parses
#   any test failures, and appends them to workspace.silver.quality_failures.
#   Implements severity-based pipeline control:
#     - severity: error → raises an exception (halts the pipeline)
#     - severity: warn  → logs a warning but does NOT raise (pipeline continues)
#
# WHY A SEPARATE NOTEBOOK TASK INSTEAD OF A dbt POST-HOOK?
#   dbt post-hooks run per model during `dbt run`. In this pipeline there are
#   no dbt models — only `dbt test`. Even with on-run-end hooks, they run
#   within the dbt process and cannot easily write structured rows to a Delta
#   table with the full test result context. run_results.json is only written
#   after the entire `dbt test` command completes. A dedicated Workflow notebook
#   task runs after `dbt test` finishes, has full access to run_results.json,
#   and can use the Spark session to write structured Delta rows.
# =============================================================================

# COMMAND ----------

import json
import os
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

# COMMAND ----------

pipeline_run_id = dbutils.jobs.taskValues.get(
    taskKey="ingest_bronze",
    key="pipeline_run_id",
    debugValue=str(uuid.uuid4()),
)

print(f"pipeline_run_id : {pipeline_run_id}")

# COMMAND ----------

# Read dbt run_results.json
# The dbt_test task writes this file to dbt/target/ after `dbt test` completes.
run_results_path = os.path.join(os.getcwd(), "dbt", "target", "run_results.json")

run_results = None
try:
    with open(run_results_path, "r") as f:
        run_results = json.load(f)
    print(f"✅ Loaded run_results.json from: {run_results_path}")
except FileNotFoundError:
    print(
        f"⚠️  run_results.json not found at: {run_results_path}\n"
        "    This is expected if dbt test was skipped or the path differs.\n"
        "    No quality_failures rows will be written for this run."
    )
except Exception as e:
    print(f"⚠️  Could not read run_results.json: {e}")

# COMMAND ----------

# Parse test failures and write to quality_failures
error_failures = []
warn_failures  = []

if run_results is not None:
    results = run_results.get("results", [])
    failing_results = [r for r in results if r.get("status") == "fail"]

    print(f"Total test results : {len(results)}")
    print(f"Failing tests      : {len(failing_results)}")

    failure_rows = []

    for result in failing_results:
        unique_id = result.get("unique_id", "")
        node      = result.get("node", {})
        config    = node.get("config", {})

        check_name = node.get("name") or unique_id.split(".")[-1]

        attached_node = node.get("attached_node", "")
        if attached_node:
            model_name = attached_node.split(".")[-1]
        else:
            model_name = unique_id.split(".")[2] if len(unique_id.split(".")) > 2 else "unknown"

        severity = config.get("severity", "ERROR").lower()
        failing_row_count = int(result.get("failures") or 0)
        sample_failing_values = json.dumps(result.get("message", ""))

        failure_rows.append((
            pipeline_run_id,
            check_name,
            model_name,
            severity,
            failing_row_count,
            sample_failing_values,
        ))

        if severity == "error":
            error_failures.append(check_name)
        else:
            warn_failures.append(check_name)

    if failure_rows:
        schema = StructType([
            StructField("pipeline_run_id",      StringType(), True),
            StructField("check_name",            StringType(), True),
            StructField("model_name",            StringType(), True),
            StructField("severity",              StringType(), True),
            StructField("failing_row_count",     LongType(),   True),
            StructField("sample_failing_values", StringType(), True),
        ])

        failures_df = (
            spark.createDataFrame(failure_rows, schema=schema)
            .withColumn("recorded_at", F.current_timestamp())
        )

        (
            failures_df.write
            .format("delta")
            .mode("append")
            .saveAsTable("workspace.silver.quality_failures")
        )

        print(f"✅ Wrote {len(failure_rows)} failure row(s) to workspace.silver.quality_failures.")
    else:
        print("✅ No failing tests — nothing to write to quality_failures.")

# COMMAND ----------

# Severity-based pipeline control
if warn_failures:
    print(
        f"⚠️  WARN-level test failures (pipeline continues):\n"
        + "\n".join(f"    - {name}" for name in warn_failures)
    )

if error_failures:
    error_list = "\n".join(f"  - {name}" for name in error_failures)
    raise Exception(
        f"❌ ERROR-level dbt test failures — pipeline halted.\n"
        f"Failing tests:\n{error_list}\n\n"
        f"Inspect workspace.silver.quality_failures for details.\n"
        f"pipeline_run_id: {pipeline_run_id}"
    )

print("✅ Quality failure check complete — no error-level failures.")
