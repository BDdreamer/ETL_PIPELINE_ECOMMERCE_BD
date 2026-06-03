# Learning Plan — Online Retail Pipeline

This guide walks you through the key concepts behind this pipeline in the order you'll encounter them while building it. Each checkpoint has a concept to study, a hands-on exercise, and a clear definition of done.

Work through these checkpoints **before** running the pipeline end-to-end. By the end, you'll understand not just *what* the pipeline does but *why* each design decision was made — and you'll be able to explain it confidently in a data engineering interview.

---

## Checkpoint 1 — Auto Loader and Incremental File Ingestion

**Concept to study**
Auto Loader (`cloudFiles`) is Databricks' incremental file ingestion framework. It monitors a cloud storage path for new files and processes them exactly once using a checkpoint directory. This is more reliable than `spark.read.csv` because it tracks which files have already been processed and prevents re-ingestion.

**Read**
- [Auto Loader overview](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Auto Loader with Unity Catalog](https://docs.databricks.com/ingestion/auto-loader/unity-catalog.html)

**Hands-on exercise**
1. Open [`notebooks/01_ingest_bronze.py`](notebooks/01_ingest_bronze.py) and read the Auto Loader section.
2. Notice `cloudFiles.schemaLocation` and `checkpointLocation` point to `/Volumes/...` paths — not DBFS. Understand why.
3. Notice `trigger(availableNow=True)` — this processes all pending files and stops, rather than running continuously.
4. Run the notebook. Check `workspace.bronze.raw_online_retail` in the Catalog Explorer.
5. Answer: what happens if you delete the checkpoint directory and re-run?

**File in this repo**: [`notebooks/01_ingest_bronze.py`](notebooks/01_ingest_bronze.py), [`docs/bronze.md`](docs/bronze.md)

**Definition of done**: You can explain what Auto Loader does, what the checkpoint prevents, why `availableNow=True` is used, and why Volume paths are required on Free Edition.

---

## Checkpoint 2 — PySpark DataFrame API for Production Transformations

**Concept to study**
PySpark's DataFrame API is the production-grade way to transform data in Databricks. It's type-safe, optimised by the Catalyst query planner, and more readable than raw SQL strings for complex multi-step transformations. Key operations: `filter()`, `withColumn()`, `cast()`, `Window`, `dropDuplicates()`.

**Read**
- [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [PySpark functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Window functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)

**Hands-on exercise**
1. Open [`notebooks/02_transform_silver.py`](notebooks/02_transform_silver.py) and trace the transformation chain.
2. Find the cancellation removal step — understand why `InvoiceNo.startswith("C")` identifies cancellations.
3. Find the deduplication step — understand why the key is `(invoice_no, stock_code, invoice_date, quantity)` and not just `(invoice_no, stock_code)`.
4. Find the quarantine step — understand the three rejection reasons and why `unit_price = 0` is NOT rejected.
5. Run `spark.read.table("workspace.silver.transactions").count()` and compare to the Bronze row count.
6. Answer: what is the difference between `filter()` and `dropDuplicates()` in PySpark?

**File in this repo**: [`notebooks/02_transform_silver.py`](notebooks/02_transform_silver.py), [`docs/silver.md`](docs/silver.md)

**Definition of done**: You can write a PySpark transformation chain using `filter()`, `withColumn()`, `cast()`, and `Window`, and explain why the DataFrame API is preferred over SQL strings for complex transformations.

---

## Checkpoint 3 — Delta Lake: ACID Transactions, Time Travel, and Optimisation

**Concept to study**
Delta Lake is the storage format used for all tables in this pipeline. It adds ACID transactions, schema enforcement, time travel, and optimised write performance on top of Parquet files. Key operations: `DESCRIBE HISTORY`, `OPTIMIZE`, `ZORDER`, `VACUUM`.

**Read**
- [Delta Lake overview](https://docs.databricks.com/delta/index.html)
- [Delta OPTIMIZE and ZORDER](https://docs.databricks.com/delta/optimize.html)
- [Delta time travel](https://docs.databricks.com/delta/history.html)

**Hands-on exercise**
1. After running the pipeline, run:
   ```sql
   DESCRIBE HISTORY workspace.gold.fact_sales;
   ```
2. Find the OPTIMIZE entry in the history (if it ran successfully).
3. Run `SELECT COUNT(*) FROM workspace.gold.fact_sales` before and after a second pipeline run — confirm no duplicates.
4. Read the OPTIMIZE/ZORDER section in [`notebooks/03_build_gold.py`](notebooks/03_build_gold.py) — understand why `ZORDER BY (customer_key, product_key)` is chosen.
5. Answer: what does ZORDER do, and why does it improve query performance for joins?

**File in this repo**: [`notebooks/03_build_gold.py`](notebooks/03_build_gold.py) (Delta optimisation section), [`docs/gold.md`](docs/gold.md)

**Definition of done**: You can explain what ACID means in the context of Delta Lake, how time travel works, what ZORDER does for data skipping, and why VACUUM is needed.

---

## Checkpoint 4 — Star Schema Dimensional Modelling

**Concept to study**
A star schema organises data into a central fact table surrounded by dimension tables. The fact table stores measures (revenue, quantity) and foreign keys to dimensions. Dimensions provide descriptive context (who, what, when, where). The grain of the fact table defines exactly what one row represents — this is the most important design decision.

**Read**
- [Kimball dimensional modelling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Star schema vs snowflake schema](https://docs.databricks.com/lakehouse/medallion.html)

**Hands-on exercise**
1. Open [`notebooks/03_build_gold.py`](notebooks/03_build_gold.py) and identify the five tables being built.
2. Find the `fact_sales` grain definition — understand why it's at line-item level, not order level.
3. Find the surrogate key generation — understand why `xxhash64()` is used instead of `monotonically_increasing_id()`.
4. Run this query and understand the result:
   ```sql
   SELECT d.year, d.month, c.region, SUM(f.revenue) AS revenue
   FROM workspace.gold.fact_sales f
   JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
   JOIN workspace.gold.dim_country c ON f.country_key = c.country_key
   GROUP BY d.year, d.month, c.region ORDER BY d.year, d.month;
   ```
5. Answer: what is the grain of `fact_sales`, and what analytical questions can you answer at this grain?

**File in this repo**: [`notebooks/03_build_gold.py`](notebooks/03_build_gold.py), [`docs/gold.md`](docs/gold.md)

**Definition of done**: You can define the grain of a fact table, explain the difference between a fact and a dimension, and describe why surrogate keys are used instead of natural keys.

---

## Checkpoint 5 — SCD Type 2: Tracking Historical Changes

**Concept to study**
Slowly Changing Dimension Type 2 (SCD2) tracks historical changes by adding new rows rather than overwriting. Each version of a record has an `effective_date`, `expiry_date`, and `current_flag`. This preserves history — you can see what a customer's country was at any point in time.

**Read**
- [SCD Type 2 overview](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
- [Delta MERGE for SCD2](https://docs.databricks.com/delta/merge.html)

**Hands-on exercise**
1. Open [`notebooks/03_build_gold.py`](notebooks/03_build_gold.py) and find the `dim_customer` SCD2 section.
2. Understand the two-step MERGE: (1) expire the old version, (2) insert the new version.
3. Find the temporal join in `fact_sales`:
   ```python
   (col("order_date") >= col("effective_date")) &
   ((col("order_date") < col("expiry_date")) | col("expiry_date").isNull())
   ```
   Understand why this is correct vs `BETWEEN` (which fails for NULL expiry_date).
4. Query `dim_customer` and find a customer with multiple versions:
   ```sql
   SELECT * FROM workspace.gold.dim_customer WHERE customer_id IN (
     SELECT customer_id FROM workspace.gold.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
   ) ORDER BY customer_id, effective_date;
   ```
5. Answer: what is the difference between SCD Type 1 (overwrite) and SCD Type 2 (add new row)?

**File in this repo**: [`notebooks/03_build_gold.py`](notebooks/03_build_gold.py) (dim_customer section), [`docs/gold.md`](docs/gold.md)

**Definition of done**: You can implement SCD Type 2 using a two-step Delta MERGE, explain the temporal join predicate, and describe when you would choose SCD2 over SCD1.

---

## Checkpoint 6 — dbt Testing (Tests-Only Mode)

**Concept to study**
dbt is typically used for SQL transformations, but it can also be used purely for data quality testing. In this pipeline, all transformations are done in PySpark — dbt only runs `schema.yml` tests against the PySpark-built tables. This demonstrates how to integrate dbt testing into a non-dbt transformation pipeline.

**Read**
- [dbt tests overview](https://docs.getdbt.com/docs/build/tests)
- [dbt sources](https://docs.getdbt.com/docs/build/sources)
- [Custom generic tests](https://docs.getdbt.com/guides/best-practices/writing-custom-generic-tests)

**Hands-on exercise**
1. Open [`dbt/models/sources/sources.yml`](dbt/models/sources/sources.yml) — notice it declares PySpark-built tables as dbt sources.
2. Open [`dbt/models/silver/schema.yml`](dbt/models/silver/schema.yml) — find the `not_null`, `assert_composite_unique`, and `assert_positive_value` tests.
3. Open [`dbt/macros/assert_positive_value.sql`](dbt/macros/assert_positive_value.sql) — understand how a generic test returns failing rows.
4. Run `dbt test --target dev` and review the output.
5. Answer: why is `dbt run` not needed in this pipeline? What does `dbt compile` validate?

**File in this repo**: [`dbt/models/silver/schema.yml`](dbt/models/silver/schema.yml), [`dbt/macros/assert_positive_value.sql`](dbt/macros/assert_positive_value.sql)

**Definition of done**: You can write a `schema.yml` test block, explain how a custom generic test macro works, and describe how to use dbt for testing without any transformation models.

---

## Checkpoint 7 — Databricks Asset Bundles (DABs)

**Concept to study**
Databricks Asset Bundles are the IaC framework for Databricks. They let you define jobs, workflows, and resources as YAML files, version-control them in Git, and deploy to multiple environments with a single CLI command.

**Read**
- [DABs overview](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Bundle configuration reference](https://docs.databricks.com/dev-tools/bundles/reference.html)

**Hands-on exercise**
1. Open [`databricks.yml`](databricks.yml) and read through every section.
2. Open [`resources/workflow.yml`](resources/workflow.yml) and trace the 6-task dependency chain.
3. Notice `dbt_test` has `warehouse_id` set explicitly — understand why this is separate from `serverless: true`.
4. Run `databricks bundle validate --target dev` from the project root.
5. Answer: what does `mode: development` do differently from `mode: production`?

**File in this repo**: [`databricks.yml`](databricks.yml), [`resources/workflow.yml`](resources/workflow.yml), [`docs/orchestration.md`](docs/orchestration.md)

**Definition of done**: You can explain what a DAB is, what `databricks bundle deploy` does, and why `warehouse_id` is required on `dbt_task` tasks even when `serverless: true` is set at the job level.

---

## Checkpoint 8 — Databricks Workflows and Task Orchestration

**Concept to study**
Databricks Workflows orchestrate multi-task pipelines with dependency management, scheduling, and observability. The `task_values` API lets tasks pass data to downstream tasks at runtime — used here to propagate `pipeline_run_id` from the ingestion notebook to all downstream tasks.

**Read**
- [Databricks Workflows overview](https://docs.databricks.com/workflows/index.html)
- [Task values](https://docs.databricks.com/workflows/jobs/how-to-share-task-values.html)

**Hands-on exercise**
1. Open [`resources/workflow.yml`](resources/workflow.yml) and trace the task dependency chain.
2. Find where `pipeline_run_id` is set (`01_ingest_bronze.py`) and where it's read (`02_transform_silver.py`, `03_build_gold.py`, `04_audit_log.py`).
3. Deploy and run: `databricks bundle run online_retail_pipeline_workflow --target dev`
4. In the Databricks UI, open the workflow run and inspect each task's logs.
5. Query the audit log to see all events from the run:
   ```sql
   SELECT * FROM workspace.bronze.pipeline_audit_log ORDER BY logged_at;
   ```
6. Answer: what would happen if `transform_silver` failed — would `build_gold` still execute?

**File in this repo**: [`resources/workflow.yml`](resources/workflow.yml), [`notebooks/04_audit_log.py`](notebooks/04_audit_log.py), [`docs/orchestration.md`](docs/orchestration.md)

**Definition of done**: You can read a DAB workflow YAML, explain task dependencies, describe how `task_values` work, and query the audit log to verify pipeline run health.

---

## What's Next?

After completing all 8 checkpoints:

1. Run the full pipeline: `databricks bundle run online_retail_pipeline_workflow --target dev`
2. Query the Gold tables:
   ```sql
   -- Revenue by product category
   SELECT p.category, SUM(f.revenue) AS total_revenue, COUNT(*) AS line_items
   FROM workspace.gold.fact_sales f
   JOIN workspace.gold.dim_product p ON f.product_key = p.product_key
   GROUP BY p.category ORDER BY total_revenue DESC;

   -- Monthly revenue trend
   SELECT d.year, d.month, SUM(f.revenue) AS monthly_revenue
   FROM workspace.gold.fact_sales f
   JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
   GROUP BY d.year, d.month ORDER BY d.year, d.month;
   ```
3. Check the quarantine table: `SELECT rejection_reason, COUNT(*) FROM workspace.silver.quarantine_transactions GROUP BY rejection_reason`
4. Run dbt tests: `cd dbt && dbt test --target dev`
