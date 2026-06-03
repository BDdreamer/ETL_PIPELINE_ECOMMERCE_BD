# Observability — Pipeline Health and Data Quality

## What's Available on Free Edition

Databricks Premium tier provides:
- **Lakehouse Monitoring** — continuously tracks data quality metrics and statistical drift
- **System tables** (`system.lakeflow`, `system.query.history`) — account-wide job run history

Both require Premium tier and are **not available on Databricks Free Edition**.

This pipeline achieves equivalent observability using data it writes itself:

| Premium feature | Free Edition replacement |
|---|---|
| `system.lakeflow.job_run_timeline` | `workspace.bronze.pipeline_audit_log` |
| Lakehouse Monitoring drift detection | `workspace.silver.quality_failures` (dbt tests) |
| Data quality metrics | `workspace.silver.quarantine_transactions` |

---

## The Audit Log — `workspace.bronze.pipeline_audit_log`

Four event types are recorded across the pipeline:

| Event | Written by | When |
|---|---|---|
| `INGESTION_COMPLETE` | `notebooks/01_ingest_bronze.py` | After Auto Loader stream finishes |
| `SILVER_COMPLETE` | `notebooks/02_transform_silver.py` | After Silver write completes |
| `GOLD_COMPLETE` | `notebooks/03_build_gold.py` | After all Gold tables are built |
| `PIPELINE_COMPLETE` | `notebooks/04_audit_log.py` | After all tasks finish |

### Useful queries

```sql
-- All events for the most recent pipeline run
SELECT pipeline_run_id, event_type, status, rows_processed, duration_seconds, logged_at
FROM workspace.bronze.pipeline_audit_log
ORDER BY logged_at DESC
LIMIT 20;

-- Pipeline run history (PIPELINE_COMPLETE events only)
SELECT pipeline_run_id, status, rows_processed, duration_seconds, logged_at
FROM workspace.bronze.pipeline_audit_log
WHERE event_type = 'PIPELINE_COMPLETE'
ORDER BY logged_at DESC;

-- Run success rate
SELECT status, COUNT(*) AS run_count
FROM workspace.bronze.pipeline_audit_log
WHERE event_type = 'PIPELINE_COMPLETE'
GROUP BY status;

-- Average Silver transformation duration
SELECT ROUND(AVG(duration_seconds), 1) AS avg_silver_duration_seconds
FROM workspace.bronze.pipeline_audit_log
WHERE event_type = 'SILVER_COMPLETE' AND status = 'SUCCESS';
```

---

## Silver Quarantine — `workspace.silver.quarantine_transactions`

Malformed rows isolated during Silver transformation. Three rejection reasons:

| Rejection reason | Condition |
|---|---|
| `NEGATIVE_QUANTITY` | `quantity <= 0` after casting |
| `INVALID_TIMESTAMP` | `invoice_date` unparseable |
| `NEGATIVE_PRICE` | `unit_price < 0` after casting |

```sql
-- Quarantine summary by rejection reason and run
SELECT _pipeline_run_id, rejection_reason, COUNT(*) AS rejected_rows
FROM workspace.silver.quarantine_transactions
GROUP BY _pipeline_run_id, rejection_reason
ORDER BY _pipeline_run_id DESC, rejected_rows DESC;

-- Sample quarantine rows for investigation
SELECT * FROM workspace.silver.quarantine_transactions
WHERE rejection_reason = 'NEGATIVE_QUANTITY'
LIMIT 20;
```

---

## dbt Quality Failures — `workspace.silver.quality_failures`

Written by `notebooks/05_load_quality_failures.py` after each `dbt test` run. One row per failing test per pipeline run.

```sql
-- All failures in the last run
SELECT check_name, model_name, severity, failing_row_count, sample_failing_values
FROM workspace.silver.quality_failures
WHERE pipeline_run_id = (
    SELECT pipeline_run_id FROM workspace.silver.quality_failures
    ORDER BY recorded_at DESC LIMIT 1
);

-- Recurring failures across runs
SELECT check_name, model_name, COUNT(*) AS failure_count, SUM(failing_row_count) AS total_failing_rows
FROM workspace.silver.quality_failures
GROUP BY check_name, model_name
ORDER BY failure_count DESC;
```

---

## Diagnosing a Failed Pipeline Run

1. **Check the Workflow run logs** in the Databricks UI: **Workflows → online_retail_pipeline_workflow → \<run\>**. Click the failed task to see its error output.

2. **Check the audit log** for the run:
   ```sql
   SELECT * FROM workspace.bronze.pipeline_audit_log
   WHERE pipeline_run_id = '<your-run-id>'
   ORDER BY logged_at;
   ```

3. **Check quality failures** if `dbt_test` or `load_quality_failures` failed:
   ```sql
   SELECT * FROM workspace.silver.quality_failures
   WHERE pipeline_run_id = '<your-run-id>';
   ```

4. **Check the quarantine table** if Silver row counts look wrong:
   ```sql
   SELECT rejection_reason, COUNT(*) FROM workspace.silver.quarantine_transactions
   WHERE _pipeline_run_id = '<your-run-id>'
   GROUP BY rejection_reason;
   ```

5. **Check the Bronze quarantine** if ingestion failed:
   ```sql
   SELECT * FROM workspace.bronze.quarantine
   WHERE _pipeline_run_id = '<your-run-id>';
   ```

---

## Gold Layer Analytical Queries

```sql
-- Revenue by country and year
SELECT c.region, c.country_name, d.year, SUM(f.revenue) AS total_revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_country c ON f.country_key = c.country_key
JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
GROUP BY c.region, c.country_name, d.year
ORDER BY d.year, total_revenue DESC;

-- Top 20 products by revenue
SELECT p.stock_code, p.description, p.category, SUM(f.revenue) AS total_revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.stock_code, p.description, p.category
ORDER BY total_revenue DESC LIMIT 20;

-- Top customers by revenue (current version only)
SELECT c.customer_id, c.country, c.customer_segment, SUM(f.revenue) AS total_revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_customer c ON f.customer_key = c.customer_key
WHERE c.current_flag = true
GROUP BY c.customer_id, c.country, c.customer_segment
ORDER BY total_revenue DESC LIMIT 20;

-- Monthly revenue trend
SELECT d.year, d.month, d.month_name, SUM(f.revenue) AS monthly_revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Weekend vs weekday revenue
SELECT d.weekend_flag, SUM(f.revenue) AS total_revenue, COUNT(*) AS line_items
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.weekend_flag;
```
