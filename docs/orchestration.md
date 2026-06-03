# Orchestration — Databricks Workflows and DABs

## Databricks Workflows

A Databricks Workflow is a directed acyclic graph (DAG) of tasks. Each task can be a notebook, a dbt command, a Python script, or a SQL query. Tasks run in the order defined by their `depends_on` relationships — if a task fails, downstream tasks don't run.

This pipeline's workflow has 6 tasks:

```
ingest_bronze
    ↓
transform_silver
    ↓
  build_gold
    ↓
  dbt_test
    ↓
load_quality_failures
    ↓
  audit_log
```

**Implementation:** [`resources/workflow.yml`](../resources/workflow.yml)

---

## Task Dependency Chain

| Task | Type | What it does | Depends on |
|---|---|---|---|
| `ingest_bronze` | notebook | Auto Loader → `bronze.raw_online_retail` | — |
| `transform_silver` | notebook | PySpark → `silver.transactions` + quarantine | `ingest_bronze` |
| `build_gold` | notebook | PySpark → `dim_*` + `fact_sales` (star schema, SCD2) | `transform_silver` |
| `dbt_test` | dbt_task | Runs all schema.yml tests against Silver + Gold tables | `build_gold` |
| `load_quality_failures` | notebook | Parses test results → `silver.quality_failures` | `dbt_test` |
| `audit_log` | notebook | Writes `PIPELINE_COMPLETE` to audit log | `load_quality_failures` |

If `ingest_bronze` fails, none of the downstream tasks run. If `dbt_test` finds an `error`-severity failure, `load_quality_failures` raises an exception, which prevents `audit_log` from recording a false success.

---

## `task_values` — Inter-Task Communication

The `pipeline_run_id` correlation ID needs to flow from the ingestion notebook to the dbt tasks and the audit log notebook. Databricks Workflows provides a key-value store scoped to a single job run called **task values**.

**Setting a value** (in `notebooks/01_ingest_bronze.py`):
```python
dbutils.jobs.taskValues.set(key="pipeline_run_id", value=pipeline_run_id)
```

**Reading a value in a notebook task** (in `notebooks/02_transform_silver.py`, `notebooks/03_build_gold.py`, `notebooks/04_audit_log.py`):
```python
pipeline_run_id = dbutils.jobs.taskValues.get(
    taskKey="ingest_bronze",   # must match the task_key in workflow.yml
    key="pipeline_run_id",
    debugValue=str(uuid.uuid4()),  # fallback for interactive runs
)
```

Note: in this pipeline, `pipeline_run_id` is passed to notebook tasks via `task_values`. The `dbt_test` task does not need it — dbt tests run against the already-built tables and don't require a run ID.

---

## Serverless Compute vs SQL Warehouse

This pipeline uses two different compute types, and understanding the distinction is important:

**`serverless: true` at the job level** — applies to notebook tasks. Databricks provisions serverless compute automatically; no cluster configuration is needed.

**`warehouse_id` on `dbt_task` tasks** — dbt executes SQL against a Databricks SQL warehouse. This is separate from the serverless compute used by notebooks. Even with `serverless: true` at the job level, `dbt_task` tasks will fail at runtime if `warehouse_id` is not set on the task itself.

```yaml
- task_key: dbt_test
  dbt_task:
    warehouse_id: ${var.warehouse_id}   # required — not covered by job-level serverless: true
    commands:
      - "dbt test"
```

Find your warehouse ID in the Databricks UI: **SQL Warehouses → \<your warehouse\> → Connection details** (the last segment of the HTTP path).

---

## Databricks Asset Bundles (DABs)

DABs are the IaC framework for Databricks. They let you define jobs, workflows, and resources as YAML files, version-control them in Git, and deploy to multiple environments with a single CLI command.

### Project structure

```
databricks.yml          ← bundle root: variables, targets, includes
resources/
  workflow.yml          ← workflow resource definition
```

`databricks.yml` includes `resources/*.yml` via:
```yaml
include:
  - resources/*.yml
```

### Variables and targets

Variables are defined at the bundle level with defaults, then overridden per target:

```yaml
variables:
  warehouse_id:
    default: ""

targets:
  dev:
    variables:
      warehouse_id: "<dev-warehouse-id>"
  prod:
    variables:
      warehouse_id: "<prod-warehouse-id>"
```

This means the same `workflow.yml` works for both environments — no hardcoded values.

### Deploy and run

```bash
# Deploy to dev
databricks bundle deploy --target dev

# Trigger a manual run
databricks bundle run online_retail_pipeline_workflow --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

### Schedule

The workflow schedule is controlled by `${var.cron_schedule}` and is set to `pause_status: PAUSED` in dev so it doesn't fire automatically during development. In prod, the schedule runs at 04:00 UTC daily.

---

## Further Reading

- [Databricks Workflows overview](https://docs.databricks.com/workflows/index.html)
- [Task values](https://docs.databricks.com/workflows/jobs/how-to-share-task-values.html)
- [dbt task type](https://docs.databricks.com/workflows/jobs/dbt-task.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Bundle configuration reference](https://docs.databricks.com/dev-tools/bundles/reference.html)
