# Online Retail Pipeline

An end-to-end data engineering project on **Databricks Free Edition** using the UCI Online Retail dataset (~541,000 UK e-commerce transactions, 2010–2011). The pipeline ingests a CSV file through a Bronze → Silver → Gold medallion architecture. PySpark handles all transformations. The Gold layer implements a proper **star schema dimensional model** with SCD Type 2 on the customer dimension. dbt is used for data quality testing only.

This project is designed as a **learning journey** demonstrating production-grade Databricks and PySpark skills. Read [`LEARNING.md`](LEARNING.md) before diving into the build steps.

---

## How to Learn with This Project

> **Start here before running anything.**

1. Read [`LEARNING.md`](LEARNING.md) — 8 checkpoints covering every major concept.
2. Work through the checkpoints in order. Each one points to a specific file in this repo.
3. Once you've completed the checkpoints, follow the "Running the Pipeline" steps below.

The `docs/` directory has a deep-dive for each layer:
- [`docs/governance.md`](docs/governance.md) — Unity Catalog, workspace catalog, Volumes
- [`docs/bronze.md`](docs/bronze.md) — Auto Loader, checkpoint deduplication, quarantine
- [`docs/silver.md`](docs/silver.md) — PySpark transformations, null handling, dedup, quarantine
- [`docs/gold.md`](docs/gold.md) — Star schema, SCD Type 2, surrogate keys, OPTIMIZE/ZORDER
- [`docs/orchestration.md`](docs/orchestration.md) — Databricks Workflows, DABs, task_values
- [`docs/observability.md`](docs/observability.md) — Audit log, quality failures, quarantine queries

---

## Repository Structure

```
online-retail-pipeline/
├── databricks.yml                      # DAB root config — bundle, variables, targets
├── resources/
│   └── workflow.yml                    # 6-task Databricks Workflow definition
├── notebooks/
│   ├── 00_setup_catalog.py             # One-time setup: schemas, volumes, upload instructions
│   ├── 01_ingest_bronze.py             # Auto Loader → bronze.raw_online_retail
│   ├── 02_transform_silver.py          # PySpark → silver.transactions + quarantine_transactions
│   ├── 03_build_gold.py                # PySpark → dim_* + fact_sales (star schema, SCD2)
│   ├── 04_audit_log.py                 # PIPELINE_COMPLETE audit event
│   └── 05_load_quality_failures.py     # dbt test results → silver.quality_failures
├── dbt/
│   ├── dbt_project.yml                 # dbt project config (tests-only, no models)
│   ├── profiles.yml                    # dbt connection profile (databricks adapter)
│   ├── models/
│   │   ├── sources/
│   │   │   └── sources.yml             # Declares Silver + Gold tables as dbt sources
│   │   ├── silver/
│   │   │   └── schema.yml              # Tests on silver.transactions
│   │   └── gold/
│   │       └── schema.yml              # Tests on fact_sales + dim_*
│   └── macros/
│       ├── assert_positive_value.sql   # Custom generic test: value > 0
│       └── assert_composite_unique.sql # Custom generic test: composite key uniqueness
├── docs/
│   ├── bronze.md
│   ├── silver.md
│   ├── gold.md
│   ├── orchestration.md
│   ├── governance.md
│   └── observability.md
├── .github/
│   └── workflows/
│       └── ci.yml                      # GitHub Actions: validate → dev deploy → prod deploy
├── LEARNING.md                         # 8-checkpoint learning plan
├── requirements.txt                    # Pinned Python/CLI dependencies
└── .gitignore
```

---

## Data Model

```
bronze.raw_online_retail          ← all 8 source columns as STRING + 3 metadata cols
         ↓ PySpark (02_transform_silver.py)
silver.transactions               ← typed, deduplicated, no cancellations, Revenue + OrderDate
silver.quarantine_transactions    ← malformed rows (negative qty, bad timestamp, negative price)
         ↓ PySpark (03_build_gold.py)
gold.dim_date                     ← date spine (date_key, day, month, quarter, year, weekend_flag)
gold.dim_country                  ← countries with derived region mapping
gold.dim_product                  ← products with keyword-derived category
gold.dim_customer                 ← SCD Type 2 on country change (effective_date, current_flag)
gold.fact_sales                   ← grain: one row per invoice line item (InvoiceNo + StockCode)
                                     measures: quantity, revenue, order_count
                                     keys: customer_key, product_key, date_key, country_key
```

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Python | 3.10+ | [python.org](https://www.python.org/downloads/) |
| Databricks CLI | 0.220+ | `pip install databricks-cli` |
| dbt-databricks | 1.8+ | `pip install dbt-databricks` |
| dbt-core | 1.8+ | Installed with dbt-databricks |

```bash
pip install -r requirements.txt
```

---

## Before You Run

### Step 1 — Authenticate the Databricks CLI

```bash
databricks configure --token
# Enter your workspace URL: https://<your-workspace>.azuredatabricks.net
# Enter your personal access token
```

### Step 2 — Set environment variables for dbt

```bash
export DATABRICKS_HOST=https://<your-workspace>.azuredatabricks.net
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<your-warehouse-id>
export DATABRICKS_TOKEN=<your-personal-access-token>
```

Find your HTTP path: **Databricks UI → SQL Warehouses → \<your warehouse\> → Connection details**

### Step 3 — Run the one-time setup notebook

Open `notebooks/00_setup_catalog.py` in your Databricks workspace and run it. This creates:
- Schemas: `workspace.bronze`, `workspace.silver`, `workspace.gold`
- Volumes: `workspace.bronze.landing`, `workspace.bronze.autoloader_checkpoint`, `workspace.bronze.autoloader_schema`

### Step 4 — Upload the dataset

The Online Retail dataset is an XLSX file from the UCI Machine Learning Repository. Convert it to CSV first, then upload:

**Option 1 — Databricks CLI:**
```bash
databricks fs cp online_retail.csv /Volumes/workspace/bronze/landing/online_retail.csv
```

**Option 2 — Databricks UI:**
Go to **Catalog → workspace → bronze → landing → Upload to this volume**

### Step 5 — Set your warehouse ID in `databricks.yml`

Find your serverless SQL warehouse ID in the Databricks UI under **SQL Warehouses → \<your warehouse\> → Connection details**.

Uncomment and set the `warehouse_id` variable in `databricks.yml`:
```yaml
variables:
  warehouse_id:
    default: "<your-warehouse-id>"
```

---

## Running the Pipeline

```bash
# Deploy to dev
databricks bundle deploy --target dev

# Trigger a manual run
databricks bundle run online_retail_pipeline_workflow --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

### Run dbt tests locally (optional)

```bash
cd dbt
dbt deps
dbt test --target dev
```

Note: `dbt run` is not used — there are no dbt transformation models. Only `dbt test` is needed.

---

## Gold Layer Queries

After a successful pipeline run, query the dimensional model:

```sql
-- Revenue by country and year
SELECT c.region, d.year, SUM(f.revenue) AS total_revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_country c ON f.country_key = c.country_key
JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
GROUP BY c.region, d.year ORDER BY d.year, total_revenue DESC;

-- Top 20 products by revenue
SELECT p.stock_code, p.description, p.category, SUM(f.revenue) AS total_revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.stock_code, p.description, p.category
ORDER BY total_revenue DESC LIMIT 20;

-- Customer SCD2 history
SELECT customer_id, country, customer_segment, effective_date, expiry_date, current_flag
FROM workspace.gold.dim_customer
ORDER BY customer_id, effective_date;
```

---

## CI/CD

Every pull request triggers `databricks bundle validate` and `dbt compile`. Merges to `main` auto-deploy to `dev`. Prod deployments require manual approval via a GitHub Environment protection rule. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
