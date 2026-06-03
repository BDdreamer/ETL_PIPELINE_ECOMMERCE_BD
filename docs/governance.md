# Governance — Unity Catalog

## What is Unity Catalog?

Unity Catalog is Databricks' unified governance layer for all data assets. It provides:
- A **three-level namespace** for organising tables: `catalog.schema.table`
- **Fine-grained access control** at the catalog, schema, table, and column level
- **Data lineage** tracking across notebooks, jobs, and SQL queries
- **Data discovery** via the Catalog Explorer UI

On Databricks Free Edition, Unity Catalog is included and a catalog named `workspace` is automatically provisioned. You don't need to create it manually.

---

## The Three-Level Namespace

Every table in this pipeline uses the full three-level namespace:

```
workspace.bronze.raw_online_retail
│         │      └── table name
│         └── schema name
└── catalog name
```

This convention is documented at the top of `databricks.yml` and used consistently across all notebooks, dbt models, and SQL statements. Using the full namespace ensures:
- No ambiguity about which catalog or schema a table belongs to
- dbt models work correctly with the Unity Catalog adapter
- Lineage tracking in the Catalog Explorer shows the correct upstream/downstream relationships

---

## The `workspace` Catalog on Free Edition

Databricks Free Edition automatically provisions a catalog named `workspace`. This is the only catalog available by default — you cannot create additional catalogs via the Account Console (which requires admin access not available on Free Edition), but you can create them via SQL:

```sql
CREATE CATALOG IF NOT EXISTS my_catalog;
```

This pipeline uses `workspace` as the single catalog for all layers. For a learning project, this is sufficient. If you need environment isolation (dev vs prod), you can create a second catalog manually and update the `catalog_name` variable in `databricks.yml`.

---

## Schemas

Schemas are namespaces within a catalog that group related tables. This pipeline uses three schemas corresponding to the medallion layers:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
```

These are created by [`notebooks/00_setup_catalog.py`](../notebooks/00_setup_catalog.py), which must be run once before the pipeline workflow is triggered.

Schema comments are added for data discovery:
```sql
COMMENT ON SCHEMA workspace.bronze IS 'Bronze layer — raw ingestion...';
```

---

## Unity Catalog Volumes

Volumes are Unity Catalog's managed storage for **non-tabular files** (CSVs, JSON, images, model artifacts, etc.). They're accessed via `/Volumes/<catalog>/<schema>/<volume>/` paths.

**Why Volumes instead of DBFS?**

On Databricks Free Edition with Unity Catalog enabled, DBFS paths (`/dbfs/...`) are not supported for:
- Auto Loader `cloudFiles.schemaLocation`
- Auto Loader `checkpointLocation`
- The source path for file ingestion

Attempting to use DBFS paths will raise a permission or "path not found" error at runtime. Volume paths are the correct abstraction and are governed by Unity Catalog access controls.

This pipeline uses three volumes in the `bronze` schema:

| Volume | Path | Purpose |
|---|---|---|
| `landing` | `/Volumes/workspace/bronze/landing/` | Drop zone for `online_retail.csv` |
| `autoloader_checkpoint` | `/Volumes/workspace/bronze/autoloader_checkpoint/` | Auto Loader checkpoint |
| `autoloader_schema` | `/Volumes/workspace/bronze/autoloader_schema/` | Auto Loader schema location |

All three are created by the setup notebook:
```sql
CREATE VOLUME IF NOT EXISTS workspace.bronze.landing;
CREATE VOLUME IF NOT EXISTS workspace.bronze.autoloader_checkpoint;
CREATE VOLUME IF NOT EXISTS workspace.bronze.autoloader_schema;
```

---

## Table Registration

When the ingestion notebook writes to `workspace.bronze.raw_online_retail` using `toTable("workspace.bronze.raw_online_retail")`, the table is automatically registered in Unity Catalog. You can immediately see it in:
- **Databricks UI → Catalog → workspace → bronze → raw_online_retail**
- SQL: `SHOW TABLES IN workspace.bronze`

PySpark notebooks write all Silver and Gold tables using `saveAsTable("workspace.<schema>.<table>")` with the three-level namespace, so they are all automatically registered in Unity Catalog.

---

## Column Comments for Data Discovery

Column-level comments are defined in the `CREATE TABLE` DDL in each notebook. They appear in the Catalog Explorer when you click on a table and inspect its columns.

Example from the Bronze ingestion notebook:
```sql
InvoiceNo  STRING COMMENT 'Invoice number; starts with C for cancellations',
StockCode  STRING COMMENT 'Product/item code',
CustomerID STRING COMMENT 'Customer identifier; may be NULL for guest transactions',
```

For Gold tables, column comments are particularly important because these are the tables that analysts query directly. All five Gold tables (`fact_sales`, `dim_customer`, `dim_product`, `dim_date`, `dim_country`) have column-level comments defined in their `CREATE TABLE` DDL in `notebooks/03_build_gold.py`.

---

## Further Reading

- [Unity Catalog overview](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
- [Manage schemas](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/index.html)
- [dbt-databricks Unity Catalog setup](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
