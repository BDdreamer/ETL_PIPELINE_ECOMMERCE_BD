# Bronze Layer — Raw Ingestion

## Purpose

The Bronze layer is the first stop for all data entering the pipeline. Its job is simple: **preserve the source data exactly as received**, with no transformations, no type casting, and no filtering. Every row from the Online Retail CSV lands here as-is.

This "raw fidelity" principle means:
- If the source data has errors, they're visible and auditable in Bronze
- You can always replay transformations from Bronze if Silver or Gold logic changes
- Debugging is easier because you can compare Bronze to Silver to understand what changed

**Tables in this layer:**
- `workspace.bronze.raw_online_retail` — the main ingestion table (all 8 source columns as STRING)
- `workspace.bronze.quarantine` — rows that couldn't be parsed by Auto Loader
- `workspace.bronze.pipeline_audit_log` — one row per pipeline event for observability

**Implementation:** [`notebooks/01_ingest_bronze.py`](../notebooks/01_ingest_bronze.py)

---

## Auto Loader (`cloudFiles`)

Auto Loader is Databricks' incremental file ingestion framework built on Spark Structured Streaming. It monitors a cloud storage path for new files and processes them **exactly once**.

### How it works

```
/Volumes/workspace/bronze/landing/   ← source files land here
         ↓
   Auto Loader reads new files
         ↓
/Volumes/workspace/bronze/autoloader_checkpoint/   ← tracks what's been processed
         ↓
   workspace.bronze.raw_online_retail   ← Delta table
```

The **checkpoint directory** is the key mechanism. After each micro-batch, Auto Loader writes the file offsets (names, sizes, modification times) it successfully committed. On the next run, it reads the checkpoint and skips already-processed files. This gives **exactly-once ingestion semantics**.

### `trigger(availableNow=True)`

This trigger processes all files that have arrived since the last checkpoint, then stops. It behaves like a batch job but uses the full streaming checkpoint mechanism for deduplication. This is the right pattern for scheduled pipeline jobs.

### Why Volume paths, not DBFS?

On Databricks Free Edition with Unity Catalog, DBFS paths (`/dbfs/...`) are not supported for:
- `cloudFiles.schemaLocation`
- `checkpointLocation`
- The source path itself

Volume paths (`/Volumes/workspace/bronze/...`) are required. They're governed by Unity Catalog access controls and work correctly on Free Edition.

---

## Why All Columns Are STRING

Every column in `workspace.bronze.raw_online_retail` is stored as `STRING`, including `Quantity`, `UnitPrice`, and `InvoiceDate`. This is intentional.

The Bronze layer's job is to preserve raw data. Type casting is a transformation — it belongs in Silver. If you cast `Quantity` to INT in Bronze and the source has a value like `"12 units"`, the cast fails and you lose the row. As a STRING, it lands in Bronze and the Silver notebook can handle the edge case explicitly.

---

## Delta Table Properties

```sql
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
```

Streaming writes produce many small Parquet files, which degrades read performance. These properties tell Databricks to automatically optimise file sizes on write and compact small files in the background.

---

## Further Reading

- [Auto Loader overview](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Auto Loader with Unity Catalog](https://docs.databricks.com/ingestion/auto-loader/unity-catalog.html)
- [Delta Lake overview](https://docs.databricks.com/delta/index.html)
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
