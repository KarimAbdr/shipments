# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Bronze: load raw CSV batches
# MAGIC
# MAGIC **Purpose:** read each CSV file from the volume as text, add `_source_file` and `_ingested_at`, and append it to `bronze_shipments`.
# MAGIC Bronze never cleans data. Quality rules run later, in notebook 20.
# MAGIC
# MAGIC **Run on:** Serverless
# MAGIC
# MAGIC Run 00 first (for example with `days=1`).

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from shipments_pipeline import config
cfg = config.from_widgets(dbutils)
run_id = config.get_widget(dbutils, "run_id", "manual")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List the batch files in the volume

# COMMAND ----------

bronze_table = cfg.table(config.BRONZE)
volume_files = sorted(f.name for f in dbutils.fs.ls(cfg.volume_path) if f.name.endswith(".csv"))
print("Files in volume:", volume_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Choose which files to load
# MAGIC
# MAGIC Bronze must be **idempotent**: running this notebook twice must not load a file twice.
# MAGIC The notebook gives `list_new_files` file names only (no paths), so both sides compare the same kind of value.

# COMMAND ----------

# TODO-07: Incremental bronze
# WHAT:   `list_new_files`
# WHY:    Re-running a notebook must not load the same file twice (idempotency).
# PRODUCT: Daily jobs can be retried safely without double-counting shipments.
# SCOPE:  ~5 lines + notebook, ~30 min, difficulty 2/3
# HINTS:  Plain Python set difference over file names. The notebook already passes file names (basenames) on both sides and reduces `_source_file` to a basename, so compare them directly. Docs: https://docs.databricks.com/aws/en/ingestion/file-metadata-column
# DONE WHEN: running 10_bronze twice keeps bronze at 1,260 (day 1)
# SEE:    bronze.py:read_batch, docs/TODO.md#todo-07

from shipments_pipeline import bronze

ingested: set[str] = set()
if spark.catalog.tableExists(bronze_table):
    rows = spark.table(bronze_table).select("_source_file").distinct().collect()
    ingested = {os.path.basename(r["_source_file"]) for r in rows}

try:
    new_files = bronze.list_new_files(volume_files, ingested)
except NotImplementedError:
    print("WARNING: TODO-07 not done yet - falling back to ALL files. Running this notebook again will duplicate rows.")
    new_files = volume_files
print("Files to load:", new_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read each file and append it to bronze
# MAGIC
# MAGIC `mode("append")` adds rows and never rewrites old ones. Bronze is a history of what arrived.

# COMMAND ----------

# TODO-10: Schema evolution
# WHAT:   bronze append needs `mergeSchema`, the quarantine append in 20_silver needs `mergeSchema`, and the silver MERGE must use `UPDATE SET *` / `INSERT *` with schema evolution (`MERGE WITH SCHEMA EVOLUTION INTO ...`)
# WHY:    Upstream systems add columns. You decide whether to accept them (evolution) or reject them (enforcement).
# PRODUCT: New carrier data flows in without a manual schema migration; older rows show NULL.
# SCOPE:  ~7 lines, ~30 min, difficulty 2/3
# HINTS:  Delta schema evolution docs (named in WHAT). Expect day 3 to fail on this append: the new `carrier` column is not in the table yet. The older session-wide alternative is spark.databricks.delta.schema.autoMerge.enabled; prefer the per-statement form. Docs: https://docs.databricks.com/aws/en/delta/update-schema
# DONE WHEN: 00 days=3 → 10 → 20: bronze 1,665, silver 1,350, `carrier` column exists, NULL for older rows
# SEE:    docs/CONCEPTS.md#schema-enforcement-vs-evolution, docs/TODO.md#todo-10
# Note for day 3: without TODO-10, the append below stops with a schema-mismatch error. That is the expected starting point.

from shipments_pipeline import audit

rows_written = 0
for name in new_files:
    df = bronze.read_batch(spark, f"{cfg.volume_path}/{name}")
    count = df.count()
    df.write.mode("append").saveAsTable(bronze_table)
    rows_written += count
    print(f"appended {count} rows from {name}")

audit.log_run(spark, cfg.table(config.PIPELINE_RUNS), run_id, "bronze", bronze_table, rows_written)
print("Bronze rows written in this run:", rows_written)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check the result
# MAGIC
# MAGIC Every row carries the file it came from. Use this count to see whether a file was loaded twice.

# COMMAND ----------

display(spark.table(bronze_table).groupBy("_source_file").count())
