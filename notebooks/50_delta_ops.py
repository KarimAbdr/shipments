# Databricks notebook source
# MAGIC %md
# MAGIC # 50 — Delta operations: history, time travel, RESTORE, OPTIMIZE
# MAGIC
# MAGIC **Purpose:** look at the Delta transaction log of `silver_shipments`. Then practise time travel (TODO-11) and file layout (TODO-12).
# MAGIC
# MAGIC **Run on:** Serverless
# MAGIC
# MAGIC > **Warning:** never run `VACUUM ... RETAIN 0 HOURS` in this project. It deletes the old files that time travel and `RESTORE` need.

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
# MAGIC ## 1. Reference: history and time travel
# MAGIC
# MAGIC Every write creates a new version in the transaction log. `DESCRIBE HISTORY` lists them.

# COMMAND ----------

silver_table = cfg.table(config.SILVER)
display(spark.sql(f"DESCRIBE HISTORY {silver_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC `VERSION AS OF 0` reads the table as it was at its first version.

# COMMAND ----------

now_rows = spark.table(silver_table).count()
v0_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {silver_table} VERSION AS OF 0").first()["n"]
print(f"silver rows now: {now_rows}; at VERSION AS OF 0: {v0_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Time travel diff and RESTORE (TODO-11)

# COMMAND ----------

# TODO-11: Time travel + RESTORE
# WHAT:   diff two versions (EXCEPT), `DELETE` Midwest rows, `RESTORE TABLE ... TO VERSION AS OF n`
# WHY:    Delta keeps a transaction log, so a bad write can be inspected and undone.
# PRODUCT: A bad delete in silver can be recovered in minutes without a backup restore.
# SCOPE:  ~10 lines SQL, ~20 min, difficulty 2/3
# HINTS:  DESCRIBE HISTORY, VERSION AS OF, EXCEPT, DELETE FROM, RESTORE TABLE ... TO VERSION AS OF. Docs: time travel https://docs.databricks.com/aws/en/delta/history ; RESTORE https://docs.databricks.com/aws/en/sql/language-manual/delta-restore
# DONE WHEN: silver count back to pre-delete value; history shows DELETE and RESTORE ops
# SEE:    50_delta_ops reference cell, docs/TODO.md#todo-11
print("TODO-11 not done yet - skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. OPTIMIZE and liquid clustering (TODO-12)

# COMMAND ----------

# TODO-12: OPTIMIZE + liquid clustering
# WHAT:   `ALTER TABLE workspace.shipments.silver_shipments CLUSTER BY (region, event_date)`, `OPTIMIZE`, compare `DESCRIBE DETAIL` numFiles/clusteringColumns
# WHY:    Many small files slow reads. OPTIMIZE compacts them, and clustering groups rows that are often filtered together.
# PRODUCT: Faster queries on region and date filters, with less compute per query.
# SCOPE:  ~6 lines SQL, ~25 min, difficulty 2/3
# HINTS:  DESCRIBE DETAIL, OPTIMIZE, ALTER TABLE ... CLUSTER BY. Docs: liquid clustering https://docs.databricks.com/aws/en/delta/clustering ; OPTIMIZE https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize . Free Edition behaviour is not verified by the scaffold author: observe, do not assume.
# DONE WHEN: `clusteringColumns` shows both columns; write down numFiles before/after
# SEE:    50_delta_ops, docs/TODO.md#todo-12
print("TODO-12 not done yet - skipped")
