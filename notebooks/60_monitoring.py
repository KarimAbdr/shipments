# Databricks notebook source
# MAGIC %md
# MAGIC # 60 — Monitoring: run log, query plan, cost
# MAGIC
# MAGIC **Purpose:** read the audit log, then look at the query plan (TODO-13) and the billing system table (TODO-18).
# MAGIC
# MAGIC **Run on:** Serverless

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
# MAGIC ## 1. Last 20 audit rows

# COMMAND ----------

from pyspark.sql import functions as F

runs_table = cfg.table(config.PIPELINE_RUNS)
if spark.catalog.tableExists(runs_table):
    display(spark.table(runs_table).orderBy(F.col("logged_at").desc()).limit(20))
else:
    print("pipeline_runs does not exist yet - run 10_bronze first")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query plan of the gold query (TODO-13)

# COMMAND ----------

# TODO-13: Audit everywhere + query plan
# WHAT:   call `log_run` in silver/gold/dq tasks; `explain("formatted")` gold query, find Exchange (shuffle) and explain why
# WHY:    Row counts per task make runs comparable. The plan shows where Spark moves data between machines (a shuffle, which is expensive).
# PRODUCT: Ops can see row counts per run and find the slow step.
# SCOPE:  ~10 lines, ~30 min, difficulty 2/3
# HINTS:  DataFrame.explain with the "formatted" mode: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html . Run it on the gold aggregate, find the Exchange operator, and write down which key causes it.
# DONE WHEN: pipeline_runs has a row per task per run; note in docs/NOTES.md where the shuffle is
# SEE:    audit.py:log_run, 10_bronze, docs/TODO.md#todo-13
print("TODO-13 not done yet - skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cost by SKU for the last 7 days (TODO-18)

# COMMAND ----------

# TODO-18: Stretch: cost awareness
# WHAT:   query `system.billing.usage` for last 7 days (DBUs by sku); if not accessible on Free Edition, record the error
# WHY:    Compute spend is a metric like row counts. The billing system table exposes DBUs per SKU.
# PRODUCT: Cost per pipeline becomes visible. If your tier cannot query it, that gap is documented.
# SCOPE:  ~8 lines SQL, ~20 min, difficulty 2/3
# HINTS:  Billing system table: https://docs.databricks.com/aws/en/admin/system-tables/billing . Free Edition limits: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations . Wrap spark.sql in try/except and print the error if the table is not accessible.
# DONE WHEN: query returns rows OR error documented in docs/NOTES.md
# SEE:    60_monitoring, docs/TODO.md#todo-18
print("TODO-18 not done yet - skipped")
