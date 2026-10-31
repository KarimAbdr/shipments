# Databricks notebook source
# MAGIC %md
# MAGIC # 40 — Quality checks
# MAGIC
# MAGIC **Purpose:** run data checks on bronze, silver and quarantine. Save every result to `dq_results`.
# MAGIC The reconciliation check is the reference. TODO-09 adds more checks and makes a critical failure stop the job.
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
# MAGIC ## 1. Reference check: no shipment lost

# COMMAND ----------

from shipments_pipeline import checks

bronze_df = spark.table(cfg.table(config.BRONZE))
silver_df = spark.table(cfg.table(config.SILVER))
quarantine_df = spark.table(cfg.table(config.QUARANTINE))

results = [checks.check_no_shipment_lost(bronze_df, silver_df, quarantine_df)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. More checks (TODO-09)

# COMMAND ----------

# TODO-09: More DQ checks
# WHAT:   3 functions
# WHY:    A check that only prints is not a control. A critical failure must stop the job.
# PRODUCT: Bad data stops the pipeline visibly and leaves an audit trail in dq_results.
# SCOPE:  ~25 lines, ~30 min, difficulty 2/3
# HINTS:  Implement check_unique_key, check_not_empty and raise_on_critical in checks.py. No link on the approved list fits this TODO; look up DataFrame.groupBy in the PySpark API reference.
# DONE WHEN: dq_results has 3+ rows per run; temporarily break a check → notebook fails with RuntimeError
# SEE:    checks.py:check_no_shipment_lost, docs/TODO.md#todo-09
try:
    results.append(checks.check_unique_key(silver_df, "shipment_id", "silver_unique_shipment_id"))
    results.append(checks.check_not_empty(silver_df, "silver_not_empty"))
except NotImplementedError:
    print("TODO-09 not done yet - skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save the results
# MAGIC
# MAGIC Results are written before any failure is raised, so a failing run still leaves evidence.

# COMMAND ----------

dq_df = checks.results_to_df(spark, results, run_id)
dq_df.write.mode("append").saveAsTable(cfg.table(config.DQ_RESULTS))
display(dq_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fail the job on a critical check (TODO-09)

# COMMAND ----------

# TODO-09: More DQ checks
# WHAT:   3 functions
# WHY:    A check that only prints is not a control. A critical failure must stop the job.
# PRODUCT: Bad data stops the pipeline visibly and leaves an audit trail in dq_results.
# SCOPE:  ~25 lines, ~30 min, difficulty 2/3
# HINTS:  Implement checks.raise_on_critical so it raises RuntimeError when a critical check failed. No link on the approved list fits this TODO.
# DONE WHEN: dq_results has 3+ rows per run; temporarily break a check → notebook fails with RuntimeError
# SEE:    checks.py:check_no_shipment_lost, docs/TODO.md#todo-09
try:
    checks.raise_on_critical(results)
except NotImplementedError:
    print("TODO-09 not done yet - skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Audit (TODO-13)

# COMMAND ----------

# TODO-13: Audit everywhere + query plan
# WHAT:   call `log_run` in silver/gold/dq tasks; `explain("formatted")` gold query, find Exchange (shuffle) and explain why
# WHY:    Row counts per task make runs comparable. The plan shows where Spark moves data between machines (a shuffle, which is expensive).
# PRODUCT: Ops can see row counts per run and find the slow step.
# SCOPE:  ~10 lines, ~30 min, difficulty 2/3
# HINTS:  audit.log_run (already in audit.py; 10_bronze shows the call). Call it at the end of this cell with the task name "dq" and the number of results. Docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
# DONE WHEN: pipeline_runs has a row per task per run; note in docs/NOTES.md where the shuffle is
# SEE:    audit.py:log_run, 10_bronze, docs/TODO.md#todo-13
print("TODO-13 not done yet - skipped")
