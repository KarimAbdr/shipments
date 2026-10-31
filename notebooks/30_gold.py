# Databricks notebook source
# MAGIC %md
# MAGIC # 30 — Gold: aggregates for reporting
# MAGIC
# MAGIC **Purpose:** build the gold tables from silver. `gold_region_part` is the reference aggregate.
# MAGIC `gold_supplier_scorecard` is TODO-06 (Spark SQL with a join and a window function).
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
# MAGIC ## 1. Reference aggregate: region x part type

# COMMAND ----------

from shipments_pipeline import gold

silver_table = cfg.table(config.SILVER)
region_part = gold.build_region_part(spark.table(silver_table))
region_part.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table(config.GOLD_REGION_PART))
display(spark.table(cfg.table(config.GOLD_REGION_PART)).orderBy("region", "part_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Supplier scorecard (TODO-06)
# MAGIC
# MAGIC The cell is skipped until `build_supplier_scorecard` is written.

# COMMAND ----------

# TODO-06: Supplier scorecard (SQL)
# WHAT:   `build_supplier_scorecard`
# WHY:    Ranking suppliers is a join plus a window function. Writing it in SQL shows the same logic in a second style.
# PRODUCT: Procurement gets a ranked supplier list to follow up on delays.
# SCOPE:  ~25 lines, ~30 min, difficulty 2/3
# HINTS:  Implement gold.build_supplier_scorecard with spark.sql and a WITH (CTE) block. Aggregate from silver and join the dim onto it (silver-side join); with supplier_version=2 the dim has 25 rows, so a dim-side join would yield 25 rows instead of 24. Docs: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions
# DONE WHEN: gold_supplier_scorecard has 24 rows, delay_rank 1 = highest delayed_pct
# SEE:    gold.py:build_region_part, dims.py (SQL style), docs/TODO.md#todo-06
dim_table = cfg.table(config.DIM_SUPPLIER)
try:
    scorecard = gold.build_supplier_scorecard(spark, silver_table, dim_table)
    scorecard.write.mode("overwrite").saveAsTable(cfg.table(config.GOLD_SUPPLIER_SCORECARD))
    display(scorecard)
except NotImplementedError:
    print("TODO-06 not done yet - skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Broadcast join (TODO-17)

# COMMAND ----------

# TODO-17: Stretch: broadcast join
# WHAT:   join silver with dim_supplier with and without `F.broadcast`, compare `explain()` (BroadcastHashJoin vs SortMergeJoin), read about AQE
# WHY:    A small table can be copied to every worker, which avoids a shuffle. Knowing when Spark does this is a common interview topic.
# PRODUCT: Cheaper gold runs when a dimension is small.
# SCOPE:  ~10 lines, ~30 min, difficulty 3/3
# HINTS:  pyspark.sql.functions.broadcast: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html . DataFrame.explain: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html . AQE: https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
# DONE WHEN: written comparison in docs/NOTES.md
# SEE:    30_gold, docs/TODO.md#todo-17
print("TODO-17 not done yet - skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Audit (TODO-13)

# COMMAND ----------

# TODO-13: Audit everywhere + query plan
# WHAT:   call `log_run` in silver/gold/dq tasks; `explain("formatted")` gold query, find Exchange (shuffle) and explain why
# WHY:    Row counts per task make runs comparable. The plan shows where Spark moves data between machines (a shuffle, which is expensive).
# PRODUCT: Ops can see row counts per run and find the slow step.
# SCOPE:  ~10 lines, ~30 min, difficulty 2/3
# HINTS:  audit.log_run (already in audit.py; 10_bronze shows the call). Call it at the end of this cell with the task name "gold" and the row count. Docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
# DONE WHEN: pipeline_runs has a row per task per run; note in docs/NOTES.md where the shuffle is
# SEE:    audit.py:log_run, 10_bronze, docs/TODO.md#todo-13
print("TODO-13 not done yet - skipped")
