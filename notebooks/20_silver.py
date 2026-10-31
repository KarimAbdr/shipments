# Databricks notebook source
# MAGIC %md
# MAGIC # 20 — Silver: quality rules, quarantine, dedup, dimension
# MAGIC
# MAGIC **Purpose:** check every bronze row with the quality rules. Valid rows go to `silver_shipments`, the others to `quarantine_shipments` with their reasons.
# MAGIC The supplier dimension is refreshed first (SCD1 MERGE reference in `dims.py`).
# MAGIC
# MAGIC **Run on:** Serverless
# MAGIC
# MAGIC **Two paths:**
# MAGIC - `USE_MERGE = False` (default): rebuild silver and quarantine from ALL bronze rows. This works on day 1 with every TODO still open.
# MAGIC - `USE_MERGE = True`: only process new files and MERGE them into silver. Needs TODO-08.

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
# MAGIC ## 1. Supplier dimension (SCD1 MERGE reference)
# MAGIC
# MAGIC Version 1 creates `dim_supplier`. Version 2 updates three ratings and adds one supplier, so you can watch the MERGE work.

# COMMAND ----------

from shipments_pipeline import dims

supplier_version = int(config.get_widget(dbutils, "supplier_version", "1"))
dim_table = cfg.table(config.DIM_SUPPLIER)
dims.upsert_dim_supplier(spark, dims.supplier_df(spark, supplier_version), dim_table)
print(f"dim_supplier rows after version {supplier_version}:", spark.table(dim_table).count())

# COMMAND ----------

# TODO-16: Stretch: SCD2 dim
# WHAT:   `upsert_dim_supplier_scd2`; write to a NEW table `dim_supplier_scd2` (the SCD1 `dim_supplier` lacks valid_from/valid_to/is_current) and add a cell in notebooks/20_silver.py that runs v1 then v2 into it
# WHY:    SCD1 overwrites history. SCD2 keeps every version with validity dates, which audits usually ask for.
# PRODUCT: Reports can answer "what was this supplier's rating on date X?"
# SCOPE:  ~35 lines, ~60 min, difficulty 3/3
# HINTS:  Start from upsert_dim_supplier. Add the columns valid_from, valid_to and is_current. Think about how one MERGE can both close the old row and insert the new version; read the MERGE docs for that pattern. Docs: https://docs.databricks.com/aws/en/delta/merge
# DONE WHEN: dim_supplier_scd2 after v1 then v2: 28 rows, 25 with is_current=true, SUP001 has 2 rows
# SEE:    dims.py:upsert_dim_supplier, docs/TODO.md#todo-16
print("TODO-16 not done yet - skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Choose the path
# MAGIC
# MAGIC Keep `USE_MERGE = False` until TODO-08 is done. Then set it to `True`.

# COMMAND ----------

# TODO-08: Silver MERGE upsert
# WHAT:   `merge_into_silver`
# WHY:    A status update must change an existing row, not add a new one. MERGE is the Delta tool for upserts.
# PRODUCT: Silver always shows the latest status per shipment without rebuilding the whole table each day.
# SCOPE:  ~20 lines, ~40 min, difficulty 2/3
# HINTS:  Read silver.merge_into_silver and dims.upsert_dim_supplier. Flip the flag only when merge_into_silver works. Docs: https://docs.databricks.com/aws/en/delta/merge
# DONE WHEN: (with TODO-02 and TODO-04 done) 00 days=2 → 10 → 20: silver 1,300, quarantine 50; SHP000150 status DELIVERED; rerun 20 → no change
# SEE:    dims.py:upsert_dim_supplier, docs/TODO.md#todo-08
USE_MERGE = False  # flip to True when TODO-08 is done

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quality rules and quarantine split
# MAGIC
# MAGIC `cast_types` turns text into numbers and dates (bad values become NULL). `split_valid_quarantine` then applies the rules.

# COMMAND ----------

from shipments_pipeline import quality, silver

bronze_table = cfg.table(config.BRONZE)
silver_table = cfg.table(config.SILVER)
quarantine_table = cfg.table(config.QUARANTINE)

if USE_MERGE:
    # Incremental: only bronze files that silver and quarantine have not seen yet.
    incoming = silver.unprocessed_bronze(spark, bronze_table, silver_table, quarantine_table)
else:
    # Reference path: rebuild from ALL bronze rows.
    incoming = spark.table(bronze_table)

typed = quality.cast_types(incoming)
valid, quarantine = quality.split_valid_quarantine(typed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dedup: one row per shipment
# MAGIC
# MAGIC If TODO-04 is not done, this step is skipped with a warning and silver can contain duplicate shipments.

# COMMAND ----------

def _dedup(df):
    """Keep the latest event per shipment (TODO-04). Returns the input unchanged until TODO-04 is done."""
    try:
        return silver.dedup_latest(df)
    except NotImplementedError:
        print("WARNING: TODO-04 not done yet - skipping dedup. Silver may contain duplicate shipments.")
        return df

clean = _dedup(valid)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write silver and quarantine
# MAGIC
# MAGIC Full refresh (`USE_MERGE = False`): both tables are overwritten.
# MAGIC Incremental (`USE_MERGE = True`): silver is MERGEd and quarantine is appended.

# COMMAND ----------

if USE_MERGE:
    silver.merge_into_silver(spark, clean, silver_table)
    # TODO-10: also add mergeSchema here (see docs/TODO.md#todo-10)
    quarantine.write.mode("append").saveAsTable(quarantine_table)
else:
    clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
    quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)

print("SILVER rows:", spark.table(silver_table).count())
print("QUARANTINE rows:", spark.table(quarantine_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Why rows were quarantined

# COMMAND ----------

display(spark.table(quarantine_table).groupBy("_dq_reasons").count().orderBy("_dq_reasons"))

# COMMAND ----------

# TODO-13: Audit everywhere + query plan
# WHAT:   call `log_run` in silver/gold/dq tasks; `explain("formatted")` gold query, find Exchange (shuffle) and explain why
# WHY:    Row counts per task make runs comparable. The plan shows where Spark moves data between machines (a shuffle, which is expensive).
# PRODUCT: Ops can see row counts per run and find the slow step.
# SCOPE:  ~10 lines, ~30 min, difficulty 2/3
# HINTS:  audit.log_run (already in audit.py; 10_bronze shows the call). Call it at the end of this cell with the task name "silver" and the row counts. The plan part is in 60_monitoring. Docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
# DONE WHEN: pipeline_runs has a row per task per run; note in docs/NOTES.md where the shuffle is
# SEE:    audit.py:log_run, 10_bronze, docs/TODO.md#todo-13
print("TODO-13 not done yet - skipped")
