# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Generate synthetic shipment batches
# MAGIC
# MAGIC **Purpose:** write deterministic daily CSV files into the Unity Catalog volume `raw`.
# MAGIC Day 1 is the first load. Day 2 has status updates and new shipments. Day 3 adds a `carrier` column.
# MAGIC
# MAGIC **Run on:** Serverless
# MAGIC
# MAGIC Widget `days` takes a comma list such as `1` or `1,2,3`. Running a day again rewrites its file with the same content.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from shipments_pipeline import config, generator

cfg = config.from_widgets(dbutils)
run_id = config.get_widget(dbutils, "run_id", "manual")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the schema and the volume
# MAGIC
# MAGIC A **volume** is a Unity Catalog location for files. The CSV batches live there, not in a table yet.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{cfg.volume}")
print("Volume ready:", cfg.volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write the requested daily batches
# MAGIC
# MAGIC `dbutils.fs.put` writes one text file. `overwrite=True` makes the step safe to repeat.

# COMMAND ----------

days = [int(d) for d in config.get_widget(dbutils, "days", "1").split(",") if d.strip()]
for day in days:
    path = f"{cfg.volume_path}/{generator.batch_filename(day)}"
    dbutils.fs.put(path, generator.batch_csv(day), overwrite=True)
    print(f"wrote day {day}: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Look at what is in the volume

# COMMAND ----------

display(dbutils.fs.ls(cfg.volume_path))
