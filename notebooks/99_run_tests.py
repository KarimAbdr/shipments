# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Run the pytest suite on Databricks
# MAGIC
# MAGIC **Purpose:** run `tests/` inside a Databricks serverless notebook, so you need no local Java.
# MAGIC The conftest uses the notebook's own Spark session when `DATABRICKS_RUNTIME_VERSION` is set.
# MAGIC
# MAGIC **Run on:** Serverless
# MAGIC
# MAGIC Databricks path not yet verified by the scaffold author. Expected to work; confirm on Free Edition and tick it in docs/TODO.md.

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
# MAGIC ## 1. Install pytest
# MAGIC
# MAGIC `%pip` restarts the Python process, so the next cell imports everything it needs again.

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run the tests
# MAGIC
# MAGIC Skipped tests (the TODO placeholders) count as a pass here. The job fails only when a test fails.

# COMMAND ----------

import os, sys
import pytest

sys.dont_write_bytecode = True
rc = pytest.main(["-q", "-p", "no:cacheprovider", os.path.abspath(os.path.join(os.getcwd(), "..", "tests"))])
assert rc == 0, f"pytest finished with exit code {rc}"
print("pytest exit code:", rc)
