"""Silver layer: one clean, current row per shipment.

Bronze keeps every event. Silver keeps the latest valid state of each shipment.
The functions here work on DataFrames. Notebook 20_silver does the reads and writes.
"""

from pyspark.sql import DataFrame, SparkSession


# TODO-04: Dedup latest event
# WHAT:   `dedup_latest` with Window + row_number
# WHY:    Source systems send several events for one shipment. Silver must keep one current row per key.
# PRODUCT: Dashboards count each shipment once and show its latest known status.
# SCOPE:  ~8 lines, ~25 min, difficulty 2/3
# HINTS:  Window.partitionBy on shipment_id, orderBy on event_ts descending (event_id descending as tie-break), F.row_number().over(...), then keep row number 1. Docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html and https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions
# DONE WHEN: silver 1,200 rows, `count(distinct shipment_id) = count(*)`; SHP000001 status DELIVERED
# SEE:    quality.py:split_valid_quarantine (DataFrame style), docs/TODO.md#todo-04
def dedup_latest(df: DataFrame) -> DataFrame:
    raise NotImplementedError("TODO-04: see docs/TODO.md#todo-04")


# TODO-08: Silver MERGE upsert
# WHAT:   `merge_into_silver`
# WHY:    A status update must change an existing row, not add a new one. MERGE is the Delta tool for upserts.
# PRODUCT: Silver always shows the latest status per shipment without rebuilding the whole table each day.
# SCOPE:  ~20 lines, ~40 min, difficulty 2/3
# HINTS:  spark.catalog.tableExists and saveAsTable for the first load (same pattern as dims.upsert_dim_supplier). For the upsert, read the MERGE reference in dims.upsert_dim_supplier and the MERGE docs. The WHEN MATCHED clause accepts an extra AND condition, which is where the timestamp guard goes. Use a temp view as the MERGE source. Docs: https://docs.databricks.com/aws/en/delta/merge
# DONE WHEN: 00 days=2 → 10 → 20: silver 1,300, quarantine 50; SHP000150 status DELIVERED; rerun 20 → no change
# SEE:    dims.py:upsert_dim_supplier, docs/TODO.md#todo-08
def merge_into_silver(spark: SparkSession, updates: DataFrame, target_table: str) -> None:
    raise NotImplementedError("TODO-08: see docs/TODO.md#todo-08")


def unprocessed_bronze(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    quarantine_table: str,
) -> DataFrame:
    """Bronze rows from files that silver and quarantine have not seen yet.

    The check works per file: a file is either loaded or not. Re-running the notebook
    therefore does not process the same file again. A missing target table counts as empty.

    Why the `s.event_ts > t.event_ts` guard in the MERGE still matters: if old events are
    ever replayed (for example after a manual reset), the MERGE only overwrites a shipment
    when the incoming event is newer. Replaying old data is then harmless.
    """
    bronze = spark.table(bronze_table)
    seen_files: DataFrame | None = None
    for table in (silver_table, quarantine_table):
        if spark.catalog.tableExists(table):
            files = spark.table(table).select("_source_file").distinct()
            seen_files = files if seen_files is None else seen_files.union(files)
    if seen_files is None:
        return bronze
    return bronze.join(seen_files, on="_source_file", how="left_anti")


# (TODO-10 modifies merge_into_silver above and the writes in notebooks/10_bronze.py + 20_silver.py)
# TODO-10: Schema evolution
# WHAT:   bronze append needs `mergeSchema`, the quarantine append in 20_silver needs `mergeSchema`, and the silver MERGE must use `UPDATE SET *` / `INSERT *` with schema evolution (`MERGE WITH SCHEMA EVOLUTION INTO ...`)
# WHY:    Upstream systems add columns. You decide whether to accept them (evolution) or reject them (enforcement).
# PRODUCT: New carrier data flows in without a manual schema migration; older rows show NULL.
# SCOPE:  ~7 lines, ~30 min, difficulty 2/3
# HINTS:  Delta schema evolution docs (named in WHAT). Reproduce the day-3 error first. The older session-wide alternative is spark.databricks.delta.schema.autoMerge.enabled; prefer the per-statement form. Docs: https://docs.databricks.com/aws/en/delta/update-schema
# DONE WHEN: 00 days=3 → 10 → 20: bronze 1,665, silver 1,350, `carrier` column exists, NULL for older rows
# SEE:    docs/CONCEPTS.md#schema-enforcement-vs-evolution, docs/TODO.md#todo-10
