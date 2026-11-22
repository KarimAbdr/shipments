"""Silver layer: one clean, current row per shipment.

Bronze keeps every event. Silver keeps the latest valid state of each shipment.
The functions here work on DataFrames. Notebook 20_silver does the reads and writes.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def dedup_latest(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("shipment_id").orderBy(F.col("event_ts").desc(), F.col("event_id").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

def merge_into_silver(spark: SparkSession, updates: DataFrame, target_table: str) -> None:
    if not spark.catalog.tableExists(target_table):
        updates.write.saveAsTable(target_table)
        return
    updates.createOrReplaceTempView("silver_updates")
    spark.sql(f"""
        MERGE INTO {target_table} AS t
        USING silver_updates AS s
        ON t.shipment_id = s.shipment_id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)



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
