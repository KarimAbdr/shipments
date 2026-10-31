"""Bronze layer: raw batches land here as text, plus two metadata columns.

Bronze is a faithful copy of the source files. Nothing is cleaned or typed here, so a bad
value stays visible until silver and quarantine decide what to do with it.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


def read_batch(spark: SparkSession, path: str) -> DataFrame:
    """Read one CSV file as strings and add `_source_file` and `_ingested_at`.

    Why strings and no inferSchema: schema inference guesses each column's type from a
    few rows, so one bad value can change the type of a whole column. Reading everything
    as text keeps the data exactly as it arrived. Casting happens in quality.cast_types().

    `_source_file` is the file each row came from. It is how the pipeline knows which
    files were already loaded.
    """
    raw = spark.read.option("header", True).csv(path)
    return raw.withColumn("_source_file", F.col("_metadata.file_path")).withColumn(
        "_ingested_at", F.current_timestamp()
    )


# TODO-07: Incremental bronze
# WHAT:   `list_new_files`
# WHY:    Re-running a notebook must not load the same file twice (idempotency).
# PRODUCT: Daily jobs can be retried safely without double-counting shipments.
# SCOPE:  ~5 lines + notebook, ~30 min, difficulty 2/3
# HINTS:  Plain Python set difference over file names. The notebook already passes file names (basenames) on both sides and reduces `_source_file` to a basename, so compare them directly. Docs: https://docs.databricks.com/aws/en/ingestion/file-metadata-column
# DONE WHEN: running 10_bronze twice keeps bronze at 1,260 (day 1)
# SEE:    bronze.py:read_batch, docs/TODO.md#todo-07
def list_new_files(all_files: list[str], ingested_files: set[str]) -> list[str]:
    raise NotImplementedError("TODO-07: see docs/TODO.md#todo-07")
