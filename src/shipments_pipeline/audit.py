"""Audit log: one row per task per run in the pipeline_runs table.

Why: "did the job run, and how many rows did it touch?" should be answerable with a SQL query,
not by reading notebook output.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

RUN_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("task", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("status", StringType(), False),
    ]
)


def log_run(
    spark: SparkSession,
    table_fqn: str,
    run_id: str,
    task: str,
    target_table: str,
    rows: int,
    status: str = "SUCCESS",
) -> None:
    """Append one audit row. The table is created on the first call (mode "append")."""
    row = [(run_id, task, target_table, int(rows), status)]
    log_df = spark.createDataFrame(row, schema=RUN_LOG_SCHEMA).withColumn("logged_at", F.current_timestamp())
    log_df.write.mode("append").saveAsTable(table_fqn)
