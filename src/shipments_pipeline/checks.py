"""Data quality checks that run after the pipeline and record their results.

A check looks at the data and returns a CheckResult. Results are written to the dq_results
table, so every run leaves evidence. A failed "critical" check should stop the job.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one check. severity is "critical" (stops the job) or "warning" (only recorded)."""

    name: str
    severity: str
    passed: bool
    observed: str
    expected: str


def check_no_shipment_lost(bronze: DataFrame, silver: DataFrame, quarantine: DataFrame) -> CheckResult:
    """Reconciliation check: every shipment in bronze must end up in silver or in quarantine.

    Counts distinct shipment ids on both sides. If the numbers differ, rows were lost
    somewhere between the layers.
    """
    expected = bronze.select("shipment_id").distinct().count()
    observed = silver.select("shipment_id").union(quarantine.select("shipment_id")).distinct().count()
    return CheckResult(
        name="no_shipment_lost",
        severity="critical",
        passed=observed == expected,
        observed=str(observed),
        expected=str(expected),
    )


RESULT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("observed", StringType(), True),
        StructField("expected", StringType(), True),
    ]
)


def results_to_df(spark: SparkSession, results: list[CheckResult], run_id: str) -> DataFrame:
    """Turn check results into a DataFrame. Adds `run_id` and `checked_at` so runs can be compared."""
    rows = [(run_id, r.name, r.severity, r.passed, r.observed, r.expected) for r in results]
    return spark.createDataFrame(rows, schema=RESULT_SCHEMA).withColumn("checked_at", F.current_timestamp())


# TODO-09: More DQ checks
# WHAT:   3 functions
# WHY:    A check that only prints is not a control. A critical failure must stop the job.
# PRODUCT: Bad data stops the pipeline visibly and leaves an audit trail in dq_results.
# SCOPE:  ~25 lines, ~30 min, difficulty 2/3
# HINTS:  Use the CheckResult dataclass above. For duplicates: groupBy the key, count, keep groups with count > 1. For not-empty: DataFrame.isEmpty() or count(). raise_on_critical: build a RuntimeError message that lists the failed critical names. No link on the approved list fits this TODO; look up DataFrame.groupBy in the PySpark API reference.
# DONE WHEN: dq_results has 3+ rows per run; temporarily break a check → notebook fails with RuntimeError
# SEE:    checks.py:check_no_shipment_lost, docs/TODO.md#todo-09
def check_unique_key(df: DataFrame, key: str, name: str) -> CheckResult:
    raise NotImplementedError("TODO-09: see docs/TODO.md#todo-09")


def check_not_empty(df: DataFrame, name: str) -> CheckResult:
    raise NotImplementedError("TODO-09: see docs/TODO.md#todo-09")


def raise_on_critical(results: list[CheckResult]) -> None:
    raise NotImplementedError("TODO-09: see docs/TODO.md#todo-09")
