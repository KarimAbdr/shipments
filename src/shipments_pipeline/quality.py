"""Data quality rules and the quarantine split.

Idea: each rule is a column expression. It returns a short reason text when a row is bad
and NULL when the row is fine. Concatenating all the reasons gives one `_dq_reasons`
string per row. Rows with an empty string are valid and go to silver. The others go to
quarantine together with their reasons. No row is silently dropped.
"""

from pyspark.sql import Column, DataFrame, functions as F

REASONS_COL = "_dq_reasons"
VALID_STATUSES = ("IN_TRANSIT", "DELIVERED", "DELAYED")

# Target type for each raw column that needs typing. Bronze keeps all of them as text.
_CAST_RULES = {
    "event_date": "DATE",
    "event_ts": "TIMESTAMP",
    "quantity": "INT",
    "unit_cost_usd": "DECIMAL(12,2)",
}


def cast_types(df: DataFrame) -> DataFrame:
    """Cast raw text columns to real types. A value that cannot be converted becomes NULL.

    `try_cast` returns NULL where `cast` would raise an error. Serverless runs in ANSI
    mode, where a failing `cast` stops the job. The quality rules then flag those NULLs.
    Columns that are not in the DataFrame are skipped. Extra columns (such as `carrier`)
    are left untouched.
    """
    for name, sql_type in _CAST_RULES.items():
        if name in df.columns:
            df = df.withColumn(name, F.expr(f"try_cast({name} AS {sql_type})"))
    return df


def rule_missing_supplier() -> Column:
    """Reason `missing_supplier` when supplier_id is NULL or only spaces."""
    return F.when(
        F.col("supplier_id").isNull() | (F.trim(F.col("supplier_id")) == ""),
        F.lit("missing_supplier"),
    )


# TODO-02: Remaining quality rules
# WHAT:   implement 4 rule functions
# WHY:    Each rule is one column expression; null handling is where most bugs hide.
# PRODUCT: Bad quantity, cost, date and status rows get a reason code instead of reaching gold reports.
# SCOPE:  ~12 lines, ~25 min, difficulty 1/3
# HINTS:  F.when, Column.isNull, F.trim, Column.isin, F.lit. A comparison with NULL gives NULL, so test for NULL explicitly. The date rule only checks for NULL because cast_types already ran try_cast. Docs: https://docs.databricks.com/aws/en/sql/language-manual/functions/try_cast
# DONE WHEN: after rerun of 20_silver (day 1): quarantine 45 rows, 5 distinct reason values; after TODO-03, the 4 rule tests pass (not skipped)
# SEE:    quality.py:rule_missing_supplier, docs/TODO.md#todo-02
def rule_invalid_quantity() -> Column:
    raise NotImplementedError("TODO-02: see docs/TODO.md#todo-02")


def rule_invalid_unit_cost() -> Column:
    raise NotImplementedError("TODO-02: see docs/TODO.md#todo-02")


def rule_invalid_date() -> Column:
    raise NotImplementedError("TODO-02: see docs/TODO.md#todo-02")


def rule_invalid_status() -> Column:
    raise NotImplementedError("TODO-02: see docs/TODO.md#todo-02")


ALL_RULES = [
    rule_missing_supplier,
    rule_invalid_quantity,
    rule_invalid_unit_cost,
    rule_invalid_date,
    rule_invalid_status,
]


def active_rules() -> list[Column]:
    """Build every rule that is implemented. A rule that raises NotImplementedError is skipped.

    This is what lets the pipeline run on day 1 before any TODO is done.
    """
    rules: list[Column] = []
    for fn in ALL_RULES:
        try:
            rules.append(fn())
        except NotImplementedError:
            print(f"skipping {fn.__name__}: not implemented yet")
    return rules


def with_quality_flags(df: DataFrame) -> DataFrame:
    """Add `_dq_reasons`: the reasons joined with '; ', or an empty string when no rule fires."""
    return df.withColumn(REASONS_COL, F.concat_ws("; ", *active_rules()))


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid, quarantine). Valid rows lose the helper column; quarantine keeps it."""
    flagged = with_quality_flags(df)
    valid = flagged.filter(F.col(REASONS_COL) == "").drop(REASONS_COL)
    quarantine = flagged.filter(F.col(REASONS_COL) != "")
    return valid, quarantine
