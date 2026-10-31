"""Gold layer: aggregates that reports and dashboards read.

Gold tables are small and shaped for one question each. They are rebuilt from silver.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


def build_region_part(silver: DataFrame) -> DataFrame:
    """One row per (region, part_type) with volume, modeled value and delay figures.

    `modeled_inventory_value_usd` is units times unit cost. It is a model, not revenue.
    `delayed_pct` is a percentage rounded to 2 decimals.
    """
    is_delayed = F.when(F.col("status") == "DELAYED", 1).otherwise(0)
    return silver.groupBy("region", "part_type").agg(
        F.count("*").alias("shipment_count"),
        F.sum("quantity").alias("units_total"),
        F.round(F.sum(F.col("quantity") * F.col("unit_cost_usd")), 2).alias("modeled_inventory_value_usd"),
        F.sum(is_delayed).alias("delayed_shipments"),
        F.round(100.0 * F.sum(is_delayed) / F.count("*"), 2).alias("delayed_pct"),
        F.min("event_date").alias("first_event_date"),
        F.max("event_date").alias("last_event_date"),
    )


# TODO-06: Supplier scorecard (SQL)
# WHAT:   `build_supplier_scorecard`
# WHY:    Ranking suppliers is a join plus a window function. Writing it in SQL shows the same logic in a second style.
# PRODUCT: Procurement gets a ranked supplier list to follow up on delays.
# SCOPE:  ~25 lines, ~30 min, difficulty 2/3
# HINTS:  spark.sql with a WITH (CTE) block. Aggregate silver per supplier first, then join the dimension (SCD1 today: one row per supplier). Aggregate from silver and join the dim onto it (silver-side join); with supplier_version=2 the dim has 25 rows, so a dim-side join would yield 25 rows instead of 24. RANK() OVER (ORDER BY delayed_pct DESC) gives the rank. Docs: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions
# DONE WHEN: gold_supplier_scorecard has 24 rows, delay_rank 1 = highest delayed_pct
# SEE:    gold.py:build_region_part, dims.py (SQL style), docs/TODO.md#todo-06
def build_supplier_scorecard(spark: SparkSession, silver_table: str, dim_table: str) -> DataFrame:
    raise NotImplementedError("TODO-06: see docs/TODO.md#todo-06")
