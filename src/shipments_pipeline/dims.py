"""Dimension tables: slowly changing reference data, here the supplier dimension.

A dimension describes things (suppliers) that the fact rows (shipments) point to.
The SCD1 upsert below is the reference MERGE pattern for the whole project.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from shipments_pipeline import generator

SUPPLIER_SCHEMA = StructType(
    [
        StructField("supplier_id", StringType(), False),
        StructField("supplier_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("rating", IntegerType(), True),
    ]
)


def supplier_df(spark: SparkSession, version: int) -> DataFrame:
    """Supplier rows as a DataFrame with an explicit schema and an `updated_at` timestamp.

    An explicit schema matters: without it Spark guesses the types, and `rating` could
    come out as the wrong type.
    """
    rows = [
        (r["supplier_id"], r["supplier_name"], r["country"], r["rating"])
        for r in generator.supplier_rows(version)
    ]
    return spark.createDataFrame(rows, schema=SUPPLIER_SCHEMA).withColumn(
        "updated_at", F.current_timestamp()
    )


def upsert_dim_supplier(spark: SparkSession, source: DataFrame, target_table: str) -> None:
    """SCD1 upsert: the dimension holds one row per supplier, and changes overwrite the old values.

    SCD1 means "slowly changing dimension, type 1": no history is kept. Use TODO-16 for
    history (SCD2).

    First run: the table does not exist, so the source becomes the table.
    Later runs: MERGE the source into the table, matching on supplier_id.
    """
    if not spark.catalog.tableExists(target_table):
        source.write.saveAsTable(target_table)
        return

    # MERGE reads from a SQL name, so register the DataFrame as a temporary view.
    source.createOrReplaceTempView("supplier_updates")
    spark.sql(
        f"""
        MERGE INTO {target_table} AS t
        USING supplier_updates AS s
        ON t.supplier_id = s.supplier_id
        -- Clause 1: the supplier exists AND something changed -> overwrite it.
        -- <=> is the NULL-safe "equal" operator, so two NULLs count as equal.
        WHEN MATCHED AND NOT (
              t.supplier_name <=> s.supplier_name
          AND t.country       <=> s.country
          AND t.rating        <=> s.rating
        ) THEN UPDATE SET
          t.supplier_name = s.supplier_name,
          t.country       = s.country,
          t.rating        = s.rating,
          t.updated_at    = s.updated_at
        -- Clause 2: the supplier is new -> insert it.
        WHEN NOT MATCHED THEN INSERT (supplier_id, supplier_name, country, rating, updated_at)
          VALUES (s.supplier_id, s.supplier_name, s.country, s.rating, s.updated_at)
        """
    )


# TODO-16: Stretch: SCD2 dim
# WHAT:   `upsert_dim_supplier_scd2`; write to a NEW table `dim_supplier_scd2` (the SCD1 `dim_supplier` lacks valid_from/valid_to/is_current) and add a cell in notebooks/20_silver.py that runs v1 then v2 into it
# WHY:    SCD1 overwrites history. SCD2 keeps every version with validity dates, which audits usually ask for.
# PRODUCT: Reports can answer "what was this supplier's rating on date X?"
# SCOPE:  ~35 lines, ~60 min, difficulty 3/3
# HINTS:  Start from upsert_dim_supplier. Add the columns valid_from, valid_to and is_current. Think about how one MERGE can both close the old row and insert the new version; read the MERGE docs for that pattern. Docs: https://docs.databricks.com/aws/en/delta/merge
# DONE WHEN: dim_supplier_scd2 after v1 then v2: 28 rows, 25 with is_current=true, SUP001 has 2 rows
# SEE:    dims.py:upsert_dim_supplier, docs/TODO.md#todo-16
def upsert_dim_supplier_scd2(spark: SparkSession, source: DataFrame, target_table: str) -> None:
    raise NotImplementedError("TODO-16: see docs/TODO.md#todo-16")
