"""Tests for the quality rules. The reference tests need a Spark session (see tests/conftest.py)."""

from datetime import date
from decimal import Decimal

from shipments_pipeline import quality

# Every column that any rule reads. The test DataFrame carries all of them, so the test
# still works after TODO-02 adds the other rules.
QUALITY_SCHEMA = (
    "event_id string, shipment_id string, supplier_id string, quantity int, "
    "unit_cost_usd decimal(12,2), event_date date, status string"
)


def test_missing_supplier_rule(spark):
    rows = [
        ("E1", "SHP000001", None, 5, Decimal("10.00"), date(2026, 10, 1), "DELIVERED"),
        ("E2", "SHP000002", "  ", 5, Decimal("10.00"), date(2026, 10, 1), "DELIVERED"),
        ("E3", "SHP000003", "SUP001", 5, Decimal("10.00"), date(2026, 10, 1), "DELIVERED"),
    ]
    df = spark.createDataFrame(rows, schema=QUALITY_SCHEMA)
    reasons = {r["event_id"]: r[quality.REASONS_COL] for r in quality.with_quality_flags(df).collect()}
    assert reasons == {"E1": "missing_supplier", "E2": "missing_supplier", "E3": ""}


def test_cast_types_invalid_date_becomes_null(spark):
    df = spark.createDataFrame(
        [("E1", "not-a-date"), ("E2", "2026-10-01")],
        schema="event_id string, event_date string",
    )
    result = {r["event_id"]: r["event_date"] for r in quality.cast_types(df).collect()}
    assert result["E1"] is None
    assert result["E2"] == date(2026, 10, 1)


# TODO-03: Tests for rules
# WHAT:   replace 4 skips with real tests
# WHY:    A rule without a test is a rule you will break later without noticing.
# PRODUCT: Quarantine logic is checked by pytest on every change, before it reaches the Databricks job.
# SCOPE:  ~30 lines, ~25 min, difficulty 1/3
# HINTS:  pytest fixtures: https://docs.pytest.org/en/stable/how-to/fixtures.html (the `spark` fixture lives in tests/conftest.py). Copy the shape of test_missing_supplier_rule. For the date test, reuse cast_types as the reference test does.
# DONE WHEN: `pytest -q` shows the 4 tests passed, not skipped
# SEE:    tests/test_quality.py:test_missing_supplier_rule, docs/TODO.md#todo-03
def _reasons(spark, rows, schema=QUALITY_SCHEMA):
    """Run the quality flags on `rows` and return {event_id: _dq_reasons}."""
    df = spark.createDataFrame(rows, schema=schema)
    return {r["event_id"]: r[quality.REASONS_COL] for r in quality.with_quality_flags(df).collect()}


def test_invalid_quantity_rule(spark):
    rows = [
        ("E1", "SHP000001", "SUP001", 0, Decimal("10.00"), date(2026, 10, 1), "DELIVERED"),
        ("E2", "SHP000002", "SUP001", -3, Decimal("10.00"), date(2026, 10, 1), "DELIVERED"),
        ("E3", "SHP000003", "SUP001", None, Decimal("10.00"), date(2026, 10, 1), "DELIVERED"),
        ("E4", "SHP000004", "SUP001", 5, Decimal("10.00"), date(2026, 10, 1), "DELIVERED"),
    ]
    assert _reasons(spark, rows) == {
        "E1": "invalid_quantity",
        "E2": "invalid_quantity",
        "E3": "invalid_quantity",
        "E4": "",
    }


def test_invalid_unit_cost_rule(spark):
    rows = [
        ("E1", "SHP000001", "SUP001", 5, Decimal("-5.00"), date(2026, 10, 1), "DELIVERED"),
        ("E2", "SHP000002", "SUP001", 5, None, date(2026, 10, 1), "DELIVERED"),
        ("E3", "SHP000003", "SUP001", 5, Decimal("0.00"), date(2026, 10, 1), "DELIVERED"),
        ("E4", "SHP000004", "SUP001", 5, Decimal("10.00"), date(2026, 10, 1), "DELIVERED"),
    ]
    # A cost of 0.00 is allowed (rule is "< 0"), so E3 is valid.
    assert _reasons(spark, rows) == {
        "E1": "invalid_unit_cost",
        "E2": "invalid_unit_cost",
        "E3": "",
        "E4": "",
    }


def test_invalid_date_rule(spark):
    # Start from raw strings, like bronze, so the test covers cast_types + the rule together.
    raw_schema = (
        "event_id string, shipment_id string, supplier_id string, quantity string, "
        "unit_cost_usd string, event_date string, status string"
    )
    rows = [
        ("E1", "SHP000001", "SUP001", "5", "10.00", "not-a-date", "DELIVERED"),
        ("E2", "SHP000002", "SUP001", "5", "10.00", None, "DELIVERED"),
        ("E3", "SHP000003", "SUP001", "5", "10.00", "2026-10-01", "DELIVERED"),
    ]
    df = quality.cast_types(spark.createDataFrame(rows, schema=raw_schema))
    reasons = {r["event_id"]: r[quality.REASONS_COL] for r in quality.with_quality_flags(df).collect()}
    assert reasons == {"E1": "invalid_date", "E2": "invalid_date", "E3": ""}


def test_invalid_status_rule(spark):
    rows = [
        ("E1", "SHP000001", "SUP001", 5, Decimal("10.00"), date(2026, 10, 1), "UNKNOWN"),
        ("E2", "SHP000002", "SUP001", 5, Decimal("10.00"), date(2026, 10, 1), None),
        ("E3", "SHP000003", "SUP001", 5, Decimal("10.00"), date(2026, 10, 1), "DELAYED"),
    ]
    assert _reasons(spark, rows) == {"E1": "invalid_status", "E2": "invalid_status", "E3": ""}
