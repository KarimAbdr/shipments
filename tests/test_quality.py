"""Tests for the quality rules. The reference tests need a Spark session (see tests/conftest.py)."""

from datetime import date
from decimal import Decimal

import pytest

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
def test_invalid_quantity_rule():
    pytest.skip("TODO-03: see docs/TODO.md#todo-03")


def test_invalid_unit_cost_rule():
    pytest.skip("TODO-03: see docs/TODO.md#todo-03")


def test_invalid_date_rule():
    pytest.skip("TODO-03: see docs/TODO.md#todo-03")


def test_invalid_status_rule():
    pytest.skip("TODO-03: see docs/TODO.md#todo-03")
