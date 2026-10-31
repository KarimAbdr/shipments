"""Tests for the silver transforms. Needs a Spark session (see tests/conftest.py)."""

import pytest


# TODO-05: Test dedup
# WHAT:   test with 3 events / 2 shipments
# WHY:    Dedup is the easiest place to keep the wrong row, so a test pins down the rule.
# PRODUCT: Protects the "latest status wins" promise from regressions.
# SCOPE:  ~20 lines, ~15 min, difficulty 1/3
# HINTS:  pytest fixtures: https://docs.pytest.org/en/stable/how-to/fixtures.html . Build a 3-row DataFrame with spark.createDataFrame, run dedup_latest, and assert on the shipment ids and statuses that come back. TODO-04 must be done first.
# DONE WHEN: test passes
# SEE:    tests/test_quality.py:test_missing_supplier_rule, docs/TODO.md#todo-05
def test_dedup_latest_keeps_newest_event():
    pytest.skip("TODO-05: see docs/TODO.md#todo-05")
