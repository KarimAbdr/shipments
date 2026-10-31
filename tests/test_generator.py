"""Pure-Python tests for the synthetic data contract. No Spark needed.

The numbers asserted here are the contract that the docs and the notebooks rely on
(SCAFFOLD_SPEC section 1). If one of them fails, the generator changed.
"""

import csv
import io

import pytest

from shipments_pipeline import generator as g


def _csv_rows(day: int) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(g.batch_csv(day))))


def test_day1_has_1260_rows():
    assert len(g.batch_rows(1)) == 1260


def test_day1_has_45_rule_breaking_rows():
    bad = [r for r in g.batch_rows(1) if r["shipment_id"].startswith("BAD-")]
    assert len(bad) == 45


def test_day1_has_12_missing_suppliers():
    assert sum(1 for r in g.batch_rows(1) if r["supplier_id"] == "") == 12


def test_day1_has_1245_distinct_shipments():
    assert len({r["shipment_id"] for r in g.batch_rows(1)}) == 1245


def test_day1_csv_header_is_the_base_columns():
    rows = _csv_rows(1)
    assert list(rows[0].keys()) == list(g.COLUMNS)
    assert len(rows) == 1260


def test_day2_has_305_rows_and_5_lost_statuses():
    rows = g.batch_rows(2)
    assert len(rows) == 305
    assert sum(1 for r in rows if r["status"] == "LOST") == 5


def test_day3_has_100_rows():
    assert len(g.batch_rows(3)) == 100


def test_day3_csv_header_has_carrier_as_last_column():
    header = g.batch_csv(3).splitlines()[0].split(",")
    assert header[-1] == "carrier"
    assert len(header) == len(g.COLUMNS) + 1


def test_day1_and_day2_csv_have_no_carrier_column():
    assert "carrier" not in g.batch_csv(1).splitlines()[0]
    assert "carrier" not in g.batch_csv(2).splitlines()[0]


def test_valid_distinct_shipments_across_three_days_is_1350():
    ids = {
        r["shipment_id"]
        for day in (1, 2, 3)
        for r in g.batch_rows(day)
        if not r["shipment_id"].startswith("BAD-")
    }
    assert len(ids) == 1350


def test_supplier_v1_has_24_rows():
    assert len(g.supplier_rows(1)) == 24


def test_supplier_v2_has_25_rows_and_updated_ratings():
    v2 = {r["supplier_id"]: r for r in g.supplier_rows(2)}
    assert len(v2) == 25
    assert v2["SUP001"]["rating"] == 5
    assert v2["SUP025"]["supplier_name"] == "Supplier 25"


def test_unknown_day_raises_value_error():
    with pytest.raises(ValueError):
        g.batch_rows(4)


def test_unknown_supplier_version_raises_value_error():
    with pytest.raises(ValueError):
        g.supplier_rows(3)


def test_generation_is_deterministic():
    assert g.batch_csv(1) == g.batch_csv(1)
    assert g.batch_csv(3) == g.batch_csv(3)
