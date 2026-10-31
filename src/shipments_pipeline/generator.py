"""Deterministic synthetic shipment data for the learning pipeline.

Why this module exists: the pipeline needs input files that are identical every time
they are generated. Then a test can assert "exactly 1,260 rows" and you can predict what
each quality rule should catch.

This module is plain Python (no Spark), so it runs anywhere. The numbers below are a
contract: the docs and the tests depend on them (SCAFFOLD_SPEC section 1). Do not change them.
"""

import csv
import io
import random
from datetime import date, datetime, timedelta

# Column order of every raw CSV file. Every value is written as text on purpose:
# bronze keeps raw strings, and typing happens later in quality.cast_types().
COLUMNS = (
    "event_id",
    "shipment_id",
    "supplier_id",
    "part_type",
    "region",
    "event_date",
    "event_ts",
    "quantity",
    "unit_cost_usd",
    "status",
)
# Day 3 adds one column at the end. That schema change is what TODO-10 is about.
COLUMNS_DAY3 = COLUMNS + ("carrier",)

REGIONS = ("Gulf Coast", "Midwest", "Southwest", "Northeast")
PART_TYPES = ("Transformer", "Inverter", "Turbine Component", "Cooling Module")
STATUSES = ("IN_TRANSIT", "DELIVERED", "DELAYED")
CARRIERS = ("FastFreight", "BlueLine", "RailCo")
SEED = 20261029

_BASE_DATE = date(2026, 10, 1)

# Day 1 rule-breaking rows, one tuple per kind:
# (id prefix, count, supplier_id, part_type, region, event_date, quantity, unit_cost, status)
_DAY1_BAD_ROWS = (
    ("BAD-SUP", 12, "", "Transformer", "Gulf Coast", "2026-10-12", "10", "500.00", "DELIVERED"),
    ("BAD-QTY", 10, "SUP003", "Inverter", "Southwest", "2026-10-13", "0", "300.00", "DELAYED"),
    ("BAD-COST", 8, "SUP004", "Cooling Module", "Midwest", "2026-10-14", "6", "-5.00", "IN_TRANSIT"),
    ("BAD-DATE", 6, "SUP005", "Transformer", "Northeast", "not-a-date", "3", "100.00", "DELIVERED"),
    ("BAD-STATUS", 9, "SUP006", "Inverter", "Gulf Coast", "2026-10-15", "8", "800.00", "UNKNOWN"),
)


def _check_day(day: int) -> None:
    if day not in (1, 2, 3):
        raise ValueError(f"unknown batch day {day!r}; expected 1, 2 or 3")


def _check_version(version: int) -> None:
    if version not in (1, 2):
        raise ValueError(f"unknown supplier version {version!r}; expected 1 or 2")


def _shipment(i: int, rng: random.Random) -> dict[str, str]:
    """Base values for valid shipment number i (1-based).

    The random generator draws quantity and then unit cost. Keep the order of the calls
    the same, or the generated numbers change.
    """
    quantity = rng.randint(1, 100)
    unit_cost = rng.uniform(100, 3500)
    return {
        "shipment_id": f"SHP{i:06d}",
        "supplier_id": f"SUP{(i % 24) + 1:03d}",
        "part_type": PART_TYPES[i % 4],
        "region": REGIONS[(i // 4) % 4],
        "event_date": (_BASE_DATE + timedelta(days=(i - 1) % 28)).isoformat(),
        "quantity": str(quantity),
        "unit_cost_usd": f"{unit_cost:.2f}",
    }


def _day1_shipments() -> list[dict[str, str]]:
    """The 1,200 valid shipments of day 1. Later days copy values from this list."""
    rng = random.Random(SEED + 1)
    return [_shipment(i, rng) for i in range(1, 1201)]


def _fields(
    shipment_id: str,
    supplier_id: str,
    part_type: str,
    region: str,
    event_date: str,
    quantity: str,
    unit_cost: str,
) -> dict[str, str]:
    """Explicit values for a hand-made row (used for the bad rows)."""
    return {
        "shipment_id": shipment_id,
        "supplier_id": supplier_id,
        "part_type": part_type,
        "region": region,
        "event_date": event_date,
        "quantity": quantity,
        "unit_cost_usd": unit_cost,
    }


def _ts(start: str, seconds: int) -> str:
    """Return `start` plus `seconds`, as an ISO timestamp string for event_ts."""
    return (datetime.fromisoformat(start) + timedelta(seconds=seconds)).isoformat()


def _add(
    rows: list[dict[str, str]],
    day: int,
    fields: dict[str, str],
    event_ts: str,
    status: str,
    carrier: str | None = None,
) -> None:
    """Append one row. event_id is D<day>-<running number in this batch>, so it is unique per batch."""
    row = {"event_id": f"D{day}-{len(rows) + 1:06d}", **fields, "event_ts": event_ts, "status": status}
    if carrier is not None:
        row["carrier"] = carrier
    rows.append(row)


def _rows_day1() -> list[dict[str, str]]:
    base = _day1_shipments()
    rows: list[dict[str, str]] = []
    # 1,200 valid shipments. The status cycles with the shipment number.
    for i, shipment in enumerate(base, start=1):
        _add(rows, 1, shipment, _ts("2026-10-01T00:00:00", i), STATUSES[i % 3])
    # 45 rule-breaking rows. Each one has its own shipment id.
    for prefix, count, supplier, part, region, event_date, qty, cost, status in _DAY1_BAD_ROWS:
        for k in range(1, count + 1):
            fields = _fields(f"{prefix}-{k:03d}", supplier, part, region, event_date, qty, cost)
            _add(rows, 1, fields, "2026-10-01T12:00:00", status)
    # 15 later events for shipments 1..15: same values, but a newer status and event time.
    for k in range(1, 16):
        _add(rows, 1, base[k - 1], _ts("2026-10-01T23:00:00", k), "DELIVERED")
    return rows


def _rows_day2() -> list[dict[str, str]]:
    base = _day1_shipments()
    rng = random.Random(SEED + 2)
    rows: list[dict[str, str]] = []
    # 200 status updates for existing shipments 101..300: same values, status DELIVERED.
    for k, i in enumerate(range(101, 301), start=1):
        _add(rows, 2, base[i - 1], _ts("2026-10-02T08:00:00", k), "DELIVERED")
    # 100 new shipments 1201..1300.
    for k, i in enumerate(range(1201, 1301), start=1):
        _add(rows, 2, _shipment(i, rng), _ts("2026-10-02T09:00:00", k), STATUSES[i % 3])
    # 5 rows with a status that the rules do not allow.
    for k in range(1, 6):
        fields = _fields(f"BAD-STATUS2-{k:03d}", "SUP006", "Inverter", "Gulf Coast", "2026-10-15", "8", "800.00")
        _add(rows, 2, fields, "2026-10-02T10:00:00", "LOST")
    return rows


def _rows_day3() -> list[dict[str, str]]:
    base = _day1_shipments()
    rng = random.Random(SEED + 3)
    rows: list[dict[str, str]] = []
    # 50 status updates for existing shipments 301..350.
    for k, i in enumerate(range(301, 351), start=1):
        _add(rows, 3, base[i - 1], _ts("2026-10-03T08:00:00", k), "DELAYED", carrier=CARRIERS[i % 3])
    # 50 new shipments 1301..1350.
    for k, i in enumerate(range(1301, 1351), start=1):
        _add(rows, 3, _shipment(i, rng), _ts("2026-10-03T09:00:00", k), STATUSES[i % 3], carrier=CARRIERS[i % 3])
    return rows


def batch_rows(day: int) -> list[dict[str, str]]:
    """Rows of one daily batch as dicts of text values. Day 1 = first load, 2 = updates and new, 3 = adds carrier."""
    _check_day(day)
    if day == 1:
        return _rows_day1()
    if day == 2:
        return _rows_day2()
    return _rows_day3()


def batch_filename(day: int) -> str:
    """File name for a batch, for example shipments_2026-10-01.csv."""
    _check_day(day)
    return f"shipments_2026-10-{day:02d}.csv"


def batch_csv(day: int) -> str:
    """Full CSV text of a batch: header row, then the rows, with \\n line endings."""
    rows = batch_rows(day)
    columns = COLUMNS_DAY3 if day == 3 else COLUMNS
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def supplier_rows(version: int) -> list[dict]:
    """Supplier dimension rows. Version 2 is a later snapshot: three ratings change and SUP025 is new.

    This is the input for the dimension MERGE (SCD1 today, SCD2 in TODO-16).
    """
    _check_version(version)
    rows: list[dict] = []
    for n in range(1, 25):
        rows.append(
            {
                "supplier_id": f"SUP{n:03d}",
                "supplier_name": f"Supplier {n:02d}",
                "country": ("US", "MX", "CA")[n % 3],
                "rating": 1 + (n % 5),
            }
        )
    if version == 2:
        for row in rows[:3]:  # SUP001..SUP003
            row["rating"] = 5
        rows.append({"supplier_id": "SUP025", "supplier_name": "Supplier 25", "country": "US", "rating": 3})
    return rows
