"""Shared pytest setup: import paths and one Spark session for the whole test run.

Pure-Python tests (test_generator.py) do not need Spark at all. Tests that take the
`spark` fixture get a Spark session here.
"""

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Make `import shipments_pipeline` work whether pytest runs locally or on Databricks.
for path in (str(REPO_ROOT / "src"), str(REPO_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture(scope="session")
def spark():
    """Return a Spark session. Order: an active session, then the Databricks runtime, then a local one.

    The local session uses one thread (local[1]). ANSI mode is on so the tests behave like
    serverless, which also runs in ANSI mode.
    """
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Running inside a Databricks notebook: the runtime provides the session.
        from databricks.sdk.runtime import spark as runtime_spark

        return runtime_spark

    return (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
