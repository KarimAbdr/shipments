"""Names and locations used by the shipments pipeline.

Change table names or the catalog/schema/volume in this one module. Notebooks read the
catalog, schema and volume from widgets (or job parameters), so the same code can run
in another schema without editing it.
"""

from dataclasses import dataclass
from typing import Any

# Table names without catalog/schema. Combine them with PipelineConfig.table().
BRONZE = "bronze_shipments"
SILVER = "silver_shipments"
QUARANTINE = "quarantine_shipments"
DIM_SUPPLIER = "dim_supplier"
GOLD_REGION_PART = "gold_region_part"
GOLD_SUPPLIER_SCORECARD = "gold_supplier_scorecard"
DQ_RESULTS = "dq_results"
PIPELINE_RUNS = "pipeline_runs"


@dataclass(frozen=True)
class PipelineConfig:
    """Where the pipeline reads and writes.

    `frozen=True` means the object cannot be changed after it is created, so no
    function can quietly point the pipeline at another schema halfway through a run.
    """

    catalog: str = "workspace"
    schema: str = "shipments"
    volume: str = "raw"

    @property
    def volume_path(self) -> str:
        """Path of the Unity Catalog volume that holds the raw CSV batches."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    def table(self, name: str) -> str:
        """Fully qualified table name, for example workspace.shipments.silver_shipments."""
        return f"{self.catalog}.{self.schema}.{name}"


def get_widget(dbutils: Any, name: str, default: str) -> str:
    """Return a notebook widget value, creating the widget with `default` if it is missing.

    Widgets are how a notebook receives parameters. You type them in the UI, or a job
    passes them in as base_parameters or job parameters.
    """
    try:
        return dbutils.widgets.get(name)
    except Exception:
        # The widget does not exist yet in this notebook session, so create it.
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)


def from_widgets(dbutils: Any) -> PipelineConfig:
    """Build the config from the catalog, schema and volume widgets (defaults: workspace/shipments/raw)."""
    return PipelineConfig(
        catalog=get_widget(dbutils, "catalog", "workspace"),
        schema=get_widget(dbutils, "schema", "shipments"),
        volume=get_widget(dbutils, "volume", "raw"),
    )
