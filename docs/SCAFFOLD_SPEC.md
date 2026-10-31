# SCAFFOLD_SPEC — exact build spec for the learning scaffold

Internal spec written by the orchestrator (Claude Code / Opus) for implementer and reviewer agents.
`PLAN.md` = the why; this file = the exact what. If they disagree, this file wins.
Kept in the repo so the human can see how the scaffold was specified.

## 0. Global rules

- Target runtime: **Databricks Free Edition, serverless notebooks/jobs** (Spark Connect).
  - NEVER use `spark.sparkContext`, RDDs, `dbfs:/`, `/dbfs/`, cluster configs, `spark.conf.set` of non-allowed keys, `%run` of outside paths.
  - Data lives only in Unity Catalog: catalog `workspace`, schema `shipments`, volume `raw` (all overridable).
- Local test runtime: PySpark (classic, `local[1]`), Python 3.12, Java 17. Code in `src/` must work in BOTH.
- Use only `pyspark.sql` DataFrame API / Spark SQL. No `delta-spark` Python package (not available locally); MERGE etc. via `spark.sql(...)`.
- ANSI mode is ON on serverless → use `try_cast` (via `F.expr("try_cast(x AS INT)")`) when casting raw strings.
- Unfinished functions: body is exactly `raise NotImplementedError("TODO-NN: see docs/TODO.md#todo-NN")`.
- Notebooks must run end-to-end on day 1 ("Run all") even with every TODO unfinished: TODO code cells catch
  `NotImplementedError` or are no-op cells that print `"TODO-NN not done yet - skipped"`.
- Every TODO site uses this exact comment template (fill all fields from §3):

```
# TODO-<NN>: <short title>
# WHAT:   ...
# WHY:    ...
# PRODUCT: ...
# SCOPE:  ~N lines, ~M min, difficulty D/3
# HINTS:  ... + <official doc link>
# DONE WHEN: ...
# SEE:    <file>:<function>, docs/TODO.md#todo-NN
```

- Reference (implemented) code: clear docstrings, short comments explaining the *concept* (this is a teaching repo), type hints, no cleverness.
- Do NOT modify: `notebooks/01_bronze_silver_gold.*`, `quality_portal/**`, `tests/test_core.py`, `Dockerfile`, `compose.yaml`, `LICENSE`, `docs/ARCHITECTURE.md`, `docs/SECURITY.md`, `docs/VALIDATION.md`, `docs/evidence/**`, `PUBLISH.md`, `scripts/**`.

## 1. Data contract (generator) — numbers the docs depend on

Raw CSV columns (header row, all values as text), in this order:
`event_id, shipment_id, supplier_id, part_type, region, event_date, event_ts, quantity, unit_cost_usd, status`
Day 3 adds a trailing column `carrier`.

Constants: `REGIONS = ("Gulf Coast","Midwest","Southwest","Northeast")`, `PART_TYPES = ("Transformer","Inverter","Turbine Component","Cooling Module")`,
`STATUSES = ("IN_TRANSIT","DELIVERED","DELAYED")`, `CARRIERS = ("FastFreight","BlueLine","RailCo")`, `SEED = 20261029`.

For shipment number `i` (1-based): `shipment_id = f"SHP{i:06d}"`, `supplier_id = f"SUP{(i % 24) + 1:03d}"`,
`part_type = PART_TYPES[i % 4]`, `region = REGIONS[(i // 4) % 4]`, `event_date = 2026-10-01 + ((i-1) % 28) days` (ISO),
quantity `rng.randint(1,100)`, unit_cost `f"{rng.uniform(100,3500):.2f}"`. `rng = random.Random(SEED + day)`.
`event_id` = `f"D{day}-{n:06d}"` with n = running row number inside the batch (1-based).

**Day 1** — file `shipments_2026-10-01.csv`, 1,260 rows:
- 1,200 valid: i = 1..1200, status `STATUSES[i % 3]`, event_ts `2026-10-01T00:00:00` + i seconds.
- 45 rule-breaking rows, unique shipment ids, event_ts `2026-10-01T12:00:00`:
  - 12 `BAD-SUP-{k:03d}`: supplier_id empty string; rest valid (Transformer, Gulf Coast, 2026-10-12, qty 10, cost 500.00, DELIVERED)
  - 10 `BAD-QTY-{k:03d}`: quantity `0` (SUP003, Inverter, Southwest, 2026-10-13, cost 300.00, DELAYED)
  - 8 `BAD-COST-{k:03d}`: unit_cost `-5.00` (SUP004, Cooling Module, Midwest, 2026-10-14, qty 6, IN_TRANSIT)
  - 6 `BAD-DATE-{k:03d}`: event_date `not-a-date` (SUP005, Transformer, Northeast, qty 3, cost 100.00, DELIVERED)
  - 9 `BAD-STATUS-{k:03d}`: status `UNKNOWN` (SUP006, Inverter, Gulf Coast, 2026-10-15, qty 8, cost 800.00)
- 15 later events for SHP000001..SHP000015: copy of the shipment's values but status `DELIVERED`, event_ts `2026-10-01T23:00:00` + k seconds.
**Day 2** — `shipments_2026-10-02.csv`, 305 rows: 200 updates for i=101..300 (copy values, status `DELIVERED`, event_ts `2026-10-02T08:00:00`+k s);
100 new shipments i=1201..1300 (status `STATUSES[i % 3]`, event_ts `2026-10-02T09:00:00`+k s); 5 new `BAD-STATUS2-{k:03d}` with status `LOST`.
**Day 3** — `shipments_2026-10-03.csv`, 100 rows, extra column `carrier = CARRIERS[i % 3]`: 50 updates i=301..350 (status `DELAYED`,
event_ts `2026-10-03T08:00:00`+k s) and 50 new i=1301..1350 (event_ts `2026-10-03T09:00:00`+k s).

Expected table counts once ALL core TODOs are done (used in DONE WHEN checks):

| After loading | bronze rows | quarantine rows | silver rows (distinct shipments) | gold_region_part rows |
|---|---|---|---|---|
| day 1 | 1,260 | 45 | 1,200 | 16 |
| days 1–2 | 1,565 | 50 | 1,300 | 16 |
| days 1–3 | 1,665 | 50 | 1,350 | 16 |

Day-1 reference state (no TODOs done): quarantine 12 (only `missing_supplier`), silver 1,248 rows (no dedup, other bad rows pass).

Supplier dimension `generator.supplier_rows(version)`: version 1 = 24 rows `supplier_id SUP001..SUP024`, `supplier_name f"Supplier {n:02d}"`,
`country ("US","MX","CA")[n % 3]`, `rating` int `1 + (n % 5)`. Version 2 = version 1 with rating of SUP001..SUP003 set to 5 and a new SUP025
(`Supplier 25`, `US`, 3). dim_supplier rows: v1 → 24; v1 then v2 with SCD1 → 25; with SCD2 (TODO-16) → 28 (25 current + 3 expired).

## 2. Files and exact contents

### `src/shipments_pipeline/__init__.py` — empty docstring module.

### `src/shipments_pipeline/config.py` [REFERENCE]
```python
BRONZE = "bronze_shipments"; SILVER = "silver_shipments"; QUARANTINE = "quarantine_shipments"
DIM_SUPPLIER = "dim_supplier"; GOLD_REGION_PART = "gold_region_part"; GOLD_SUPPLIER_SCORECARD = "gold_supplier_scorecard"
DQ_RESULTS = "dq_results"; PIPELINE_RUNS = "pipeline_runs"

@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "workspace"; schema: str = "shipments"; volume: str = "raw"
    @property def volume_path(self) -> str: "/Volumes/{catalog}/{schema}/{volume}"
    def table(self, name: str) -> str: "{catalog}.{schema}.{name}"

def from_widgets(dbutils) -> PipelineConfig  # creates text widgets catalog/schema/volume (defaults above) and reads them
def get_widget(dbutils, name: str, default: str) -> str  # creates text widget if missing, returns value
```

### `src/shipments_pipeline/generator.py` [REFERENCE] — pure Python, no Spark.
`COLUMNS`, `COLUMNS_DAY3`, constants from §1; `batch_rows(day: int) -> list[dict[str,str]]`; `batch_filename(day) -> str`;
`batch_csv(day) -> str` (header + rows via `csv` module, `\n` line endings); `supplier_rows(version: int) -> list[dict]`.
Raise `ValueError` for unknown day/version.

### `src/shipments_pipeline/bronze.py`
- [REFERENCE] `read_batch(spark, path: str) -> DataFrame`: `spark.read.option("header", True).csv(path)` (all strings, no inferSchema),
  then add `_source_file` = `F.col("_metadata.file_path")` and `_ingested_at` = `F.current_timestamp()`. Docstring explains why bronze keeps raw strings.
- [TODO-07] `list_new_files(all_files: list[str], ingested_files: set[str]) -> list[str]` — NotImplementedError.

### `src/shipments_pipeline/quality.py`
- `REASONS_COL = "_dq_reasons"`; `VALID_STATUSES = ("IN_TRANSIT","DELIVERED","DELAYED")`.
- [REFERENCE] `cast_types(df) -> DataFrame`: typed columns via try_cast: `event_date` DATE, `event_ts` TIMESTAMP, `quantity` INT,
  `unit_cost_usd` DECIMAL(12,2); strings stay strings; extra columns (e.g. `carrier`, `_source_file`, `_ingested_at`) are kept untouched.
- [REFERENCE] `rule_missing_supplier() -> Column`: `F.when(supplier_id is null OR trim(supplier_id) == "", F.lit("missing_supplier"))`.
- [TODO-02] `rule_invalid_quantity()` (null or <= 0 → `invalid_quantity`), `rule_invalid_unit_cost()` (null or < 0 → `invalid_unit_cost`),
  `rule_invalid_date()` (event_date null after cast → `invalid_date`), `rule_invalid_status()` (null or not in VALID_STATUSES → `invalid_status`):
  each NotImplementedError. ONE TODO block above the four functions.
- [REFERENCE] `ALL_RULES = [rule_missing_supplier, rule_invalid_quantity, ...]`; `active_rules() -> list[Column]` calls each, skips
  NotImplementedError with `print(f"skipping {fn.__name__}: not implemented yet")`.
- [REFERENCE] `with_quality_flags(df) -> DataFrame` adds `_dq_reasons = concat_ws("; ", *active_rules())` ("" when no rule fires).
- [REFERENCE] `split_valid_quarantine(df) -> tuple[DataFrame, DataFrame]`: valid = reasons == "" (column dropped); quarantine keeps the column.

### `src/shipments_pipeline/silver.py`
- [TODO-04] `dedup_latest(df) -> DataFrame`: one row per shipment_id, latest `event_ts` (tie-break `event_id` desc).
- [TODO-08] `merge_into_silver(spark, updates: DataFrame, target_table: str) -> None`: MERGE on shipment_id; update all columns only
  when `s.event_ts > t.event_ts`; insert when not matched. (Create table from `updates` if it does not exist.)
- [REFERENCE] `unprocessed_bronze(spark, bronze_table, silver_table, quarantine_table) -> DataFrame`: bronze rows whose `_source_file`
  is not present in silver or quarantine (left_anti join; if a target table doesn't exist treat as empty). Docstring explains why the
  `s.event_ts > t.event_ts` guard makes re-processing safe.

### `src/shipments_pipeline/dims.py`
- [REFERENCE] `supplier_df(spark, version: int) -> DataFrame` from `generator.supplier_rows` with explicit schema (rating INT) + `updated_at` current_timestamp.
- [REFERENCE] `upsert_dim_supplier(spark, source: DataFrame, target_table: str) -> None`: SCD1. If `not spark.catalog.tableExists(target_table)`:
  `source.write.saveAsTable(target_table)`; else temp view `supplier_updates` + `spark.sql("MERGE INTO ... WHEN MATCHED AND (changed) THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...")`.
  Comment every MERGE clause — this is THE reference for the MERGE pattern.
- [TODO-16] `upsert_dim_supplier_scd2(spark, source, target_table) -> None` NotImplementedError (columns `valid_from`, `valid_to`, `is_current`).

### `src/shipments_pipeline/gold.py`
- [REFERENCE] `build_region_part(silver: DataFrame) -> DataFrame`: group by region, part_type: `shipment_count`, `units_total`,
  `modeled_inventory_value_usd` (round(sum(qty*cost),2)), `delayed_shipments`, `delayed_pct` (round 2), `first_event_date`, `last_event_date`.
- [TODO-06] `build_supplier_scorecard(spark, silver_table: str, dim_table: str) -> DataFrame`: Spark SQL with a CTE; join dim_supplier (SCD1, one row per supplier);
  columns `supplier_id, supplier_name, country, rating, shipment_count, delayed_pct, avg_unit_cost_usd, delay_rank` (`RANK() OVER (ORDER BY delayed_pct DESC)`).

### `src/shipments_pipeline/checks.py`
- [REFERENCE] `@dataclass(frozen=True) class CheckResult: name, severity ("critical"|"warning"), passed: bool, observed: str, expected: str`.
- [REFERENCE] `check_no_shipment_lost(bronze, silver, quarantine) -> CheckResult` (critical): distinct shipment_id in bronze == distinct
  shipment_id in silver ∪ quarantine.
- [REFERENCE] `results_to_df(spark, results, run_id: str) -> DataFrame` (adds `run_id`, `checked_at`).
- [TODO-09] `check_unique_key(df, key: str, name: str) -> CheckResult` (critical), `check_not_empty(df, name: str) -> CheckResult` (critical),
  `raise_on_critical(results) -> None` (raise `RuntimeError` listing failed critical checks). One TODO block.

### `src/shipments_pipeline/audit.py`
- [REFERENCE] `log_run(spark, table_fqn: str, run_id: str, task: str, target_table: str, rows: int, status: str = "SUCCESS") -> None`:
  one-row DataFrame (`run_id, task, target_table, rows, status, logged_at`) appended to `table_fqn`.

### Notebooks (Databricks source format: `# Databricks notebook source`, cells split by `# COMMAND ----------`, markdown as `# MAGIC %md`)
Every new notebook starts with: markdown title + purpose + "Run on: Serverless"; a setup cell:
```python
%load_ext autoreload
%autoreload 2
```
(own cell; in Databricks source format write them as `# MAGIC %load_ext autoreload` / `# MAGIC %autoreload 2`) then
```python
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from shipments_pipeline import config
cfg = config.from_widgets(dbutils)
run_id = config.get_widget(dbutils, "run_id", "manual")
```
- `00_generate_batches.py`: widget `days` (default `"1"`, comma list). `CREATE SCHEMA IF NOT EXISTS`, `CREATE VOLUME IF NOT EXISTS`,
  writes each day's CSV with `dbutils.fs.put(f"{cfg.volume_path}/{name}", generator.batch_csv(day), overwrite=True)`, lists the volume.
- `10_bronze.py`: lists CSVs in the volume (`dbutils.fs.ls`), TODO-07 cell tries `bronze.list_new_files(all, ingested)` (ingested =
  distinct `_source_file` from bronze if it exists) and falls back to ALL files with a warning that reruns duplicate data. Reads each file with
  `read_batch`, appends to bronze (`mode("append")`, no mergeSchema — TODO-10 comment block here explains the day-3 error to expect),
  `audit.log_run(...)`, displays counts per `_source_file`.
- `20_silver.py`: widget `supplier_version` (default "1"): `dims.upsert_dim_supplier(...)`. Then: choose input — if MERGE (TODO-08) works:
  `unprocessed_bronze` → cast → split → dedup → `merge_into_silver`, quarantine `append`; fallback (NotImplementedError): full refresh from
  ALL bronze → cast → split → dedup (if TODO-04 not done: skip with warning) → silver `overwrite` (+`overwriteSchema`), quarantine `overwrite`.
  The switch is a flag cell `USE_MERGE = False  # flip to True when TODO-08 is done`.
  Prints counts; TODO-13 marker (call log_run here too).
- `30_gold.py`: build_region_part → overwrite gold_region_part; TODO-06 cell (try/except NotImplementedError); TODO-17 no-op cell (broadcast join).
- `40_quality_checks.py`: reference check + TODO-09 checks (try/except), `results_to_df` → append `dq_results`, display; TODO-09 raise_on_critical.
- `50_delta_ops.py`: reference: `DESCRIBE HISTORY silver`, count at `VERSION AS OF 0` vs now. TODO-11 no-op cell (diff + simulated bad DELETE + RESTORE).
  TODO-12 no-op cell (CLUSTER BY + OPTIMIZE + DESCRIBE DETAIL). Markdown warning: never `VACUUM ... RETAIN 0 HOURS`.
- `60_monitoring.py`: reference: last 20 rows of `pipeline_runs`. TODO-13 no-op cell (explain plan of gold query, find `Exchange`). TODO-18 no-op cell (system.billing.usage, may be unavailable on Free Edition).
- `99_run_tests.py`: `%pip install pytest` cell; then `import pytest, sys; sys.dont_write_bytecode = True;`
  `rc = pytest.main(["-q", "-p", "no:cacheprovider", os.path.abspath(os.path.join(os.getcwd(), "..", "tests"))]); assert rc == 0`.
  Markdown: "Databricks path not yet verified by the scaffold author".

### Tests
- `tests/conftest.py` [REFERENCE]: add `<repo>/src` and `<repo>` to `sys.path`; session fixture `spark`: return
  `SparkSession.getActiveSession()` if not None; elif env `DATABRICKS_RUNTIME_VERSION` set: `from databricks.sdk.runtime import spark`; else
  local builder `master("local[1]")`, `spark.sql.shuffle.partitions=1`, `spark.sql.ansi.enabled=true`, `spark.ui.enabled=false`, session timezone UTC.
- `tests/test_generator.py` [REFERENCE, pure Python]: asserts the §1 numbers (1260/305/100 rows; 45 bad rows; day-3 header has carrier; supplier v1 24, v2 25).
- `tests/test_quality.py`: [REFERENCE] `test_missing_supplier_rule` (3 rows: null, "  ", "SUP001" → reasons) and `test_cast_types_invalid_date_becomes_null`;
  [TODO-03] `test_invalid_quantity_rule`, `test_invalid_unit_cost_rule`, `test_invalid_date_rule`, `test_invalid_status_rule` each
  `pytest.skip("TODO-03: ...")` with the TODO template above the first one.
- `tests/test_silver.py`: [TODO-05] `test_dedup_latest_keeps_newest_event` → `pytest.skip("TODO-05 ...")`.
- `requirements-dev.txt`: `pyspark>=3.5,<4.1`, `pytest>=8`.
- `pyproject.toml`: ADD ONLY `[tool.pytest.ini_options]` with `testpaths = ["tests"]`, `pythonpath = ["src", "."]`.

### Bundle
- `databricks.yml`: bundle name `shipments-lakehouse`; `include: [resources/*.yml]`; variables `catalog` (workspace), `schema` (shipments);
  `sync.exclude`: `notebooks/01_bronze_silver_gold.ipynb`, `docs/**`, `data/**`, `quality_portal/**`, `.venv/**`;
  target `dev` (`mode: development`, `default: true`). No host, no clusters. Top comment: "validate only — this project never deploys".
- `resources/shipments_job.yml`: job `shipments_pipeline`, name `shipments-pipeline`, job parameters `catalog` `${var.catalog}`, `schema` `${var.schema}`;
  tasks `generate` (notebook `../notebooks/00_generate_batches.py`, base_parameters `days: "1"`, `run_id: "{{job.run_id}}"`) and
  `bronze` (depends_on generate, notebook 10_bronze, `run_id`). No compute → serverless. TODO-14 template comment for silver/gold/quality tasks.

## 3. TODO catalogue (facts every implementer must use verbatim for WHAT / DONE WHEN / SEE)

| # | Title | Where | WHAT | DONE WHEN | SEE | Scope |
|---|---|---|---|---|---|---|
| 01 | Run & explore | docs only (+ notebooks) | Run original notebook, then 00 (days=1) → 10 → 20 → 30 → 40 → 50 → 60; in a cell run `spark.table(cfg.table("gold_region_part")).explain()` and `DESCRIBE HISTORY` on silver | original prints `VALIDATED`; bronze 1,260, silver 1,248, quarantine 12, gold 16 rows | notebooks/01_bronze_silver_gold.py | ~0 lines, 30m, 1 |
| 02 | Remaining quality rules | quality.py | implement 4 rule functions | after rerun of 20_silver (day 1): quarantine 45 rows, 5 distinct reason values; after TODO-03, the 4 rule tests pass (not skipped) | quality.py:rule_missing_supplier | ~12 lines, 25m, 1 |
| 03 | Tests for rules | tests/test_quality.py | replace 4 skips with real tests | `pytest -q` shows the 4 tests passed, not skipped | tests/test_quality.py:test_missing_supplier_rule | ~30 lines, 25m, 1 |
| 04 | Dedup latest event | silver.py | `dedup_latest` with Window + row_number | (with TODO-02 done) silver 1,200 rows, `count(distinct shipment_id) = count(*)`; SHP000002 status DELIVERED (its older day-1 event was DELAYED) | quality.py:split_valid_quarantine (DataFrame style) | ~8 lines, 25m, 2 |
| 05 | Test dedup | tests/test_silver.py | test with 3 events / 2 shipments | test passes | tests/test_quality.py:test_missing_supplier_rule | ~20 lines, 15m, 1 |
| 06 | Supplier scorecard (SQL) | gold.py | `build_supplier_scorecard` | gold_supplier_scorecard has 24 rows, delay_rank 1 = highest delayed_pct | gold.py:build_region_part, dims.py (SQL style) | ~25 lines, 30m, 2 |
| 07 | Incremental bronze | bronze.py + 10_bronze | `list_new_files` | running 10_bronze twice keeps bronze at 1,260 (day 1) | bronze.py:read_batch | ~5 lines + notebook, 30m, 2 |
| 08 | Silver MERGE | silver.py + 20_silver (`USE_MERGE=True`) | `merge_into_silver` | (with TODO-02 and TODO-04 done) 00 days=2 → 10 → 20: silver 1,300, quarantine 50; SHP000150 status DELIVERED; rerun 20 → no change | dims.py:upsert_dim_supplier | ~20 lines, 40m, 2 |
| 09 | More DQ checks | checks.py + 40 | 3 functions | dq_results has 3+ rows per run; temporarily break a check → notebook fails with RuntimeError | checks.py:check_no_shipment_lost | ~25 lines, 30m, 2 |
| 10 | Schema evolution | 10_bronze + silver.py | handle day 3 `carrier` column: bronze append and quarantine append with `mergeSchema`; silver MERGE with `UPDATE SET *`/`INSERT *` and `MERGE WITH SCHEMA EVOLUTION INTO` | 00 days=3 → 10 → 20: bronze 1,665, silver 1,350, `carrier` column exists, NULL for older rows | docs/CONCEPTS.md#schema-enforcement-vs-evolution | ~7 lines, 30m, 2 |
| 11 | Time travel + RESTORE | 50_delta_ops | diff two versions (EXCEPT), `DELETE` Midwest rows, `RESTORE TABLE ... TO VERSION AS OF n` | silver count back to pre-delete value; history shows DELETE and RESTORE ops | 50_delta_ops reference cell | ~10 lines SQL, 20m, 2 |
| 12 | OPTIMIZE + liquid clustering | 50_delta_ops | `ALTER TABLE workspace.shipments.silver_shipments CLUSTER BY (region, event_date)`, `OPTIMIZE`, compare `DESCRIBE DETAIL` numFiles/clusteringColumns | `clusteringColumns` shows both columns; write down numFiles before/after | 50_delta_ops | ~6 lines SQL, 25m, 2 |
| 13 | Audit everywhere + query plan | 20/30/40 + 60 | call `log_run` in silver/gold/dq tasks; `explain("formatted")` gold query, find Exchange (shuffle) and explain why | pipeline_runs has a row per task per run; note in docs/NOTES.md where the shuffle is | audit.py:log_run, 10_bronze | ~10 lines, 30m, 2 |
| 14 | Complete the job (bundle) | resources/shipments_job.yml | add silver, gold, quality_checks tasks with depends_on + run_id param; install Databricks CLI; `databricks bundle validate` (NO deploy) | `databricks bundle validate` prints "Validation OK!" | resources/shipments_job.yml bronze task | ~20 lines YAML, 30m, 2 |
| 15 | CI tests job | .github/workflows/tests.yml | add a job: setup-java 17, python 3.12, `pip install -r requirements-dev.txt`, `pytest -q`; NO deploy steps | `act`-free check: YAML valid + same commands pass locally | existing tests.yml | ~15 lines YAML, 20m, 1 |
| 16 | Stretch: SCD2 dim | dims.py | `upsert_dim_supplier_scd2` writing NEW table `dim_supplier_scd2` + a 20_silver cell running v1 then v2 | dim_supplier_scd2 after v1 then v2: 28 rows, 25 with is_current=true, SUP001 has 2 rows | dims.py:upsert_dim_supplier | ~35 lines, 60m, 3 |
| 17 | Stretch: broadcast join | 30_gold | join silver with dim_supplier with and without `F.broadcast`, compare `explain()` (BroadcastHashJoin vs SortMergeJoin), read about AQE | written comparison in docs/NOTES.md | 30_gold | ~10 lines, 30m, 3 |
| 18 | Stretch: cost awareness | 60_monitoring | query `system.billing.usage` for last 7 days (DBUs by sku); if not accessible on Free Edition, record the error | query returns rows OR error documented in docs/NOTES.md | 60_monitoring | ~8 lines SQL, 20m, 2 |

Doc links to use in HINTS (only these; do not invent others):
MERGE https://docs.databricks.com/aws/en/delta/merge · history/time travel https://docs.databricks.com/aws/en/delta/history ·
RESTORE https://docs.databricks.com/aws/en/sql/language-manual/delta-restore · OPTIMIZE https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize ·
liquid clustering https://docs.databricks.com/aws/en/delta/clustering · schema evolution https://docs.databricks.com/aws/en/delta/update-schema ·
volumes https://docs.databricks.com/aws/en/volumes/ · file metadata column https://docs.databricks.com/aws/en/ingestion/file-metadata-column ·
try_cast https://docs.databricks.com/aws/en/sql/language-manual/functions/try_cast · SQL window functions https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions ·
PySpark Window https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html ·
explain https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html ·
broadcast https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html ·
AQE https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution ·
bundles https://docs.databricks.com/aws/en/dev-tools/bundles/ · bundle resources https://docs.databricks.com/aws/en/dev-tools/bundles/resources ·
dynamic value references https://docs.databricks.com/aws/en/jobs/dynamic-value-references · CLI install https://docs.databricks.com/aws/en/dev-tools/cli/install ·
Git folders https://docs.databricks.com/aws/en/repos/ · billing system table https://docs.databricks.com/aws/en/admin/system-tables/billing ·
Free Edition limitations https://docs.databricks.com/aws/en/getting-started/free-edition-limitations · pytest fixtures https://docs.pytest.org/en/stable/how-to/fixtures.html ·
setup-java https://github.com/actions/setup-java

## 4. Honesty rules for docs
- Things verified locally by the scaffold: generator numbers, pure-Python tests, PySpark unit tests (local Spark), YAML parses.
- NOT verified by the scaffold author: anything on Databricks (volumes, MERGE on serverless, liquid clustering, system tables, `bundle validate`,
  `99_run_tests`, autoreload). Say "expected to work — not yet verified on Free Edition" and invite the learner to confirm (and tick it).
- Claims about the original author's runs stay attributed to the original author.
