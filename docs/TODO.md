# TODO guide: the 18 things you implement

This repository is a scaffold. The reference parts (config, generator, bronze read, the quality framework, the SCD1 MERGE, the gold aggregate, one check, the audit log, the job's first tasks) are already written and explained in the code. The 18 TODOs are the parts you write yourself, in order. Each TODO is small, and each one teaches one main Spark or Delta concept.

## How to work a TODO

1. **Read** the TODO section below, including its **WHAT** and **DONE WHEN**.
2. **Read the reference** at the location in **SEE**. The reference code shows the style, the names, and the pattern to copy in spirit.
3. **Implement** at the TODO site. Every site has a comment block that matches the template in the spec, and its body is `raise NotImplementedError("TODO-NN: see docs/TODO.md#todo-NN")` until you replace it.
4. **Run the DONE WHEN check.** It is either a `pytest` command or a notebook run with an expected count.
5. **Tick** `- [ ] Done` in this section, and tick the matching item in the [README checklist](../README.md#c-my-implementations).
6. **Commit** with the message `feat(todo-NN): ...`, for example `feat(todo-07): incremental bronze ingest`.

Notes:

- Things marked **Databricks-only check** are expected to work on Databricks Free Edition, but **not yet verified by the scaffold author on Free Edition**. If you confirm one, you are welcome to say so in your notes.
- Write your notes for TODO-13, TODO-17 and TODO-18 in `docs/NOTES.md`. You create that file.
- Never use `VACUUM ... RETAIN 0 HOURS`. It deletes files that time travel and concurrent readers still need.

## Summary

| # | Title | Concept | Difficulty (1–3) | Time |
|---|---|---|---|---|
| [01](#todo-01) | Run & explore | Workspace, serverless, Unity Catalog, lazy evaluation | 1 | 30m |
| [02](#todo-02) | Remaining quality rules | Column expressions, null semantics | 1 | 25m |
| [03](#todo-03) | Tests for rules | pytest and the Spark fixture | 1 | 25m |
| [04](#todo-04) | Dedup latest event | Window functions, wide transformation, shuffle | 2 | 25m |
| [05](#todo-05) | Test dedup | Testing transformations | 1 | 15m |
| [06](#todo-06) | Supplier scorecard (SQL) | SQL joins, CTEs, window ranking | 2 | 30m |
| [07](#todo-07) | Incremental bronze | Idempotency, incremental loads | 2 | 30m |
| [08](#todo-08) | Silver MERGE | Delta MERGE, upserts | 2 | 40m |
| [09](#todo-09) | More DQ checks | Data quality, fail-fast | 2 | 30m |
| [10](#todo-10) | Schema evolution | Schema enforcement vs evolution, MERGE schema evolution | 2 | 30m |
| [11](#todo-11) | Time travel + RESTORE | Transaction log, recovery | 2 | 20m |
| [12](#todo-12) | OPTIMIZE + liquid clustering | File layout, small files | 2 | 25m |
| [13](#todo-13) | Audit everywhere + query plan | Monitoring, shuffle, query plans | 2 | 30m |
| [14](#todo-14) | Complete the job (bundle) | Jobs and task dependencies, Asset Bundles | 2 | 30m |
| [15](#todo-15) | CI tests job | CI stages | 1 | 20m |
| [16](#todo-16) | Stretch: SCD2 dim | SCD type 2, MERGE with history | 3 | 60m |
| [17](#todo-17) | Stretch: broadcast join | Joins, broadcast, AQE | 3 | 30m |
| [18](#todo-18) | Stretch: cost awareness | Cost monitoring, system tables | 2 | 20m |

Core TODOs are 01–15. Stretch TODOs are 16–18.

---

<a id="todo-01"></a>
## TODO-01: Run & explore

- [ ] Done

**Concept taught:** Workspace, serverless compute, Unity Catalog names, and lazy evaluation (what `explain()` shows you).

**WHAT:** Run original notebook, then 00 (days=1) → 10 → 20 → 30 → 40 → 50 → 60; in a cell run `spark.table(cfg.table("gold_region_part")).explain()` and `DESCRIBE HISTORY` on silver

**WHY:** Before you change anything, you need to see what the pipeline produces and how Spark plans the work. This TODO is the baseline every later TODO is measured against.

**PRODUCT:** A known starting point: the original notebook passes, and the reference path produces the expected day-1 counts.

**SCOPE:** ~0 lines, 30m, difficulty 1/3

**HINTS:**
- Run `notebooks/01_bronze_silver_gold` first. It prints `VALIDATED` when its assertions pass. It writes to the `ai_ready_demo` schema, so it does not collide with the scaffold's `workspace.shipments`.
- Then follow the README run order, which also includes `60_monitoring`.
- `DataFrame.explain()` is documented here: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
- Run `DESCRIBE HISTORY` on the silver table in a SQL cell and note the operations listed.
- Read the plan from the bottom up: leaves are the scans, the top is the final result.

**Common pitfalls:**
- Nothing runs until an action. Calling `explain()` shows the plan but does not execute it.

**DONE WHEN:** original prints `VALIDATED`; bronze 1,260, silver 1,248, quarantine 12, gold 16 rows

**SEE:** notebooks/01_bronze_silver_gold.py

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-02"></a>
## TODO-02: Remaining quality rules

- [ ] Done

**Concept taught:** Column expressions and null semantics. A `NULL` comparison is neither true nor false, so each rule must handle nulls on purpose.

**WHAT:** implement 4 rule functions

**WHY:** A quality rule that silently passes nulls lets bad rows into silver. Each rule should say exactly which rows it rejects.

**PRODUCT:** Day-1 quarantine grows from 12 to 45 rows, with five different reason values, so the quarantine table explains every rejected row.

**SCOPE:** ~12 lines, 25m, difficulty 1/3

**HINTS:**
- Look at `rule_missing_supplier()` first. Your functions should follow the same shape.
- The columns are already typed by `cast_types()`: `quantity` is INT, `unit_cost_usd` is DECIMAL and `event_date` is DATE. Compare numbers with numbers.
- For `event_date`, the rule is about the value after the cast. An unparseable date becomes NULL through `try_cast`.
- Functions to look at: `F.when`, `F.col`, `.isNull()`, `.isin(...)`, `F.trim`, `F.lit`.
- `try_cast` in Databricks SQL: https://docs.databricks.com/aws/en/sql/language-manual/functions/try_cast

**Common pitfalls:**
- ANSI mode is on. Casting a string like `not-a-date` with a plain `cast` raises an error on serverless, which is why the reference uses `try_cast`.
- `quantity <= 0` is false when `quantity` is NULL, so check for NULL explicitly.
- Until TODO-04 is done, silver still contains the 15 duplicate events, so silver counts will be higher than 1,200.

**DONE WHEN:** after rerun of 20_silver (day 1): quarantine 45 rows, 5 distinct reason values; after TODO-03, the 4 rule tests pass (not skipped)

**SEE:** quality.py:rule_missing_supplier

> The 20_silver rerun is a Databricks-only check: not yet verified by the scaffold author on Free Edition. The rule tests run locally (see TODO-03).

---

<a id="todo-03"></a>
## TODO-03: Tests for rules

- [ ] Done

**Concept taught:** Unit-testing PySpark. A small local Spark session can test a `DataFrame -> DataFrame` function with a handful of rows.

**WHAT:** replace 4 skips with real tests

**WHY:** Rules are easy to break when someone edits them later. A test with three rows (null, blank, valid) makes the intended behaviour explicit.

**PRODUCT:** Each rule from TODO-02 has a test that fails if the rule changes.

**SCOPE:** ~30 lines, 25m, difficulty 1/3

**HINTS:**
- `tests/test_quality.py` has one reference test (`test_missing_supplier_rule`) and four skipped TODO-03 tests. Each skip is `pytest.skip("TODO-03: ...")`. Remove the skip and write the body.
- The `spark` fixture comes from `tests/conftest.py`. It is a local `local[1]` session unless you run tests on Databricks.
- Build small DataFrames with `spark.createDataFrame(...)`, run your rule through `with_quality_flags` or `cast_types`, and assert on `_dq_reasons`.
- pytest fixtures: https://docs.pytest.org/en/stable/how-to/fixtures.html

**Common pitfalls:**
- Test both the failing and the passing case for every rule. A test that only checks valid rows proves nothing.

**DONE WHEN:** `pytest -q` shows the 4 tests passed, not skipped

**SEE:** tests/test_quality.py:test_missing_supplier_rule

---

<a id="todo-04"></a>
## TODO-04: Dedup latest event

- [ ] Done

**Concept taught:** Window functions and the wide transformation they cause. A window partitioned by `shipment_id` moves rows with the same key to the same executor, which is a shuffle.

**WHAT:** `dedup_latest` with Window + row_number

**WHY:** A shipment can have several events. Silver should hold one row per shipment, the latest one, so that counts and sums are correct.

**PRODUCT:** Silver has exactly one row per shipment, and `SHP000001` shows its latest status.

**SCOPE:** ~8 lines, 25m, difficulty 2/3

**HINTS:**
- Look at `split_valid_quarantine` in `quality.py` to see the DataFrame style used in this repo.
- The order is `event_ts` descending, with `event_id` descending as the tie-break.
- `pyspark.sql.Window` is documented here: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html
- Window functions in Databricks SQL: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions
- `F.row_number()` is the function to number rows inside each window.

**Common pitfalls:**
- A `Window.orderBy` with no `partitionBy` puts all rows into one partition. It is correct but slow, and Spark warns about it.
- Filtering on `row_number` requires a second step: a window function cannot be used directly in a `filter`.

**DONE WHEN:** (with TODO-02 done) silver 1,200 rows, `count(distinct shipment_id) = count(*)`; SHP000002 status DELIVERED (its older day-1 event was DELAYED)

**SEE:** quality.py:split_valid_quarantine (DataFrame style)

> The `pytest` part is local. The silver counts are a Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-05"></a>
## TODO-05: Test dedup

- [ ] Done

**Concept taught:** Testing a transformation that changes the number of rows.

**WHAT:** test with 3 events / 2 shipments

**WHY:** Dedup is where silent data loss or duplication happens. A test with a known input shows that the newest event is kept.

**PRODUCT:** A regression test for dedup, with three events across two shipments.

**SCOPE:** ~20 lines, 15m, difficulty 1/3

**HINTS:**
- `tests/test_silver.py` has a `test_dedup_latest_keeps_newest_event` that currently skips. Replace the skip.
- Build three events: two for one shipment with different `event_ts`, and one for another shipment.
- Assert on the surviving row's status and on the total row count.
- pytest fixtures: https://docs.pytest.org/en/stable/how-to/fixtures.html

**DONE WHEN:** test passes

**SEE:** tests/test_quality.py:test_missing_supplier_rule

---

<a id="todo-06"></a>
## TODO-06: Supplier scorecard (SQL)

- [ ] Done

**Concept taught:** Spark SQL joins, CTEs, and `RANK()` as a window function.

**WHAT:** `build_supplier_scorecard`

**WHY:** Some people on the team write SQL rather than DataFrame code. This TODO shows that both produce the same plan, and it builds a per-supplier view on top of silver and `dim_supplier`.

**PRODUCT:** A supplier scorecard with delay rates and a rank, so the worst suppliers are easy to find.

**SCOPE:** ~25 lines, 30m, difficulty 2/3

**HINTS:**
- Look at `build_region_part` in `gold.py` for the aggregate pattern and at `dims.py` for SQL style.
- Use a CTE to compute per-supplier aggregates from the silver table, then join `dim_supplier` onto the aggregates (it is SCD1, so it has one row per supplier and no current-row filter is needed).
- Output columns: `supplier_id, supplier_name, country, rating, shipment_count, delayed_pct, avg_unit_cost_usd, delay_rank`.
- `delay_rank` is `RANK() OVER (ORDER BY delayed_pct DESC)`.
- The DONE WHEN count of 24 is for `supplier_version = 1`.
- SQL window functions: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions

**Common pitfalls:**
- Aggregate from silver first, then join the dim onto the aggregate. With `supplier_version = 2` the dim has 25 rows, so a dim-side join would give 25 rows instead of 24.
- `dim_supplier` is the SCD1 table with no `is_current` column, so the join needs no current-row filter.
- `RANK()` gives ties the same rank. Check that rank 1 really has the highest `delayed_pct`.

**DONE WHEN:** gold_supplier_scorecard has 24 rows, delay_rank 1 = highest delayed_pct

**SEE:** gold.py:build_region_part, dims.py (SQL style)

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-07"></a>
## TODO-07: Incremental bronze

- [ ] Done

**Concept taught:** Idempotency and incremental loads. A pipeline that can run twice without changing the result is idempotent.

**WHAT:** `list_new_files`

**WHY:** Daily batches are loaded again after failures and reruns. Without a file filter, every rerun appends the same rows again.

**PRODUCT:** Re-running `10_bronze` does not duplicate data. The bronze count stays at 1,260 after day 1.

**SCOPE:** ~5 lines + notebook, 30m, difficulty 2/3

**HINTS:**
- The reference `read_batch` in `bronze.py` sets `_source_file` from the file metadata column. The notebook already passes file basenames on both sides, so `list_new_files` is a set difference that keeps the original order.
- `10_bronze` has a TODO-07 cell. It calls `list_new_files(all_files, ingested_files)` and falls back to all files with a warning until you implement it.
- File metadata column: https://docs.databricks.com/aws/en/ingestion/file-metadata-column
- Volumes: https://docs.databricks.com/aws/en/volumes/

**Common pitfalls:**
- You should not need to convert path formats: both sides already hold basenames. Check the output on your runtime rather than assuming it.
- If the list is always empty, every file looks new. If it is never empty, nothing gets loaded.

**DONE WHEN:** running 10_bronze twice keeps bronze at 1,260 (day 1)

**SEE:** bronze.py:read_batch

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-08"></a>
## TODO-08: Silver MERGE

- [ ] Done

**Concept taught:** Delta `MERGE` as an upsert: match on a key, update when the source is newer, insert when there is no match.

**WHAT:** `merge_into_silver`

**WHY:** Day 2 contains status updates for shipments already in silver. Appending them would create duplicates, and rebuilding silver each time is slow and loses history of changes.

**PRODUCT:** Silver stays one row per shipment and takes the newest status. Running the job again changes nothing.

**SCOPE:** ~20 lines, 40m, difficulty 2/3

**HINTS:**
- Read `upsert_dim_supplier` in `dims.py`. It is the reference MERGE. Its comments explain each clause.
- Use the `s.event_ts > t.event_ts` guard in the `WHEN MATCHED` clause. It makes reprocessing safe.
- If the target table does not exist, create it from the updates. Look at the reference for the `tableExists` pattern.
- Set `USE_MERGE = True` in `20_silver` once this works.
- MERGE in Databricks: https://docs.databricks.com/aws/en/delta/merge

**Common pitfalls:**
- MERGE fails with an error about multiple source rows matching the same target row when the source has two rows with the same `shipment_id`. Dedup (TODO-04) must run before the MERGE.
- Using `spark.sql(...)` with a temp view is the approach the repo uses. Do not add the `delta-spark` Python package; it is not part of the setup.

**DONE WHEN:** (with TODO-02 and TODO-04 done) 00 days=2 → 10 → 20: silver 1,300, quarantine 50; SHP000150 status DELIVERED; rerun 20 → no change

**SEE:** dims.py:upsert_dim_supplier

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-09"></a>
## TODO-09: More DQ checks

- [ ] Done

**Concept taught:** Data quality checks at table level (uniqueness, not empty, reconciliation), severity, and fail-fast behaviour.

**WHAT:** 3 functions

**WHY:** Row-level rules catch bad rows. Table-level checks catch problems that no single row shows, such as duplicate keys or lost shipments. A critical failure should stop the job, not just print a warning.

**PRODUCT:** Each run writes check results to `dq_results` and fails with a clear message if a critical check fails.

**SCOPE:** ~25 lines, 30m, difficulty 2/3

**HINTS:**
- `check_no_shipment_lost` in `checks.py` is the reference. Its shape is `CheckResult(name, severity, passed, observed, expected)`.
- Implement `check_unique_key`, `check_not_empty`, and `raise_on_critical`. One TODO block covers all three.
- `raise_on_critical` should raise `RuntimeError` and list the names of failed critical checks.
- `results_to_df` already writes the results, so you only need to return `CheckResult` objects.

**Common pitfalls:**
- A check that always passes is worse than no check. Break one on purpose (for example, a duplicate key) to see it fail, then restore it.

**DONE WHEN:** dq_results has 3+ rows per run; temporarily break a check → notebook fails with RuntimeError

**SEE:** checks.py:check_no_shipment_lost

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-10"></a>
## TODO-10: Schema evolution

- [ ] Done

**Concept taught:** Schema enforcement (Delta rejects writes that do not match the table schema) and schema evolution (you allow a new column on purpose).

**WHAT:** handle day 3 `carrier` column: bronze append needs `mergeSchema`; the quarantine append in notebooks/20_silver.py also needs `mergeSchema`; the silver MERGE must use `UPDATE SET *` / `INSERT *` with per-statement schema evolution (`MERGE WITH SCHEMA EVOLUTION INTO ...`)

**WHY:** Day 3 adds a `carrier` column. Delta blocks that write by default, which protects downstream readers. You still need a deliberate way to accept a new column.

**PRODUCT:** The new column appears in bronze and silver, with NULL for older rows, and nothing else changes.

**SCOPE:** ~7 lines, 30m, difficulty 2/3

**HINTS:**
- The comment block in `10_bronze` explains the error you should see on day 3 before you change anything. Run it first.
- Schema evolution in Delta: https://docs.databricks.com/aws/en/delta/update-schema
- Read the schema evolution section of the MERGE page too: https://docs.databricks.com/aws/en/delta/merge
- Bronze: `mergeSchema` on the append write.
- Quarantine: the append in `20_silver.py` also needs `mergeSchema`.
- Silver: `MERGE WITH SCHEMA EVOLUTION INTO` with `UPDATE SET *` and `INSERT *`, so the new column is copied across. This is set per statement.
- The session config `spark.databricks.delta.schema.autoMerge.enabled` is an older session-wide alternative. Prefer the per-statement form.

**Common pitfalls:**
- Schema mismatch on day 3: the append fails with a schema error until you allow evolution. This error is the expected starting point, not a bug in the data.
- Prefer per-statement evolution (`mergeSchema` on a write, `WITH SCHEMA EVOLUTION` on a MERGE) over a session-wide config. Enable it only for the write that needs it.

**DONE WHEN:** 00 days=3 → 10 → 20: bronze 1,665, silver 1,350, `carrier` column exists, NULL for older rows

**SEE:** docs/CONCEPTS.md#schema-enforcement-vs-evolution

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-11"></a>
## TODO-11: Time travel + RESTORE

- [ ] Done

**Concept taught:** Delta time travel (`VERSION AS OF`), table history, and `RESTORE` as a new commit that rolls a table back.

**WHAT:** diff two versions (EXCEPT), `DELETE` Midwest rows, `RESTORE TABLE ... TO VERSION AS OF n`

**WHY:** Bad writes happen. Delta keeps earlier versions in the transaction log, so you can see what changed and roll back without rebuilding the table.

**PRODUCT:** You can show a diff between two versions, undo a bad delete, and point to the history entry that proves it.

**SCOPE:** ~10 lines SQL, 20m, difficulty 2/3

**HINTS:**
- The reference cell in `50_delta_ops` shows `DESCRIBE HISTORY` and `VERSION AS OF`.
- Use `EXCEPT` between a version and the current table to see what changed.
- `DELETE` on silver for region `Midwest`, then `RESTORE` to the version before the delete.
- History: https://docs.databricks.com/aws/en/delta/history
- RESTORE: https://docs.databricks.com/aws/en/sql/language-manual/delta-restore
- Time travel: https://docs.databricks.com/aws/en/delta/history

**Common pitfalls:**
- Write down the version number before the delete. Reading the version after the delete gives the wrong number.
- Time travel is limited by the retention of old files. Do not run `VACUUM ... RETAIN 0 HOURS` to save space; it removes what time travel needs. The default retention is seven days.

**DONE WHEN:** silver count back to pre-delete value; history shows DELETE and RESTORE ops

**SEE:** 50_delta_ops reference cell

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-12"></a>
## TODO-12: OPTIMIZE + liquid clustering

- [ ] Done

**Concept taught:** Small files and file layout. `OPTIMIZE` compacts many small files; liquid clustering chooses the columns the data is organised by, and it is incremental.

**WHAT:** `ALTER TABLE workspace.shipments.silver_shipments CLUSTER BY (region, event_date)`, `OPTIMIZE`, compare `DESCRIBE DETAIL` numFiles/clusteringColumns

**WHY:** Every write adds files. Many small files slow down reads. Clustering on the columns you filter by lets Delta skip files that cannot match.

**PRODUCT:** Silver is clustered on `region` and `event_date`, and you have before-and-after file counts to show the effect.

**SCOPE:** ~6 lines SQL, 25m, difficulty 2/3

**HINTS:**
- Write down `numFiles` before you run `OPTIMIZE` and again after it.
- `DESCRIBE DETAIL` shows `numFiles` and `clusteringColumns`.
- Liquid clustering: https://docs.databricks.com/aws/en/delta/clustering
- OPTIMIZE: https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize

**Common pitfalls:**
- A table can use liquid clustering or `ZORDER`, not both. Check the table properties before you add `ZORDER BY`.
- With a small test table, the file count may not change much. Say so in your notes instead of inventing a result.
- Liquid clustering behaviour on Free Edition is not yet verified by the scaffold author.

**DONE WHEN:** `clusteringColumns` shows both columns; write down numFiles before/after

**SEE:** 50_delta_ops

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-13"></a>
## TODO-13: Audit everywhere + query plan

- [ ] Done

**Concept taught:** Monitoring through an audit table, and reading a physical plan to find the shuffle (`Exchange`).

**WHAT:** call `log_run` in silver/gold/dq tasks; `explain("formatted")` gold query, find Exchange (shuffle) and explain why

**WHY:** A pipeline that writes no logs cannot be debugged after the fact. Reading the plan shows which steps move data between executors, which is usually where the time goes.

**PRODUCT:** `pipeline_runs` has one row per task per run, and you have a written note of where the shuffle is in the gold query.

**SCOPE:** ~10 lines, 30m, difficulty 2/3

**HINTS:**
- `log_run` in `audit.py` is the reference. `10_bronze` already calls it. Copy that pattern into `20_silver`, `30_gold`, and `40_quality_checks`.
- `DataFrame.explain` accepts a mode, for example `"formatted"`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
- Look for `Exchange` in the output. It marks a shuffle. Look for the `groupBy` or `join` that causes it.
- Write your explanation in `docs/NOTES.md`.

**Common pitfalls:**
- Log the row counts that the task actually wrote, not the counts from the input.
- Plan text on serverless may differ from classic Spark. Not yet verified by the scaffold author on Free Edition.

**DONE WHEN:** pipeline_runs has a row per task per run; note in docs/NOTES.md where the shuffle is

**SEE:** audit.py:log_run, 10_bronze

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-14"></a>
## TODO-14: Complete the job (bundle)

- [ ] Done

**Concept taught:** Jobs as code. A bundle describes tasks, their dependencies, and their parameters in YAML. You check it with `validate`, and this project never deploys it.

**WHAT:** add silver, gold, quality_checks tasks with depends_on + run_id param; install Databricks CLI; `databricks bundle validate` (NO deploy)

**WHY:** A pipeline you can only run by clicking through the UI is hard to review or repeat. A YAML job with explicit `depends_on` documents the order.

**PRODUCT:** The job has all the pipeline tasks in order, and `databricks bundle validate` passes.

**SCOPE:** ~20 lines YAML, 30m, difficulty 2/3

**HINTS:**
- Look at the `bronze` task in `resources/shipments_job.yml`. Each new task should follow the same shape: notebook path, `base_parameters`, and `depends_on`.
- Pass `run_id: "{{job.run_id}}"` through the job parameters, as the existing tasks do.
- Dynamic value references: https://docs.databricks.com/aws/en/jobs/dynamic-value-references
- Bundle resources: https://docs.databricks.com/aws/en/dev-tools/bundles/resources
- Install the CLI: https://docs.databricks.com/aws/en/dev-tools/cli/install
- Bundles overview: https://docs.databricks.com/aws/en/dev-tools/bundles/

**Common pitfalls:**
- Do not add `bundle deploy` or `bundle run` anywhere. This project never deploys.
- `validate` may need CLI authentication to a workspace. Not yet verified by the scaffold author.
- Keep the job on serverless: do not add cluster configuration.

**DONE WHEN:** `databricks bundle validate` prints "Validation OK!"

**SEE:** resources/shipments_job.yml bronze task

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-15"></a>
## TODO-15: CI tests job

- [ ] Done

**Concept taught:** CI stages. Continuous integration runs the same tests you run locally, on every push, without deploying anything.

**WHAT:** add a job: setup-java 17, python 3.12, `pip install -r requirements-dev.txt`, `pytest -q`; NO deploy steps

**WHY:** Tests that only run on one laptop do not protect the repository. A CI job makes the test command reproducible for anyone who opens a pull request.

**PRODUCT:** A GitHub Actions job that installs Java 17 and Python 3.12, installs the dev requirements, and runs `pytest -q`.

**SCOPE:** ~15 lines YAML, 20m, difficulty 1/3

**HINTS:**
- `.github/workflows/tests.yml` already exists from the original project. Add a job next to the existing one; do not replace it.
- `actions/setup-java` takes a Java distribution and version: https://github.com/actions/setup-java
- Use the same Python and Java versions as the README.
- pytest fixtures: https://docs.pytest.org/en/stable/how-to/fixtures.html

**Common pitfalls:**
- Do not add a step that uses Databricks credentials or deploys a bundle.
- PySpark needs Java on the runner. If the job fails on Spark startup, check the Java step first.

**DONE WHEN:** `act`-free check: YAML valid + same commands pass locally

**SEE:** existing tests.yml

---

<a id="todo-16"></a>
## TODO-16: Stretch: SCD2 dim

- [ ] Done

**Concept taught:** Slowly changing dimension type 2: keep history by closing the old row and inserting a new one, with `valid_from`, `valid_to`, and `is_current`.

**WHAT:** `upsert_dim_supplier_scd2` writing to a new table `dim_supplier_scd2` (SCD1 `dim_supplier` lacks `valid_from`, `valid_to`, `is_current`); add and fill the TODO-16 cell in `notebooks/20_silver.py` that runs v1 then v2

**WHY:** SCD1 overwrites the rating of a supplier, so you can no longer see what it was last month. SCD2 keeps every version, which matters when a report asks what was true at the time.

**PRODUCT:** `dim_supplier_scd2` keeps the old rating of SUP001 as an expired row, next to the new one.

**SCOPE:** ~35 lines, 60m, difficulty 3/3

**HINTS:**
- Read `upsert_dim_supplier` first. It is SCD1 and shows the MERGE pattern in this repo.
- Columns: `valid_from`, `valid_to`, `is_current`.
- MERGE: https://docs.databricks.com/aws/en/delta/merge
- Think about the two actions for a changed supplier: close the current row, and insert a new current row. Work out which rows need each action before you write any SQL.

**Common pitfalls:**
- One MERGE cannot both expire the old row and insert its new version for the same key. Think about how to give the new version a source row that does not match.
- Re-running the same version must not add rows again.

**DONE WHEN:** dim_supplier_scd2 after v1 then v2: 28 rows, 25 with is_current=true, SUP001 has 2 rows

**SEE:** dims.py:upsert_dim_supplier

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-17"></a>
## TODO-17: Stretch: broadcast join

- [ ] Done

**Concept taught:** Join strategies. A broadcast hash join sends a small table to every executor and avoids shuffling the large side. Adaptive Query Execution (AQE) can change the plan at runtime.

**WHAT:** join silver with dim_supplier with and without `F.broadcast`, compare `explain()` (BroadcastHashJoin vs SortMergeJoin), read about AQE

**WHY:** Joining a 25-row supplier dimension to a large fact table with a shuffle moves far more data than needed. Knowing which join Spark picked, and why, is a core tuning skill.

**PRODUCT:** A written comparison of the two join plans, with the reason for the difference.

**SCOPE:** ~10 lines, 30m, difficulty 3/3

**HINTS:**
- `F.broadcast` marks a DataFrame for broadcast: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html
- Compare the physical plans with `explain()`. Look for `BroadcastHashJoin` and `SortMergeJoin`.
- AQE: https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
- Write your comparison in `docs/NOTES.md`.

**Common pitfalls:**
- Do not compare run times from a small table as if they were a benchmark. Compare the plans, and say what the timing does and does not show.
- Spark may pick a broadcast join without the hint if the table is small enough. Check the plan with and without the hint.
- AQE and the broadcast threshold may behave differently on serverless. Not yet verified by the scaffold author on Free Edition.

**DONE WHEN:** written comparison in docs/NOTES.md

**SEE:** 30_gold

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

<a id="todo-18"></a>
## TODO-18: Stretch: cost awareness

- [ ] Done

**Concept taught:** Cost monitoring with system tables. Serverless compute is billed by usage, so you need to see which SKUs and jobs spend the units.

**WHAT:** query `system.billing.usage` for last 7 days (DBUs by sku); if not accessible on Free Edition, record the error

**WHY:** A pipeline that works but costs more than expected is still a problem. Knowing where the spend goes is part of running data platforms.

**PRODUCT:** A query over the last seven days of usage by SKU, or a written record of why it could not run.

**SCOPE:** ~8 lines SQL, 20m, difficulty 2/3

**HINTS:**
- The billing system table is documented here: https://docs.databricks.com/aws/en/admin/system-tables/billing
- Group by the SKU name and sum the usage quantity for the last seven days.
- Free Edition limits are described here: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- Put the result, or the error text, in `docs/NOTES.md`.

**Common pitfalls:**
- System tables may be unavailable or restricted on Free Edition. That is not yet verified by the scaffold author. Recording the error with the exact message is a valid outcome for this TODO.

**DONE WHEN:** query returns rows OR error documented in docs/NOTES.md

**SEE:** 60_monitoring

> Databricks-only check: not yet verified by the scaffold author on Free Edition.

---

## Total

Total: core ≈ 6¾ h (405 min), stretch ≈ 1¾ h (110 min), ≈ 8½ h overall
