# Stage 01: Run and read the pipeline (TODO-01)

> Tutor: read `docs/tutor/README.md` first. Explain in Russian; keep code and identifiers in English. TODO-01 has **no code to
> write**. The learner runs the notebooks in order and reads the output with you. Expected numbers come from
> `docs/SCAFFOLD_SPEC.md` section 1 and were derived by the scaffold author, but the Databricks run itself is **not verified
> on Free Edition**. If a number differs, do not assume the learner is wrong: find out which notebook step differs first.

## 1. Goal in one sentence

The learner runs the original notebook and then the reference path `00 (days=1) → 10 → 20 → 30 → 40 → 50 → 60`, checks the
day-1 counts, and can read a Delta table's history and a query plan.

**Interview afterwards:** "I can walk through a medallion pipeline, say what each layer holds, and show the plan and history of a
Delta table."

## 2. Before you start

- Stage 00 done: a Git folder from the learner's fork, a notebook attached to Serverless, `spark.range(5).show()` works.
- Open together: `docs/CONCEPTS.md` sections "Medallion architecture", "Delta Lake and the transaction log", "Lazy
  evaluation", "Query plans and explain"; `docs/TODO.md` TODO-01; the README table "Run on Databricks Free Edition", step 5.
- Files the learner will read: `notebooks/01_bronze_silver_gold.py` (original, do not edit),
  `notebooks/00_generate_batches.py`, `notebooks/10_bronze.py`, `notebooks/20_silver.py`, `notebooks/30_gold.py`,
  `notebooks/40_quality_checks.py`, `notebooks/50_delta_ops.py`, `notebooks/60_monitoring.py`.
- Set expectations: **no TODO is done yet**, so the reference path has known gaps. That is the baseline.

## 3. Concepts to teach first

### 3.1 Medallion layers

**Plain explanation.** Bronze keeps raw data as it arrived. Silver holds cleaned, validated, de-duplicated rows. Gold holds
aggregates for one question.

**Analogy.** A warehouse. Bronze is the receiving dock (boxes as delivered), silver is the checked stock on the shelves, gold is
the sales report made from the shelves.

**Where in this repo.** `bronze.read_batch` (bronze, all columns are text), `silver.py` and `quality.split_valid_quarantine`
(silver plus quarantine), `gold.build_region_part` (gold).

### 3.2 Delta tables and the transaction log

**Plain explanation.** A Delta table is Parquet data files plus a `_delta_log` folder. The log is a list of commits. Each write
adds a commit. Readers see a consistent version.

**Analogy.** `git log` for a table: every change is a commit you can list, compare, or go back to.

**Tiny example** (after `20_silver` has run):
```python
spark.sql("DESCRIBE HISTORY workspace.shipments.silver_shipments").select("version", "operation").show()
```
Expected: one or more rows, versions counting from 0. The operation names are expected to be write-like (for example WRITE or a
create-table-as-select); the exact text is not verified by the scaffold author.

### 3.3 Catalog, schema, table, volume

**Plain explanation.** Names are `catalog.schema.table`. Raw CSV files live in a volume, `/Volumes/workspace/shipments/raw`.

**Where in this repo.** `config.PipelineConfig.table()` builds names; `00_generate_batches` runs `CREATE SCHEMA` and
`CREATE VOLUME`. Expected tables after the full reference run: `bronze_shipments`, `silver_shipments`,
`quarantine_shipments`, `dim_supplier`, `gold_region_part`, `dq_results`, `pipeline_runs`.

**Tiny example:**
```python
display(dbutils.fs.ls("/Volumes/workspace/shipments/raw"))
```
Expected after 00 runs: one file, `shipments_2026-10-01.csv`.

### 3.4 Lazy evaluation: transformations versus actions

**Plain explanation.** Transformations (`select`, `filter`, `withColumn`, `groupBy`) only build a plan. Actions (`count`,
`show`, `collect`, `display`, writes) make Spark run it.

**Analogy.** A cook writes the order on a ticket (transformation). The food is made when the guest asks for the bill (action).

**Tiny example:**
```python
from pyspark.sql import functions as F
big = spark.range(1_000_000).filter(F.col("id") % 2 == 0)   # returns at once: nothing ran
print(big.count())                                          # the action: runs the plan, prints 500000
```
Note: Python allows `1_000_000` with underscores. Expected count: 500000.

### 3.5 `explain()`: reading the plan

**Plain explanation.** `DataFrame.explain()` prints the physical plan without running it. Read it from the bottom up: the
bottom is the scan of data, the top is the final result. Words to look for: `Scan`, `Filter`, `Exchange` (a shuffle), joins.

**Analogy.** A recipe card printed in order of work. The first steps are the ingredients.

**Tiny example:**
```python
spark.range(10).filter("id > 5").explain()
```
Expected: a plan with a range or scan step at the bottom and a filter above it. Plan text varies with the runtime (expected, not
verified on serverless).

**Where in this repo.** TODO-01 explains the saved `gold_region_part` table. A plain table scan usually shows **no** `Exchange`,
because the aggregate was computed when the table was written. The shuffle appears when you explain the `groupBy` itself. That
is TODO-13, in stage 10. Say this to the learner so they do not think something is broken.

### 3.6 Widgets and the run order

Explain the widget `days` (from stage 00) and that the order matters: 00 writes files, 10 reads files into bronze, 20 reads
bronze, 30 and 40 read silver, 50 and 60 read the history and the logs.

## 4. Step-by-step plan

1. **Run the original notebook** `notebooks/01_bronze_silver_gold`. Attach Serverless, then Run all. It creates its own schema
   `ai_ready_demo`. Do not edit it.
   *Check:* the last printed lines include `VALIDATED — actual Delta tables`, `Source rows: 1260`, `Accepted rows: 1200`,
   `Quarantined rows: 60`. Note: these are the original author's numbers, not the scaffold's.
2. **Run `00_generate_batches` with `days = 1`.** Set the widget in the top bar.
   *Check:* it prints `wrote day 1: /Volumes/workspace/shipments/raw/shipments_2026-10-01.csv`, and the last listing shows the file.
3. **Run `10_bronze`.**
   *Check:* `appended 1260 rows from shipments_2026-10-01.csv` and `Bronze rows written in this run: 1260`.
4. **Run `20_silver`** (keep `supplier_version = 1` and `USE_MERGE = False`).
   *Check:* `dim_supplier rows after version 1: 24`; a `WARNING: TODO-04 not done yet` line; four lines `skipping rule_...: not
   implemented yet`; `SILVER rows: 1248`; `QUARANTINE rows: 12`. The quarantine chart shows only `missing_supplier`.
5. **Run `30_gold`.**
   *Check:* `gold_region_part` shows 16 rows; the scorecard cell prints `TODO-06 not done yet - skipped`.
6. **Run `40_quality_checks`.**
   *Check:* the results table has one row, `no_shipment_lost`, passed. TODO-09 cells print "not done yet".
7. **Run `50_delta_ops`.**
   *Check:* the history table lists versions for silver; the line `silver rows now: 1248; at VERSION AS OF 0: 1248` appears
   (both equal on day 1 is expected).
8. **Run `60_monitoring`.**
   *Check:* `pipeline_runs` has one row: task `bronze`, rows 1260, status SUCCESS. Silver and gold are not logged until TODO-13.
9. **Scratch cell (Python), at the bottom of `60_monitoring`:**
   `spark.table(cfg.table("gold_region_part")).explain()`
   *Check:* the learner names the bottom step (the scan) and the top step (the result).
10. **Scratch cell (SQL):** `DESCRIBE HISTORY` on silver, using the Section 3.2 example.
    *Check:* the learner names two operations and explains what a version number means.
11. **Optional teaser:** tell the learner that TODO-13 (stage 10) runs `explain("formatted")` on the `groupBy` query, where the
    `Exchange` appears. Do not explain it yet.

## 5. Hint ladder per TODO

### TODO-01 (run and explore)

| Level | What you give |
|---|---|
| 0 Goal | "Done = the run order works, the four day-1 counts match, and you have read one plan and one history." |
| 1 Concept | "Each notebook writes tables the next one reads. Trace the chain: 00 writes files, 10 writes bronze, 20 writes silver and quarantine, 30 writes gold." |
| 2 Tool | "Use `DataFrame.explain()` (link in TODO-01) and SQL `DESCRIBE HISTORY <table>`. Build table names with `cfg.table(config.SILVER)` or `cfg.table(config.GOLD_REGION_PART)`." |
| 3 Skeleton | `plan_df = spark.table(cfg.table(___))` then `plan_df.explain(___)`. In a SQL cell: `DESCRIBE HISTORY ___`. Ask: which table name, and which explain mode? |
| 4 Solution | Described, not written: "walk through the two scratch cells line by line, then have the learner retype them." |

Note for the tutor: TODO-01 adds no code to the repo. The "done" state is the run output plus the learner's own notes.

## 6. Common mistakes and error messages

| Symptom | Cause | What to ask or suggest |
|---|---|---|
| Original notebook fails with `AssertionError: Select a Unity Catalog workspace catalog before running this notebook.` | The current catalog is `hive_metastore` or unset. | "Run `SELECT current_catalog()`. Which catalog is it?" Not a pipeline bug. |
| `FileNotFoundException` or "No such file or directory" from `dbutils.fs.ls` (expected wording) | `00` was not run, or the widget `catalog`/`schema` points elsewhere. | Check the widgets, then run `00` first. |
| `[TABLE_OR_VIEW_NOT_FOUND]` for `workspace.shipments.silver_shipments` | `20_silver` has not completed, or the schema widget is wrong. | "Which notebook last succeeded?" Check the run order. |
| `10_bronze` shows 2,520 rows | Ran twice. Until TODO-07 is done, every run appends every file. | This is the known bug. Record it. Do not drop tables without the learner's explicit decision. |
| `60_monitoring` prints `pipeline_runs does not exist yet - run 10_bronze first` | 10 has not run. | Run 10, then 60. |
| Gold is not 16 rows | Silver is wrong, or the widget changed. | Compare silver first, then gold. Gold has one row per region and part type. |
| Learner thinks `explain()` "did nothing" | Transformations are lazy; `explain()` prints the plan only. | Show the Spark job list appears only after an action (expected, not verified on serverless). |
| Learner sees no `Exchange` in the gold table plan | A saved table is a plain scan (see 3.5). | Explain the `groupBy` in TODO-13, not the table. |
| Learner confuses the original `VALIDATED` (1,200 + 60) with the scaffold's 1,248 | Two different pipelines and schemas. | Original: schema `ai_ready_demo`, its own data. Scaffold: `workspace.shipments`. |
| Widget keeps an old value | Widgets remember their last value. | Change the value in the top bar and run again. |
| Silver shows 1,248 and the learner expects 1,200 | Dedup (TODO-04) and the new rules (TODO-02) are not done. | Expected at this stage. Explain why, with the 15 duplicate events built in `_rows_day1()` (`src/shipments_pipeline/generator.py`). |

## 7. DONE WHEN

From `docs/TODO.md`, copied exactly:

> original prints `VALIDATED`; bronze 1,260, silver 1,248, quarantine 12, gold 16 rows

## 8. Check-your-understanding questions

1. Why is the original notebook's `VALIDATED` not the same check as the scaffold's `1,248`? *Key points: different schema and
   data; the scaffold keeps the same kind of flow but its own tables.*
2. What does "lazy" mean, and which of these is an action: `filter`, `count`, `withColumn`, `show`? *Key points: lazy means
   nothing runs yet; `count` and `show` are actions.*
3. Why is the quarantine 12 and not 45 on day 1? *Key points: only the `missing_supplier` rule exists; TODO-02 adds four
   rules.*
4. Why is silver 1,248 and not 1,200? *Key points: 15 extra events for existing shipments are not removed until TODO-04; other
   bad rows pass until TODO-02.*
5. What does `DESCRIBE HISTORY` show, and why do you use it rather than reading files? *Key points: the commit log; each write is a
   version; operations and timestamps are listed.*
6. Read a plan from the bottom up. Where is the data read? *Key points: the scan at the bottom.*
7. Why must you never run `VACUUM ... RETAIN 0 HOURS` here? *Key points: it deletes files that time travel and RESTORE need
   (`docs/CONCEPTS.md`, VACUUM).*
8. What is a volume, and why are the CSV files there and not in a table? *Key points: volumes are storage for files; bronze reads
   them as text.*

## 9. Interview link

- "What does lazy evaluation mean in Spark?" [GENERAL] Junior
- "What is the difference between transformations and actions?" [GENERAL] Junior
- "How do you read a query plan (`explain`) and find `Exchange`, `BroadcastHashJoin` and `SortMergeJoin`?" [PROJECT] Mid
- "What is the Unity Catalog three-level namespace?" [GENERAL] Junior
- "What is a Unity Catalog volume, and when do you use it instead of a table?" [PROJECT] Mid
- "How does the transaction log work, and what are checkpoints?" [GENERAL] Mid

Sentence for the learner: "The pipeline moves data from raw files to bronze, then to silver with quality rules, then to gold
aggregates, and every Delta write is a version I can inspect."

## 10. Wrap-up

- Tick `- [ ] Done` for TODO-01 in `docs/TODO.md` and the matching checkbox in `README.md` section "My implementations" (only
  after the DONE WHEN check passed).
- Commit (the learner runs it): `git add -A && git commit -m "feat(todo-01): run pipeline baseline and read plan and history"`.
  Check first that `git status` shows only the checkbox edits and the learner's own notes, not `.venv`.
- Next stage: `docs/tutor/stage-02-quality-rules.md` (TODO-02 and TODO-03).

**Say it like this (RU):**

> "Bronze — это приёмка на складе: коробки записаны такими, как пришли. Silver — проверенная полка, gold — отчёт, который
> считают с полки."

> "Ленивость Spark — как повар по заказу: сначала записываем, что хотим, а готовим, когда гость просит счёт. `count()` и
> `show()` — это и есть «просить счёт»."

> "`explain()` — это распечатанная рецептура. Читаем снизу вверх: сначала продукты, потом шаги, в конце блюдо."
