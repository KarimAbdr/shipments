# Interview Prep — Shipments Lakehouse (Databricks, Azure-focused)

Audience: a junior Data / Databricks Engineer candidate on the European job market (Azure-first).

## How to use this document

- **Format.** Every question is `**Q:**`, followed by a tag and a model answer of 3–6 sentences (lists are used only where a list is the natural shape). Each technical question is also labelled **Junior**, **Mid** or **Stretch**.
- **Tags.** `[PROJECT]` answers name a file, function or TODO from `docs/SCAFFOLD_SPEC.md` and describe the *design*. `[GENERAL]` answers are conceptual and are not about this repo.
- **"(after TODO-NN)".** Where an answer only holds once a TODO is implemented, the answer says so. Do not claim a project result before you have produced it.
- **Templates.** Section 2 STAR answers are drafts. Replace every bracketed placeholder with your own real experience. Do not invent employers, metrics or outcomes.
- **Honesty.** The project does **not** deploy anything, does not use streaming, Auto Loader, Lakeflow Declarative Pipelines (DLT), Terraform, or create any Azure resources. Where the project deliberately does not do something, the answer says so and then answers the concept.
- **Verification status.** The scaffold author verified locally: the generator row counts, pure-Python tests and PySpark unit tests on local Spark. The Databricks-specific behaviour (volumes, MERGE on serverless, liquid clustering, system tables, `databricks bundle validate`, `99_run_tests`, autoreload) is **expected to work but not yet verified on Free Edition**. Confirm it yourself and tick it off before you quote it as fact.
- **Free Edition.** Free Edition is serverless-only and has quotas and feature limits. I have not mapped every limit for this project; treat any Free-Edition-specific statement here as something to re-check on the official limitations page.

---

## 1. Project pitch

### 1.1 Pitch

**Q:** Give me the 30-second version of your project. [PROJECT]

I built a small medallion pipeline on Databricks Free Edition with serverless compute and Unity Catalog. Daily CSV batches land in a Unity Catalog volume, bronze keeps the raw strings plus file lineage, and a rule framework routes bad rows to a quarantine table instead of dropping them. Silver is deduplicated and upserted with a Delta MERGE, gold holds two aggregate tables, and every run writes data-quality results and per-task audit rows. The pure transforms are unit-tested with pytest, and the job is an Asset Bundle that I validate but never deploy. (After TODO-02, TODO-04, TODO-08, TODO-09, TODO-13 and TODO-14.)

**Q:** Give me the two-minute version. [PROJECT]

The domain is shipment events: a shipment gets status updates over several days, and some source rows are wrong. `00_generate_batches` writes three synthetic CSV files into `/Volumes/workspace/shipments/raw`: day 1 has 1,260 rows with 45 deliberately bad ones, day 2 has 305 rows (updates, new shipments and 5 unknown statuses), and day 3 has 100 rows with an extra `carrier` column. Bronze reads every column as a string, adds `_source_file` and `_ingested_at`, and appends to `bronze_shipments`; TODO-07 makes that step incremental so a rerun does not duplicate files. Silver casts with `try_cast`, applies a rule framework that writes a reason string per row, sends failing rows to `quarantine_shipments`, keeps the latest event per shipment (TODO-04), and MERGEs into `silver_shipments` with an `event_ts` guard (TODO-08). Gold rebuilds `gold_region_part` by overwrite and `gold_supplier_scorecard` in SQL with `RANK()` (TODO-06), while checks write `dq_results` and `pipeline_runs` records the work each task did. I would stress the reasoning rather than the tooling: every decision either keeps data recoverable or makes a rerun harmless.

### 1.2 Architecture walkthrough

**Q:** Walk me through the architecture from bronze to gold. [PROJECT]

Notebooks `00` to `60` only do I/O and wiring; the logic lives in `src/shipments_pipeline/`. `00_generate_batches` writes CSVs to the volume; `10_bronze` calls `bronze.read_batch` and appends to `bronze_shipments`. `20_silver` runs `quality.cast_types`, `with_quality_flags` and `split_valid_quarantine`, then `dedup_latest` and `merge_into_silver`, and upserts `dim_supplier` through `dims.upsert_dim_supplier`. `30_gold` writes `gold.build_region_part` and `gold.build_supplier_scorecard`, and `40_quality_checks` writes `dq_results`. Bronze is the append-only record of what arrived, silver is the cleaned current state per shipment, and gold is the business aggregate. (After TODO-04, TODO-06, TODO-07, TODO-08, TODO-09 and TODO-13.)

**Q:** How does the quarantine work? [PROJECT]

Each rule in `quality.py` returns a Column that is a reason string when the rule fires and null otherwise. `with_quality_flags` concatenates the active reasons with `concat_ws("; ")` into `_dq_reasons`, and `split_valid_quarantine` sends rows with an empty reason string to silver and keeps flagged rows in `quarantine_shipments` with the reasons column intact. That means an analyst can see *why* a row was rejected and replay it after a fix. On day 1 the target is 45 quarantined rows across five distinct reasons (`missing_supplier`, `invalid_quantity`, `invalid_unit_cost`, `invalid_date`, `invalid_status`), which needs TODO-02. The reconciliation check `check_no_shipment_lost` in `checks.py` proves no shipment disappears: distinct `shipment_id` in bronze must equal silver plus quarantine. (After TODO-02 for the five-reason count.)

**Q:** How does the MERGE into silver work? [PROJECT]

`merge_into_silver` (TODO-08) matches on `shipment_id`: `WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *` and `WHEN NOT MATCHED THEN INSERT *`. The `event_ts` guard means an older or replayed event cannot overwrite newer state, and an identical rerun changes nothing, which is the check to run after TODO-08 (rerun `20_silver` and confirm counts do not move). Day 1 does not MERGE at all: `merge_into_silver` creates the table from the batch when it does not exist yet, so if dedup were skipped, the 15 shipments that appear twice in day 1 would simply be stored twice, which is a silent duplicate, not an error. Duplicate source rows that match an existing target row are what make Delta fail with "multiple source rows matched", so a later batch with two events for one already-loaded shipment would trigger it. `dedup_latest` (TODO-04) runs first so the source has one row per key. The `USE_MERGE` flag in `20_silver` lets the notebook fall back to a full refresh while the MERGE is still being built. (After TODO-04 and TODO-08.)

**Q:** What do `dq_results` and `pipeline_runs` record, and how are they different? [PROJECT]

`checks.py` defines `CheckResult` (name, severity, passed, observed, expected) and `results_to_df` adds `run_id` and `checked_at`; `40_quality_checks` appends those rows to `dq_results`. `audit.log_run` appends one row per task to `pipeline_runs` (`run_id`, `task`, `target_table`, `rows`, `status`, `logged_at`), which `60_monitoring` reads. The two tables answer different questions: `dq_results` says whether the data was acceptable, and `pipeline_runs` says what each task did and when. Critical failures stop the job through `raise_on_critical`. (After TODO-09 for three-plus checks per run and the fail-fast behaviour; after TODO-13 for a row per task.)

**Q:** What does the Asset Bundle contain, and what does it *not* do? [PROJECT]

`databricks.yml` names the bundle `shipments-lakehouse`, includes `resources/*.yml`, defines `catalog` and `schema` variables, excludes large or non-pipeline folders from sync, and has one `dev` target in development mode. `resources/shipments_job.yml` defines the job `shipments-pipeline` with `generate` and `bronze` tasks, passing `catalog`, `schema` and `run_id` (`{{job.run_id}}`); with no compute block the job is meant to run on serverless. The file header says "validate only", and that is the honest scope: I run `databricks bundle validate` and never `deploy`. (After TODO-14 for the silver, gold and quality tasks. The serverless run on Free Edition is not yet verified.)

### 1.3 Why these choices

**Q:** Why do you keep raw strings in bronze? [PROJECT]

`bronze.read_batch` uses `spark.read.option("header", True).csv(path)` with no `inferSchema`, so every column is a string. Bronze is meant to be a faithful record of what arrived, so a bad value like `not-a-date` or `0` is kept rather than failing the load or being silently coerced. Typing happens in silver through `quality.cast_types` with `try_cast`, which returns NULL instead of raising under ANSI mode, and the rules then explain each NULL. The cost is extra casting and storage, which I accept because if a rule changes I can re-derive silver from bronze without re-reading the source.

**Q:** Why quarantine bad rows instead of dropping them? [PROJECT]

Dropping rows hides both the data problem and the fact that a problem existed, so the business cannot fix the source. A quarantine table keeps every rejected row with its `_dq_reasons`, so the count reconciles (bronze shipments = silver plus quarantine) and the rows can be replayed after a rule or source fix. It also gives a natural place for a dashboard of rejects by reason. The trade-off is that someone has to own the quarantine, so the design includes the reconciliation check to make sure it is not ignored. (After TODO-02 for the rule set and TODO-09 for the checks.)

**Q:** Why pure functions and pytest? [PROJECT]

Transforms are `DataFrame -> DataFrame` functions in `src/`, so a unit test can build a three-row DataFrame and assert on the output without running a notebook. Notebooks only do I/O and wiring, which keeps them short and reviewable. The reference test `test_missing_supplier_rule` runs on local PySpark with `local[1]`, and the same code runs on Databricks through the session fixture in `tests/conftest.py`. The honest caveat is that the local tests need Java 17 and Python 3.12, and the Databricks path (`99_run_tests`) is expected to work but is not yet verified by the scaffold author.

**Q:** Why MERGE with an `event_ts` guard, instead of a plain overwrite or a blind upsert? [PROJECT]

A plain overwrite of silver from all bronze would be simple but would rewrite the whole table every run and would not show the upsert pattern. A blind upsert would let an out-of-order or replayed older event overwrite a newer status, which is a silent correctness bug. The guard `s.event_ts > t.event_ts` makes the upsert order-independent for the latest event, and it makes reruns harmless. It depends on the dedup step, so the source has one row per key. (After TODO-04 and TODO-08.)

**Q:** Why overwrite the small gold tables instead of doing incremental updates? [PROJECT]

`gold_region_part` has 16 rows (4 regions times 4 part types) and is recomputed from silver on every run with `mode("overwrite")`. For a table that small a full rebuild is easier to reason about than an incremental MERGE: it is idempotent by construction and has no stale rows to clean up. The trade-off is that cost grows with silver, so at larger scale I would switch to incremental aggregation or a MERGE. `gold_supplier_scorecard` (TODO-06) follows the same full-rebuild pattern.

**Q:** Why volume-based batches and file-level incremental loading? [PROJECT]

A UC volume gives a simple landing zone without external storage setup, and `_source_file` gives every bronze row lineage back to its file. `list_new_files` (TODO-07) skips files already present in bronze, so rerunning `10_bronze` is harmless, and silver uses the same lineage through `unprocessed_bronze` to process only new files. The limit is that file-level tracking does not handle a file rewritten in place, and in production I would use Auto Loader, which tracks files with its own checkpoint. (After TODO-07.)

**Q:** Why serverless compute? [PROJECT]

Serverless means the bundle contains no cluster configuration, and it is the compute that Free Edition provides. The constraints push good habits: no `spark.sparkContext`, no RDDs, no `dbfs:/` paths and no cluster-level configs, so everything goes through Unity Catalog and the DataFrame API. I did not measure cost or startup time for this project, and I would say so if asked; the cost story is a TODO-18 question and Free Edition quotas are not fully mapped.

### 1.4 Hardest part and improvements

**Q:** What was the hardest part of the project? [PROJECT]

Template: replace this with what you actually hit. The spec points to three likely candidates: making MERGE idempotent so that rerunning `20_silver` changes nothing (TODO-08), handling the day-3 `carrier` column without breaking the append (TODO-10), and choosing a deterministic tie-break in `dedup_latest` (TODO-04). Describe one concrete symptom, how you narrowed it down (Spark UI or query plan, `DESCRIBE HISTORY`, a unit test that reproduced it), and what you changed. Keep it to the one you can explain line by line.

**Q:** What would you improve next? [PROJECT]

- Replace hand-rolled file tracking (`list_new_files`) with Auto Loader, and the hand-written rules with Lakeflow Declarative Pipelines expectations, once the manual version is understood.
- Make the rule set data-driven (a rule catalogue with name, severity and owner) so that `dq_results` and the docs cannot drift apart.
- Add a schema contract for the raw CSV (column names, types, nullability) and check it in bronze before append, rather than discovering drift through a failed write.
- Build a quarantine replay path: fix a rule, reprocess only the quarantined rows, and record the replay in `pipeline_runs`.
- Add job-failure notifications and a freshness check, so a missed daily batch is detected even when no task fails.
- Use a proper dev environment with a service principal and a CI deploy stage, instead of validate-only.
- Set job timeouts and retry policy explicitly per task, and add budget tags so cost is attributable.
- Add a late-arriving-data test: a day-2 event with an older `event_ts` than a day-1 event must not override it.

### 1.5 Scaling to production

**Q:** How would you move this to Auto Loader or streaming? [PROJECT]

Today `bronze.list_new_files` (TODO-07) plus a batch read is the incremental mechanism, and the project deliberately does not use streaming or Auto Loader. In production I would replace the file-listing logic with an Auto Loader `cloudFiles` stream into bronze, using a checkpoint location and `trigger(availableNow=True)` for scheduled batch-style runs. Schema inference and evolution would be handled by Auto Loader's schema location, which replaces the `mergeSchema` handling in TODO-10. Streaming is not used here, so I would explain the difference conceptually rather than claim I ran it.

**Q:** How would you use Lakeflow Declarative Pipelines expectations for data quality? [PROJECT]

The project's `quality.py` rule framework maps directly onto expectations: a rule that should only warn becomes `EXPECT`, a rule that should drop the row becomes `EXPECT ... ON VIOLATION DROP ROW` (or `expect_or_drop` in Python), and a rule that must stop the pipeline becomes `ON VIOLATION FAIL UPDATE`. The quarantine is the part expectations do not give you by default, so I would keep a quarantine table fed by the dropped rows. The project does not use Lakeflow Declarative Pipelines, so this is a design comparison, not an experience claim.

**Q:** How would you handle service principals and secrets in production? [PROJECT]

The project has no deploy identity: the bundle is validate-only and CI runs only tests. In production I would run the job and the CI deploy as a Microsoft Entra ID service principal, with the job's `run_as` set to that principal, and never a personal user. Secrets would live in an Azure Key Vault-backed secret scope, read with `dbutils.secrets.get`, and the CI would authenticate with workload identity federation (OIDC) rather than a stored client secret. This is a design I would apply, not something this repo implements.

**Q:** How would you set up dev, test and prod targets? [PROJECT]

`databricks.yml` already has one target `dev` with `mode: development`, which prefixes deployed resources and pauses schedules so developers do not collide. I would add `test` and `prod` targets that differ only in variables such as `catalog` and `schema` (for example `dev_shipments`, `prod_shipments`), so the same code and job definition is promoted. Each target would point at its own workspace or catalog, with grants managed through Unity Catalog. (The `dev` target validates today; additional targets are not in the scaffold.)

**Q:** How would CI/CD deploy this bundle? [PROJECT]

`.github/workflows/tests.yml` runs the offline tests today, and TODO-15 adds a pytest job with Java 17 and Python 3.12 for the PySpark tests. A production pipeline would add stages: lint, unit tests, `databricks bundle validate`, `bundle deploy` to dev on merge to main, an integration run, then a gated `deploy` to prod using the service principal. Each stage should fail fast, and no stage should deploy from a developer laptop. The scaffold stops at validate, by design. (After TODO-15.)

**Q:** How would you monitor and alert on this pipeline? [PROJECT]

The raw signals already exist: `pipeline_runs` (per-task row counts and status) and `dq_results` (per-check pass or fail), both written by the project. In production I would alert on job failure, on a critical DQ check failing, on row counts falling outside an expected band, and on a missing daily batch (freshness). I would route alerts to the on-call channel and use system tables for job-run history where available. (After TODO-09 and TODO-13.)

**Q:** How would you think about partitioning and clustering at scale? [PROJECT]

For a new table I would use liquid clustering (`CLUSTER BY`) on the columns most often filtered, such as `region` and `event_date` in `silver_shipments`, rather than hand-picked partition columns, because partitioning on a high-cardinality column creates many small files. `50_delta_ops` (TODO-12) runs `ALTER TABLE ... CLUSTER BY`, `OPTIMIZE`, and compares `DESCRIBE DETAIL` before and after. At this data volume the clustering effect is small, so I would not claim a speed-up from it. (After TODO-12.)

**Q:** What cost controls would you put in place? [PROJECT]

Serverless is billed per usage, so the controls are about not running work that is not needed: incremental ingest (TODO-07) avoids re-reading old files, and full rebuilds are limited to small gold tables. I would add budgets and tags per job, query usage from `system.billing.usage` (TODO-18, availability on Free Edition not verified), and set job timeouts so a stuck run is stopped. The honest answer is that I have not yet measured cost for this project. (After TODO-18 for the usage query.)

---

## 2. Behavioral answers (STAR templates)

Each answer is a template built from this project. Replace the bracketed parts with your own true experience; keep the structure (situation, task, action, result) and the specifics.

**Q:** Tell me about a time you took ownership of something beyond your assigned scope. [PROJECT]

Template. **Situation:** [context, e.g. a university or previous-job project] had a data pipeline whose quality checks were not run by anyone. **Task:** I took ownership of the quality layer, which was not in my original scope. **Action:** I built the quarantine split and the reconciliation check so rejected rows were visible and counted, and I wrote the tests that proved the rules, as in `quality.py` and `test_missing_supplier_rule`. **Result:** [a real, verifiable result such as "every run now reports rejected rows by reason"], and I documented the rule set so the next person could extend it.

**Q:** Describe a hard bug you debugged. [PROJECT]

Template, adapt to what happened. **Situation:** A day-2 run of the silver MERGE failed with `multiple source rows matched`, even though day 1 loaded cleanly because the table was created from the batch without a MERGE. **Task:** Find the cause without changing the business rule. **Action:** I looked at the failing batch and found two events for one shipment that already existed in silver (the scenario is a batch with two updates for one existing shipment, which happens if dedup is skipped), then confirmed it by checking `count(distinct shipment_id) = count(*)` on the batch and by running `dedup_latest` on it. I then added a unit test (TODO-05) so the failure would be caught before a notebook run. **Result:** [how long it took, and what check now guards it]. Alternative scenario: day-3 schema mismatch on the bronze append, fixed with `mergeSchema` in TODO-10; use whichever you can explain.

**Q:** Tell me about a time you learned a new technology quickly. [PROJECT]

Template. **Situation:** I had to build a Spark and Delta pipeline with no prior Databricks experience. **Task:** Be productive on Databricks, Unity Catalog and Delta within a short time. **Action:** I started with the original notebook (`01_bronze_silver_gold.py`) and ran it end to end, then used `.explain()` and `DESCRIBE HISTORY` to see what Spark and Delta were doing, before writing any TODO. I wrote the concept down in `docs/CONCEPTS.md` as I went, and I checked each step against a small, testable goal. **Result:** [what you could do at the end, stated concretely, such as "I can explain a MERGE plan and recover a table with RESTORE"], only once you have done TODO-11.

**Q:** Tell me about working with operations or other developers. [GENERAL]

Template. **Situation:** [a time you worked with an ops team or a developer team, e.g. on a release or an incident]. **Task:** [what you needed from them, or what they needed from you]. **Action:** [what you did to make the hand-off clear: a runbook, an agreed data contract, a PR description, a shared dashboard]. **Result:** [a concrete outcome you can verify]. In the project, the closest analogue is the `docs/` folder and `pipeline_runs`, which give operations a way to see what ran without reading code.

**Q:** Tell me about a mistake you made and what you learned. [PROJECT]

Template. **Situation:** [a real mistake, e.g. a merged change that broke a job or a query that double-counted]. **Task:** Fix it and prevent a repeat. **Action:** [what you did first, who you told, how you verified the fix]. **Result:** [the prevention you added, such as a test, a check or a review step]. Good project candidates: deleting a Delta table instead of running `DELETE`, or running `VACUUM` with too short a retention. Be ready to say what you would now check before running the command.

**Q:** How do you use AI coding assistants responsibly? [PROJECT]

Template and honest answer. Claude Code scaffolded about 20% of this repo as reference code (the generator, the config and the reference rules), which is clearly marked as `[REFERENCE]` in the spec. I wrote the remaining TODO-01 to TODO-18 implementations myself, and I can explain each line I submit. I verified them by running the tests, checking row counts against the spec numbers (1,260, 1,300, 1,350), and reading the code rather than trusting a green run alone. I credit the assistant in the repo and in commit messages, and I do not submit code I cannot explain.

---

## 3. Technical questions by topic

Labels: **Junior**, **Mid**, **Stretch**. Tags: `[GENERAL]` or `[PROJECT]`.

### 3.1 Apache Spark / PySpark (19)

**Q:** What are the driver and the executors, and what runs where? [GENERAL] · Junior

The driver runs your Python program, builds the logical and physical plan, and coordinates the job; executors are JVM processes on worker nodes that run the tasks over partitions of data. Data stays on executors, and only small results (for example `collect()` or `toPandas()`) come back to the driver, so collecting a large DataFrame is a common way to crash a job. In Spark Connect the Python client talks to a remote server over gRPC, which changes where the client code runs but not the executor model. On serverless you do not manage either explicitly.

**Q:** What does lazy evaluation mean in Spark? [GENERAL] · Junior

Transformations such as `filter`, `select` and `join` only build a plan; nothing reads data until an action runs. Lazy evaluation lets Catalyst optimise the whole plan, for example by pushing filters down to the reader and pruning columns. The practical consequence is that a slow step may appear at the action, not where you wrote the transformation, and that re-running an action recomputes the plan unless you cache. This is why the reference `.explain()` cell in TODO-01 matters.

**Q:** What is the difference between transformations and actions? [GENERAL] · Junior

Transformations (`select`, `withColumn`, `filter`, `groupBy`, `join`) return a new DataFrame and are lazy. Actions (`count`, `collect`, `show`, `write.saveAsTable`) trigger execution and return a result or write output. In the project, `build_region_part` is only a transformation until `30_gold` writes it, which is the action that makes the work happen.

**Q:** What is the difference between narrow and wide transformations? [GENERAL] · Junior

A narrow transformation (`select`, `filter`, `withColumn`, `map`) computes each output partition from one input partition, so no data moves between executors. A wide transformation (`groupBy`, `distinct`, `orderBy`, non-broadcast `join`, a window with `partitionBy`) needs rows with the same key to meet, so Spark performs a shuffle. The dedup in TODO-04 is wide because the window partitions by `shipment_id`.

**Q:** What is a shuffle, and why is it expensive? [GENERAL] · Mid

A shuffle redistributes rows across executors so that rows sharing a key land in the same partition. It writes intermediate files to local disk, transfers data over the network, and reads it back, so it costs I/O, serialization and memory. Shuffles appear as `Exchange` nodes in the plan. You reduce them by filtering early, selecting fewer columns, broadcasting small tables and avoiding unnecessary `distinct` or `orderBy`.

**Q:** What are partitions, and what does `spark.sql.shuffle.partitions` control? [GENERAL] · Junior

A partition is the unit of parallel work: one task processes one partition. `spark.sql.shuffle.partitions` sets how many partitions a shuffle produces, and its default in classic Spark is 200, which is too many for tiny data and too few for huge data. Adaptive Query Execution can coalesce small shuffle partitions at runtime. On serverless you cannot tune arbitrary Spark configs, so you rely on AQE and sensible data layout.

**Q:** What is the difference between `repartition` and `coalesce`? [GENERAL] · Mid

`repartition(n)` performs a full shuffle and can increase or decrease the partition count, and `repartition(col)` hashes rows by a column. `coalesce(n)` only decreases the number of partitions and avoids a full shuffle by merging existing partitions, so partitions can end up uneven. Use `coalesce` to reduce output file count before a write, and `repartition` when you need a balanced or key-based distribution.

**Q:** How do joins work in Spark, and what does a broadcast join do? [GENERAL] · Mid

Spark chooses a join strategy: a broadcast hash join sends a small table to every executor so the large side does not shuffle, while a sort-merge join shuffles and sorts both sides by the key. The default auto-broadcast threshold is around 10 MB, based on size estimates, and you can force it with `F.broadcast(df)` or a `BROADCAST` hint. TODO-17 asks you to compare the two plans for `silver` joined with `dim_supplier`.

**Q:** What is data skew, and how do you handle it (salting, AQE skew join)? [GENERAL] · Stretch

Skew means one key holds far more rows than the others, so one task does most of the work and becomes the straggler. Salting adds a random suffix to the hot key on the large side and replicates the small side across the same suffixes, spreading the hot key over several tasks. Adaptive Query Execution's skew join handling, enabled by default in recent Spark versions, splits oversized partitions at runtime, which often removes the need for manual salting. Whether skew matters depends on the data, which this synthetic dataset does not really produce.

**Q:** What is Adaptive Query Execution (AQE), and what does it do? [GENERAL] · Mid

AQE re-optimises a query at runtime using statistics collected from completed shuffle stages. It can coalesce small shuffle partitions, switch a sort-merge join to a broadcast join when one side turns out to be small, and split skewed partitions. It has been enabled by default since Spark 3.2. Its effects appear in the query plan as an `AdaptiveSparkPlan` with runtime statistics, so read the final plan, not only the initial one.

**Q:** When would you use `cache` or `persist`, and what are the pitfalls? [GENERAL] · Mid

Cache or persist a DataFrame that you reuse several times, after an expensive step, so later actions read it from memory or disk instead of recomputing the lineage. `df.cache()` uses the default storage level (memory and disk for DataFrames), and it is lazy, so the first action fills the cache. Pitfalls are caching too much, forgetting to `unpersist`, and caching before a filter that would have reduced the data. I have not verified cache behaviour on Free Edition serverless, so check that before you rely on it.

**Q:** What is the difference between a Python UDF, a built-in function, and a pandas UDF? [GENERAL] · Mid

A built-in function (`F.when`, `F.concat_ws`, or a SQL expression such as `F.expr("try_cast(x AS INT)")`) is optimised by Catalyst and runs inside Spark's engine, so it is the first choice. A Python UDF calls Python row by row and serializes data between the JVM and the Python worker, which is slow. A pandas UDF uses Apache Arrow to move batches as pandas objects, so it is much faster than a row UDF while still running Python code. Prefer built-ins, and use a pandas UDF only when the logic cannot be expressed with built-ins.

**Q:** How do you read a query plan (`explain`) and find `Exchange`, `BroadcastHashJoin` and `SortMergeJoin`? [PROJECT] · Mid

`df.explain("formatted")` prints the physical plan, which you read bottom-up: scans at the bottom, operators above. `Exchange` marks a shuffle, `BroadcastExchange` followed by `BroadcastHashJoin` marks a broadcast join, and `SortMergeJoin` with `Exchange` and `Sort` children on both sides marks a shuffled join. TODO-13 asks you to explain the `Exchange` in the gold query, and TODO-17 asks you to compare the join plans with and without `F.broadcast`.

**Q:** How do you read the Spark UI, and what is different on serverless? [GENERAL] · Mid

The Spark UI shows jobs, stages with task durations and shuffle sizes, the SQL tab with per-operator metrics, executors and storage. Look for stages where one task runs much longer than the rest (skew) and for large shuffle read or write sizes. On serverless the UI and its availability differ from a classic cluster, so I would use the query profile in the notebook or SQL editor first and treat Spark UI access as something to verify on Free Edition.

**Q:** What is Spark Connect, and how is serverless different from classic PySpark? [GENERAL] · Stretch

Spark Connect separates the client from the server: your Python program sends a logical plan over gRPC to a Spark server, which means the client does not hold a SparkContext or run the driver logic in the same process. Serverless compute on Databricks uses Spark Connect, so `spark.sparkContext`, RDD APIs and many JVM-internal APIs are unavailable, and you cannot set arbitrary Spark configs. Classic local PySpark, which the tests use, is a full driver in one process, so code that works locally can still fail on serverless if it touches the RDD or config APIs. This is why `src/` uses only `pyspark.sql` and `spark.sql`.

**Q:** When would you use the DataFrame API versus Spark SQL, and why avoid RDDs here? [GENERAL] · Junior

The DataFrame API and Spark SQL compile to the same logical plan, so the choice is about readability and team habit: SQL is clear for CTEs and joins, while DataFrame code is easier to unit-test as functions. RDDs bypass Catalyst and the schema, so they are slower and harder to optimise, and serverless does not expose them. The project keeps transforms as DataFrame functions and uses `spark.sql(...)` for MERGE and scorecard SQL.

**Q:** Why does the project read bronze as strings and use `try_cast` under ANSI mode? [PROJECT] · Mid

`bronze.read_batch` reads with no schema inference, so every column is a string and a bad value is preserved. Serverless runs with ANSI mode on, where an invalid `CAST` raises an error, and `try_cast` returns NULL instead, which lets the rule framework mark the row. `quality.cast_types` uses `F.expr("try_cast(x AS INT)")` and similar for dates, timestamps and decimals. Unit tests in `test_cast_types_invalid_date_becomes_null` check exactly this behaviour.

**Q:** How does silver deduplicate to the latest event, and why does the tie-break matter? [PROJECT] · Mid

`dedup_latest` (TODO-04) partitions by `shipment_id`, orders by `event_ts` descending and `event_id` descending, and keeps `row_number() = 1`. The `event_id` tie-break makes the result deterministic when two events share a timestamp, which matters for reruns and for tests. Without it, which row survives can change between runs, and a MERGE downstream would behave differently. (After TODO-04.)

**Q:** How would you extract a file name in Spark, and why does bronze need it? [PROJECT] · Mid

`read_batch` adds `_source_file` from the `_metadata.file_path` column, which is Delta- and Spark-provided file metadata and is available on Databricks. Storing the source path on every row gives lineage from each bronze row back to the file that delivered it, and it is the key that `list_new_files` and `unprocessed_bronze` use for incremental processing. Older tutorials use `input_file_name()`, which is deprecated in favour of the metadata column.

### 3.2 Delta Lake (14)

**Q:** What does ACID mean for Delta tables? [GENERAL] · Junior

Atomicity means a write either fully commits or does not appear at all, consistency means each committed version satisfies the table's schema and constraints, isolation means concurrent readers see a consistent snapshot, and durability means committed data survives failures. Delta gets these guarantees from its transaction log over files in object storage. On Azure the files sit in ADLS Gen2, and on AWS or GCP the same log works over S3 or GCS.

**Q:** How does the transaction log work, and what are checkpoints? [GENERAL] · Mid

The `_delta_log/` folder holds an ordered series of JSON commit files, each listing the files added and removed by one commit. To read the table, a reader replays the log from the latest checkpoint, a Parquet snapshot that Delta writes periodically (by default every 10 commits), plus the JSON commits after it. Checkpoints keep reads fast when the log is long. `DESCRIBE HISTORY` shows the commits as versions.

**Q:** What is time travel, and what limits it? [GENERAL] · Junior

Time travel reads an older version of a table with `VERSION AS OF n` or `TIMESTAMP AS OF`. Old data files are kept until `VACUUM` removes them, so the limit is retention: by default the log is kept for about 30 days and deleted data files for 7 days, so older versions can become unreadable. TODO-11 uses time travel to diff two versions and then restore.

**Q:** How does MERGE work, and what happens with duplicate source keys? [GENERAL] · Mid

MERGE joins a source to a target on a condition and applies `WHEN MATCHED` (update or delete) and `WHEN NOT MATCHED` (insert) clauses. If more than one source row matches the same target row, Delta raises an error ("multiple source rows matched"), because the result would be ambiguous. The fix is to deduplicate the source to one row per key first, which is why `dedup_latest` precedes `merge_into_silver`. Unconditional delete clauses are the one exception Delta tolerates, and you should not rely on it.

**Q:** Why add an `event_ts` guard to a MERGE update? [PROJECT] · Mid

`merge_into_silver` (TODO-08) has `WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *`, so an older or replayed event does not overwrite the current state. Without the guard, a late-arriving older update would silently regress a shipment's status. The guard also makes the MERGE idempotent: rerunning the same batch matches with equal timestamps and changes nothing. (After TODO-08.)

**Q:** What is the difference between schema enforcement and schema evolution? [GENERAL] · Junior

Schema enforcement is the default: a write whose columns do not match the table's schema fails, which protects the table from accidental drift. Schema evolution is an explicit opt-in, such as `mergeSchema` on a write or `WITH SCHEMA EVOLUTION` on a MERGE, that adds the new columns to the table. The new column is NULL for older rows. TODO-10 is the project exercise: day 3 adds `carrier`, and the append fails until you opt in. (After TODO-10.)

**Q:** What does `OPTIMIZE` do? [GENERAL] · Mid

`OPTIMIZE` compacts many small files into fewer larger ones, which reduces the per-file overhead of listing and reading. It does not change the data or the schema, and it creates a new version in the log. With clustering enabled, `OPTIMIZE` also rewrites data so that it is laid out by the clustering columns. Run it after many small writes, not after every write.

**Q:** Z-ORDER versus liquid clustering: which would you use and why? [GENERAL] · Mid

`ZORDER BY` co-locates related values within files to improve data skipping, but it is a manual option of `OPTIMIZE` that rewrites the files it touches on each run and it does not fit well with partitioning. Liquid clustering (`CLUSTER BY`) is the recommended approach for new tables: you choose the clustering keys, it clusters incrementally during `OPTIMIZE`, and it replaces both partitioning and `ZORDER` for those tables. I would use liquid clustering for `silver_shipments` on `region` and `event_date`, and TODO-12 asks you to verify `clusteringColumns` in `DESCRIBE DETAIL`.

**Q:** What does `VACUUM` do, and why is `RETAIN 0 HOURS` dangerous? [GENERAL] · Mid

`VACUUM` deletes data files that are no longer referenced by the log and are older than the retention threshold, which is 7 days by default. Shortening retention removes the files time travel needs, and `RETAIN 0 HOURS` deletes almost everything, including files a concurrent reader may still be using. `50_delta_ops` warns about this, and the default retention should only be changed deliberately and with a reason.

**Q:** What is Change Data Feed (CDF), and when would you use it? [GENERAL] · Mid

CDF records row-level changes between table versions, with the columns `_change_type` (insert, update_preimage, update_postimage, delete), `_commit_version` and `_commit_timestamp`. You enable it with the table property `delta.enableChangeDataFeed` and read it with `table_changes` or the DataFrame reader option. It is useful for feeding downstream incremental consumers without rescanning the table. The project does not enable CDF; it is a concept to know.

**Q:** What is SCD Type 1 versus Type 2? [PROJECT] · Mid

SCD Type 1 overwrites the attribute in place, so history is lost; `dims.upsert_dim_supplier` is the Type 1 reference (a MERGE that updates changed rows). SCD Type 2 keeps history by expiring the current row and inserting a new one with `valid_from`, `valid_to` and `is_current`. Type 2 is what you need when a report must show a supplier's rating as it was at the time of a shipment. TODO-16 implements it. (After TODO-16 for the Type 2 behaviour.)

**Q:** What are deletion vectors, and why do they matter? [GENERAL] · Stretch

Deletion vectors let `DELETE`, `UPDATE` and `MERGE` mark rows as deleted in a bitmap file, rather than rewriting every Parquet file that contains them. That reduces write amplification for small changes. The rows are physically removed later when the table is reorganised or optimised (`REORG TABLE ... APPLY (PURGE)`). Readers combine the data files with the bitmap, so the cost moves from writes to reads, which is a trade-off to measure rather than assume.

**Q:** How does Delta handle concurrent writes, and what conflicts occur? [GENERAL] · Stretch

Delta uses optimistic concurrency: each writer reads a snapshot, does its work, then tries to commit the next log version, and the commit fails if the work conflicts with a concurrent commit. Typical errors are `ConcurrentAppendException` (a concurrent write added files the transaction read), `ConcurrentDeleteReadException` (a file it read was deleted) and `MetadataChangedException`. The default isolation level is `WriteSerializable`. Partitioning or clustering that separates writers' data reduces conflicts, and retrying the job is the usual remedy.

**Q:** How would you recover from a bad `DELETE` or a bad write? [PROJECT] · Mid

Start with `DESCRIBE HISTORY` to find the version before the bad operation, then either read it with `VERSION AS OF` to compare, or run `RESTORE TABLE silver_shipments TO VERSION AS OF n`. `RESTORE` creates a new commit, so the bad write stays visible in history. TODO-11 asks you to delete the Midwest rows on purpose, compare counts, then restore to the pre-delete count, and check that the history shows both the DELETE and the RESTORE. (After TODO-11.)

### 3.3 Databricks platform (15)

**Q:** What is the Databricks workspace, and what lives in it? [GENERAL] · Junior

A workspace is the environment where users and jobs run: notebooks, Git folders, jobs and pipelines, SQL warehouses, and the catalog that Unity Catalog governs. It sits in a cloud account; on Azure it is an Azure Databricks resource in a resource group. The project runs entirely in a Free Edition workspace, so there is one workspace and no environment separation.

**Q:** What is the difference between all-purpose clusters, job clusters and serverless compute? [GENERAL] · Junior

All-purpose clusters are shared, interactive and billed while running, so they are the most expensive way to run scheduled work. Job clusters are created for one job run and terminated afterwards, which is cheaper and isolated. Serverless compute is fully managed by Databricks, starts quickly, has no cluster configuration, and is billed on usage. The project uses serverless only because Free Edition offers it.

**Q:** How do Databricks jobs model tasks, dependencies, retries and parameters? [PROJECT] · Mid

A job is a DAG of tasks; each task has a notebook or script, `depends_on` edges, retry settings (`max_retries`), and parameters. `resources/shipments_job.yml` defines `generate` then `bronze` with `depends_on`, and passes `catalog`, `schema` and `run_id` (`{{job.run_id}}`) as parameters, which notebooks read through widgets in `config.from_widgets`. TODO-14 adds silver, gold and quality tasks with the same pattern. (After TODO-14; retries are not configured in the scaffold.)

**Q:** What is the Unity Catalog three-level namespace? [GENERAL] · Junior

Unity Catalog names objects as `catalog.schema.object`, for example `workspace.shipments.silver_shipments`. Catalogs group schemas, schemas group tables, views, volumes and functions, and the metastore sits above the catalogs. The project's `config.table(name)` builds the three-part name, so changing the catalog is a configuration change.

**Q:** What is a Unity Catalog volume, and when do you use it instead of a table? [PROJECT] · Mid

A volume is a governed location for files, addressed as `/Volumes/<catalog>/<schema>/<volume>`, and it is used for CSV, JSON, images and other non-tabular data. The project lands the daily CSV batches in `workspace.shipments.raw` (`config.volume_path`) and reads them as files. Tables are for structured, queryable Delta data. Volumes are managed or external, like tables, and the access rules follow Unity Catalog grants.

**Q:** How do grants and lineage work in Unity Catalog? [GENERAL] · Mid

Grants (`GRANT SELECT ON TABLE ... TO group`) control who can read, write or manage each securable, and they should go to groups rather than individuals. Lineage is captured automatically for queries and pipelines, so you can see which tables feed a given table, which helps with impact analysis. Audit logs record who accessed what. I would manage grants in code in production, not by hand.

**Q:** What are Lakeflow Declarative Pipelines (formerly Delta Live Tables) and expectations? [GENERAL] · Stretch

Lakeflow Declarative Pipelines let you declare tables and their dependencies, and Databricks manages execution, retries and orchestration of the update. Expectations are data-quality rules attached to a table: `EXPECT` records violations and keeps the rows, `ON VIOLATION DROP ROW` drops them, and `ON VIOLATION FAIL UPDATE` stops the update. The project does not use Lakeflow Declarative Pipelines; the `quality.py` rule framework is hand-written and comparable, and that comparison is the point of the exercise.

**Q:** What is Auto Loader, and how is it different from reading a folder with `spark.read`? [GENERAL] · Mid

Auto Loader (the `cloudFiles` source) incrementally discovers new files in a cloud location, tracks which ones it has processed in a checkpoint, and can infer and evolve the schema through a schema location. A plain `spark.read` on a folder re-lists everything each time and has no memory of what it processed, which is what `list_new_files` simulates by hand. The project does not use Auto Loader, so I would describe it rather than demonstrate it.

**Q:** What are Databricks Asset Bundles, and what do the commands do? [GENERAL] · Mid

Asset Bundles describe jobs, pipelines and other resources as YAML in a project, deployed with the CLI. `databricks bundle validate` checks the configuration without changing the workspace, `bundle deploy` creates or updates resources in a target, and `bundle run` starts a job. The `dev` target in development mode prefixes resource names and pauses schedules. The project only validates; deploy is deliberately not run.

**Q:** What are Git folders, and how do they fit with the project? [GENERAL] · Junior

Git folders connect a Databricks workspace folder to a Git repository so notebooks and files are versioned and branches can be switched from the UI. They let you develop in the workspace while keeping the repo as the source of truth. The notebooks in this project use the `# Databricks notebook source` format so diffs are readable.

**Q:** How do secret scopes work, and what is a Key Vault-backed scope? [GENERAL] · Mid

A secret scope is a named container of secrets, read in code with `dbutils.secrets.get(scope, key)`, so the value never appears in the notebook. A Databricks-backed scope stores secrets in Databricks; an Azure Key Vault-backed scope reads them from an Azure Key Vault, which keeps rotation and access policy in Azure. Access is controlled per scope. I would use Key Vault-backed scopes for anything that needs central governance. The project has no secrets.

**Q:** What are the main levers for Databricks cost optimisation? [GENERAL] · Mid

Use serverless or job compute instead of always-on all-purpose clusters, avoid unnecessary full recomputation through incremental loads, compact small files, and set timeouts and right-sized compute. Use tags and budgets to attribute spend to jobs or teams, and query `system.billing.usage` to see DBUs by SKU. Measure first: I have not verified billing query availability on Free Edition. (TODO-18 asks you to record what happens.)

**Q:** How do you monitor Databricks jobs and data quality? [GENERAL] · Mid

Use job-run notifications for failures, the job run history for durations and retries, and system tables for usage and lineage where your workspace exposes them. For data quality, persist check results per run (the project's `dq_results`) so you can trend them. Alerting should fire on failed runs and critical checks, not on every warning. (After TODO-09 for the critical-check behaviour.)

**Q:** How do you pass parameters into notebooks in a job, and how does the project do it? [PROJECT] · Junior

Job parameters are set at the job level and base parameters are set per task; in a notebook, `dbutils.widgets.get` reads them, and a widget default is used when running interactively. The project's `config.from_widgets` and `get_widget` create text widgets with defaults for `catalog`, `schema`, `volume` and `run_id`, so the same notebook runs interactively and from the job. This is a concept you need to explain line by line.

**Q:** What are the limits of Free Edition that matter for this project? [GENERAL] · Stretch

Free Edition is serverless-only, has compute and storage quotas, and does not expose every feature of a paid workspace, such as some system tables, all Spark configs and some networking options. The project's Free Edition constraints are the reason for the no-`sparkContext`, no-`dbfs:/`, no-cluster-config rules. I would name the limit, say how I worked around it, and check the official limitations page rather than rely on memory. The exact current limits are not verified in this repo.

### 3.4 SQL (10)

Snippets used in this section (dialect: Spark SQL / Databricks SQL):

```sql
-- 1. Dedup to the latest event per shipment (pattern of TODO-04)
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY shipment_id
                            ORDER BY event_ts DESC, event_id DESC) AS rn
  FROM bronze_cast
) t
WHERE rn = 1;

-- 2. Supplier scorecard shape with a CTE and RANK (pattern of TODO-06)
WITH shipment_stats AS (
  SELECT supplier_id,
         COUNT(*) AS shipment_count,
         AVG(CASE WHEN status = 'DELAYED' THEN 1.0 ELSE 0.0 END) AS delay_ratio,
         AVG(unit_cost_usd) AS avg_unit_cost_usd
  FROM silver_shipments
  GROUP BY supplier_id
)
SELECT d.supplier_id, d.supplier_name, s.shipment_count,
       ROUND(s.delay_ratio * 100, 2) AS delayed_pct,
       s.avg_unit_cost_usd,
       RANK() OVER (ORDER BY s.delay_ratio DESC) AS delay_rank
FROM shipment_stats s
JOIN dim_supplier d ON d.supplier_id = s.supplier_id
WHERE d.is_current;   -- only valid once dim_supplier has is_current (TODO-16)

-- 3. The same dedup using QUALIFY (supported on Databricks SQL; check your engine)
SELECT *
FROM silver_candidates
QUALIFY ROW_NUMBER() OVER (PARTITION BY shipment_id
                           ORDER BY event_ts DESC, event_id DESC) = 1;
```

**Q:** What is the difference between inner, left and anti joins? [GENERAL] · Junior

An inner join keeps only matching rows from both sides; a left join keeps every left row and fills unmatched right columns with NULL; a left anti join keeps left rows with no match on the right and returns only left columns. The project uses a left anti join in `unprocessed_bronze` to find bronze rows whose `_source_file` is not yet in silver or quarantine. Anti joins are also the clean way to find orphans, such as a shipment with no supplier.

**Q:** How do `GROUP BY`, `HAVING` and `WHERE` differ? [GENERAL] · Junior

`WHERE` filters rows before aggregation, `GROUP BY` forms the groups, and `HAVING` filters groups after aggregation. Put row-level conditions in `WHERE` because they reduce the data earlier, and aggregate conditions in `HAVING`. For example, to find regions with more than 100 delayed shipments you need `HAVING`, not `WHERE`.

**Q:** What are CTEs, and when do they help? [GENERAL] · Junior

A common table expression (`WITH name AS (...)`) names an intermediate query so the main query reads top-down. It makes multi-step logic reviewable, as in snippet 2 above. CTEs are not a performance feature by themselves; the engine may inline them, so use them for clarity and check the plan if performance matters.

**Q:** What are window functions, and how do `PARTITION BY` and `ORDER BY` shape them? [GENERAL] · Mid

A window function computes a value over a set of rows related to the current row without collapsing them, unlike `GROUP BY`. `PARTITION BY` defines the window per key, `ORDER BY` defines the order within it, and the frame defines the rows included. Common uses are running totals, previous-row comparisons with `LAG`, and ranking. In Spark, a window with `PARTITION BY` is a wide transformation that causes a shuffle.

**Q:** How do you deduplicate with `ROW_NUMBER`, and how does that differ from `DISTINCT`? [PROJECT] · Mid

`DISTINCT` removes exact duplicate rows across all columns, but silver needs one row per `shipment_id` chosen by a rule (the latest event), so you use `ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY event_ts DESC, event_id DESC) = 1` as in snippet 1. The `event_id` tie-break makes the chosen row deterministic. This is the logic of `dedup_latest` (TODO-04) and the unit test in TODO-05. (After TODO-04.)

**Q:** What is the difference between `RANK`, `DENSE_RANK` and `ROW_NUMBER`? [PROJECT] · Mid

`ROW_NUMBER` gives every row a unique sequential number, even when values tie. `RANK` gives tied rows the same rank and leaves gaps after them (1, 1, 3), while `DENSE_RANK` gives tied rows the same rank without gaps (1, 1, 2). The scorecard uses `RANK() OVER (ORDER BY delayed_pct DESC)` as `delay_rank`, so suppliers with equal delay percentages share a rank. (After TODO-06.)

**Q:** What does `QUALIFY` do, and why use it? [GENERAL] · Mid

`QUALIFY` filters on window function results directly, so you can write the dedup in one statement without a subquery, as in snippet 3. Its evaluation happens after window functions are computed, similar to how `HAVING` works after aggregation. It is available in Databricks SQL and Databricks Runtime; check your engine's support before using it in another environment.

**Q:** What are the main query optimisation habits in SQL and Spark? [GENERAL] · Mid

Filter early and on the raw columns (so predicates can be pushed down and file skipping works), select only the columns you need instead of `SELECT *`, and avoid functions on filter columns that block pruning. Make joins on the same types, use a broadcast hint for a small dimension, and check the plan with `EXPLAIN`. Clustering or partitioning on the filter columns helps skipping for large tables.

**Q:** How do NULLs affect comparisons, `NOT IN` and aggregates? [GENERAL] · Stretch

`NULL = NULL` is NULL, not true, so a filter `WHERE x = y` drops rows where either side is NULL, and `x NOT IN (list)` returns no rows when the list contains a NULL. `COUNT(column)` ignores NULLs while `COUNT(*)` does not. The project's rules rely on this: `rule_missing_supplier` checks for NULL or blank explicitly, and a status rule must handle NULL on purpose, because `status NOT IN (...)` evaluates to NULL for a NULL status and the row is silently skipped rather than flagged. (After TODO-02.)

**Q:** How do you count distinct values, and what does it cost? [GENERAL] · Junior

`COUNT(DISTINCT col)` returns the number of distinct non-NULL values, and it needs a shuffle to compare values across partitions, so it is more expensive than `COUNT(*)`. The reconciliation check in `checks.py` uses distinct `shipment_id` counts, and `check_unique_key` (TODO-09) compares `count(distinct key)` with `count(*)` to detect duplicates. For approximate counts at scale, `approx_count_distinct` trades exactness for speed.

### 3.5 Data engineering fundamentals (10)

**Q:** What is the difference between ETL and ELT? [GENERAL] · Junior

ETL transforms data before loading it into the target, which was necessary when warehouse compute was expensive; ELT loads raw data first and transforms it inside the target platform. The medallion design in this project is ELT: bronze is the raw load and silver and gold are transformations on the platform. ELT keeps raw data available for replay, which is why bronze keeps strings.

**Q:** What is the difference between batch and streaming? [GENERAL] · Junior

Batch processes a bounded set of data on a schedule, and streaming processes unbounded data continuously or in micro-batches. This project is batch: daily CSV batches with an explicit `days` parameter. Streaming would need checkpoints, watermarks for late data and a different write model. I can explain these differences, but the repo does not stream.

**Q:** What does idempotency mean, and why does it matter for pipelines? [PROJECT] · Junior

An operation is idempotent if running it twice produces the same result as running it once. That matters because retries and reruns happen, and a non-idempotent pipeline duplicates rows or double-counts. The project gets idempotency from `list_new_files` (TODO-07), the `event_ts` guard in MERGE (TODO-08), and overwriting the small gold tables. (After TODO-07 and TODO-08.)

**Q:** What are the main data quality dimensions, and how does the project address them? [PROJECT] · Junior

Common dimensions are completeness (no required values missing), validity (values in allowed ranges and formats), uniqueness (one row per key), consistency (values agree across tables) and timeliness (data arrives when expected). The project addresses validity with the rules in `quality.py`, uniqueness with `dedup_latest` and `check_unique_key`, consistency with `check_no_shipment_lost`, and timeliness with the freshness alert I would add in production. (After TODO-02, TODO-04 and TODO-09.)

**Q:** What is a star schema, and where would you use one? [GENERAL] · Junior

A star schema has a central fact table of measurable events (for example shipments with quantity and cost) surrounded by dimension tables describing the who, what, where and when (supplier, part type, region, date). It is easy for BI tools to query and easy to explain. The project's `dim_supplier` is the dimension and `gold_region_part` is an aggregate built from the fact-like silver table, which is a simplified version of the pattern.

**Q:** What are incremental loads, and how do they differ from full refreshes? [PROJECT] · Junior

A full refresh reprocesses all the data every run, which is simple and correct but grows expensive. An incremental load processes only new or changed data, using a watermark, a file tracker or a change feed. The project does both: silver can refresh fully when `USE_MERGE` is False, and incremental when it is True, and bronze uses file-level tracking. (After TODO-07 and TODO-08.)

**Q:** What is late-arriving data, and how do you handle it? [PROJECT] · Mid

Late-arriving data is an event whose timestamp is earlier than data already loaded, often because the source delivered it late. You handle it by keying the target on the business key and comparing event time, not arrival time, which is what the `event_ts` guard does: a late, older event does not overwrite a newer status. The generator does not produce late events today: every day-2 and day-3 update is newer than what is already loaded, so the guard is designed for late data but the project does not yet prove it. I would add a late event (an older `event_ts` for an already-loaded shipment) to the generator and a unit test on `merge_into_silver` to show it is ignored; that is an improvement, not existing coverage. (After TODO-08.)

**Q:** What is orchestration, and what does the project's job do? [PROJECT] · Junior

Orchestration schedules tasks, orders them by dependency and handles retries and alerts. The job `shipments-pipeline` orchestrates `generate` then `bronze` and later silver, gold and quality tasks through `depends_on`. An orchestrator should not contain business logic; the notebooks call tested functions. (After TODO-14.)

**Q:** What is a backfill, and how would you run one safely? [GENERAL] · Mid

A backfill reprocesses historical data, usually after a bug fix or a late source change. Run it idempotently, with a parameter for the date range (the project's `days` parameter is a small example), on a copy or with a time-travel snapshot to compare, and check the reconciliation and DQ results before you publish. Keep the backfill separate from the daily run so it does not compete for the same resources.

**Q:** What is a data contract, and why does it matter? [GENERAL] · Mid

A data contract is an agreement between a producer and consumers on schema, types, semantics, quality expectations and delivery timing. It turns surprises like a new column or a changed status value into a visible change request. The project's generator and its column list (`COLUMNS`, `COLUMNS_DAY3`) act as a small contract, and the day-3 `carrier` column is the change a contract would have to approve.

### 3.6 DevOps and infrastructure as code (8)

**Q:** How would you branch and version a data project in Git? [GENERAL] · Junior

Use short-lived feature branches from the main branch, keep commits small and descriptive, and open a pull request for every change. The project uses a `learning-scaffold` branch with local commits and no push. For a team I would use trunk-based or GitHub-flow branching with protected main and required checks.

**Q:** What makes a good pull request in a data team? [GENERAL] · Junior

A good PR has a clear purpose, a small diff, tests for changed logic, notes on schema or data impact (for example new columns or changed row counts), and a note on how it was verified. Reviewers should be able to see the expected numbers. For this project I would list the expected counts from `SCAFFOLD_SPEC` in the PR description.

**Q:** What are the CI/CD stages for a Databricks bundle project? [GENERAL] · Mid

Typical stages are lint (ruff or flake8), unit tests (pytest), validate (`databricks bundle validate`), deploy to dev, an integration run in dev, then a gated deploy to prod. Each stage should fail the pipeline on error and no stage should depend on a laptop. Today the project runs offline tests in GitHub Actions, TODO-15 adds the PySpark test job, and validate is run locally; there is no lint stage and no deploy stage in the scaffold.

**Q:** How do you test a data pipeline at each level? [GENERAL] · Mid

Unit tests check pure transforms on tiny DataFrames with local Spark, which is what `tests/test_quality.py` does. Integration tests run the pipeline on a dev catalog with a small known input and assert on counts and keys. Data quality checks run on each production run and catch data that tests cannot predict. The three levels catch different bugs, so you need all three.

**Q:** What is Terraform, and what are its basics? [GENERAL] · Mid

Terraform is a declarative infrastructure-as-code tool: you describe resources in HCL, `terraform plan` shows the diff against the recorded state, and `terraform apply` makes the change. The state file records what exists, so it must be stored remotely and locked. Providers cover Azure (`azurerm`) and Databricks (`databricks`), so the same tool can create the storage account, workspace and Unity Catalog objects. The project does not use Terraform.

**Q:** What is Bicep, and how does it compare with Terraform? [GENERAL] · Mid

Bicep is Azure's domain-specific language that compiles to ARM templates, so it is Azure-only and uses Azure Resource Manager as its state, with no separate state file. Terraform is multi-cloud, uses its own state, and has a broader provider ecosystem, including Databricks. In an Azure-only team Bicep is a natural choice for the platform layer, and Terraform is common where teams manage several clouds or Databricks objects in code. The project uses neither; the bundle describes the Databricks job.

**Q:** How do you separate dev, test and prod environments? [GENERAL] · Junior

Use separate workspaces or at least separate catalogs and schemas, separate service principals and separate storage locations, and promote the same code through them. Bundle targets (`dev`, `test`, `prod`) hold the differences as variables. The rule is that prod data is never a development input, and developers never write to prod directly.

**Q:** Why use service principals for CI, and how does GitHub Actions authenticate to Azure without a stored secret? [GENERAL] · Mid

A service principal is a non-human identity in Microsoft Entra ID, so CI jobs do not run as a person and their access can be scoped and audited. Workload identity federation lets GitHub Actions request an OIDC token and exchange it with Entra ID for an access token, so no long-lived client secret needs to be stored. Grant the service principal only the permissions the deploy needs, on the dev or prod workspace only.

### 3.7 Cloud, Azure focus (8)

Each answer names the Azure service first, then the AWS and GCP equivalents.

**Q:** What is ADLS Gen2 and how does it relate to Databricks? [GENERAL] · Junior

ADLS Gen2 is Azure Data Lake Storage: blob storage with a hierarchical namespace that makes directory operations cheap and supports POSIX-style ACLs, accessed through the `abfss://` driver. Databricks reads and writes Delta tables stored there, and Unity Catalog controls access through storage credentials and external locations. Equivalents are Amazon S3 (with Lake Formation or IAM for access) and Google Cloud Storage (with IAM and Dataplex for governance).

**Q:** What is Azure Key Vault, and how is it used with Databricks? [GENERAL] · Junior

Key Vault stores secrets, keys and certificates, with access controlled by Azure RBAC or access policies and an audit log. A Databricks secret scope can be backed by a Key Vault, so notebooks read secrets with `dbutils.secrets.get` without storing them in code. The equivalents are AWS Secrets Manager or SSM Parameter Store, and Google Cloud Secret Manager.

**Q:** How do Entra ID, service principals and managed identities differ? [GENERAL] · Mid

Entra ID is the directory and identity provider; a service principal is the identity an application or CI job uses, with a secret or certificate or federated credential; a managed identity is an Azure-managed service principal with no credential to rotate, assigned to an Azure resource. Managed identities are the preferred option for Azure-to-Azure access. The AWS equivalents are IAM roles for workloads and instance profiles, and the GCP equivalents are service accounts and workload identity.

**Q:** How do managed identities let Databricks reach storage, and what is an Access Connector? [GENERAL] · Mid

Unity Catalog uses an Azure Databricks Access Connector, which is a managed identity, to access ADLS Gen2 through a storage credential. You grant that identity the Storage Blob Data role on the storage account, so no account key is shared with users. The AWS equivalent is an IAM role trusted by Databricks for the S3 bucket, and the GCP equivalent is a service account with bucket access.

**Q:** What is VNet injection, and why would you use it? [GENERAL] · Mid

VNet injection deploys the Databricks compute into your own Azure virtual network, so you control routing, network security groups, firewalls and private connectivity. The workspace uses a host and a container subnet, and you can disable public IPs with secure cluster connectivity. The AWS equivalent is deploying workspace compute into your own VPC, and on GCP the workspace runs in your project's VPC. The project uses Free Edition, so it has no VNet injection and I would say so.

**Q:** What are private endpoints, and how do they differ from service endpoints? [GENERAL] · Stretch

A private endpoint gives a PaaS resource such as storage or Key Vault a private IP address inside your VNet, through Azure Private Link, so traffic never crosses the public internet. A service endpoint keeps the public address but restricts traffic to your subnet, which is less isolated. The AWS equivalent is AWS PrivateLink with interface endpoints, and GCP uses Private Service Connect. Private endpoints need DNS configuration, which is a common source of failures.

**Q:** What are the security best practices for a Databricks data platform on Azure? [GENERAL] · Mid

Use least privilege through Unity Catalog grants to groups, managed identities or service principals instead of user keys, store secrets in Key Vault, restrict network access with private endpoints or VNet injection, and turn on audit logging. Keep environments separate, avoid personal access tokens in pipelines, and review access regularly. The AWS and GCP equivalents are IAM and Lake Formation, or IAM and Dataplex, with the same principles.

**Q:** What is data governance, and what does Unity Catalog give you? [GENERAL] · Mid

Governance is the set of rules for who can see and change data, how it is classified, and where it came from. Unity Catalog provides a central metastore with three-level naming, grants, lineage, audit and tags for classification, across workspaces in a region. It sits alongside cloud controls rather than replacing them, so storage access still depends on Azure RBAC on the storage account. AWS Lake Formation and GCP Dataplex address similar problems with different scope.

### 3.8 Python (6)

**Q:** How do pytest fixtures work, and how does the project use one for Spark? [PROJECT] · Junior

A pytest fixture is a function decorated with `@pytest.fixture` that provides setup to tests that request it by parameter name, and `scope="session"` creates it once per test run. `tests/conftest.py` defines a session fixture `spark` that returns the active session on Databricks or builds a `local[1]` session locally, so every test receives the same SparkSession. Fixtures can also yield for teardown, and `tmp_path` gives each test a temporary directory.

**Q:** How do you package a Python project with `pyproject.toml`, and what is a wheel? [GENERAL] · Mid

`pyproject.toml` (PEP 621) declares the project name, dependencies and build backend, so tools like `pip` and `build` can build it. `python -m build` produces a source distribution and a wheel (`.whl`), a prebuilt archive that installs quickly. The project only adds `[tool.pytest.ini_options]` with `pythonpath` and `testpaths`, so it is not a packaged library yet. For Databricks I would build a wheel and attach it to the job, rather than copy source files.

**Q:** How do type hints help, and how would you check them? [GENERAL] · Junior

Type hints document expected inputs and outputs, and a static checker such as mypy finds mismatches before runtime. The project uses hints like `batch_rows(day: int) -> list[dict[str, str]]`, which makes the generator's contract explicit. Hints are not enforced at runtime, so they help the reader and the checker, not the interpreter.

**Q:** How should Python code handle errors in a pipeline? [GENERAL] · Mid

Catch only the exceptions you can handle, raise specific exception types with a clear message, and use `raise ... from err` to keep the cause. Do not swallow exceptions silently. In the project, the TODO functions raise `NotImplementedError` with a TODO pointer, the notebooks catch only that exception, and `raise_on_critical` raises a `RuntimeError` listing failed checks so the job fails loudly.

**Q:** What are virtual environments, and how do you use one for this project? [GENERAL] · Junior

A virtual environment is an isolated Python installation with its own packages, so projects do not conflict. Create one with `python -m venv .venv`, activate it, and run `pip install -r requirements-dev.txt`; `uv` is a faster alternative. The `.venv/` folder is excluded from bundle sync, so local environments never reach the workspace.

**Q:** Why does `conftest.py` add `src` to `sys.path`, and what is the alternative? [PROJECT] · Mid

Notebooks and tests import `shipments_pipeline` from `src/`, which is not an installed package, so the test runner needs `src` on the path. `tests/conftest.py` inserts the path explicitly, and `pyproject.toml` sets `pythonpath = ["src", "."]` for pytest. The cleaner alternative is to install the package in editable mode (`pip install -e .`) so imports work without path hacks, which I would do once the project is packaged.

---

## 4. Questions to ask the interviewer

Each question is a suggestion; the note says what a good answer sounds like.

### Team and process

**Q:** How is the data engineering team organised, and where would this role sit between data engineering, analytics and platform? [GENERAL]
Listen for a clear split of ownership between platform and pipelines.

**Q:** How do you review and merge changes to production pipelines, and which checks must pass first? [GENERAL]
Listen for PR reviews, automated tests and a deploy gate.

**Q:** What does on-call look like for data jobs, and what failure do people get paged for most often? [GENERAL]
Listen for a runbook and an honest answer about the usual failure mode.

**Q:** Who owns a data contract when an upstream source changes, and how is the change communicated? [GENERAL]
Listen for a named owner and a change-notice process rather than ad hoc fixes.

### Data platform

**Q:** Which parts of the platform are governed by Unity Catalog today, and how are storage credentials and external locations managed? [GENERAL]
Listen for managed identities and grants to groups, not shared keys.

**Q:** Do you use Auto Loader or Lakeflow Declarative Pipelines, and what drove that decision? [GENERAL]
Listen for a reason tied to team skills, volume or maintenance cost.

**Q:** How are environments separated (workspaces, catalogs, service principals), and how is code promoted between them? [GENERAL]
Listen for a bundle or Terraform based promotion path.

**Q:** How do you track and attribute Databricks cost to teams or jobs? [GENERAL]
Listen for tags, budgets and a regular usage review.

### Growth

**Q:** What does a successful first 90 days look like for someone in this role? [GENERAL]
Listen for concrete deliverables and a named mentor.

**Q:** Does the team support certifications or internal training, for example the Databricks Certified Data Engineer Associate? [GENERAL]
Listen for funded time and a learning budget.

**Q:** What separates a strong engineer on this team from an average one? [GENERAL]
Listen for specific behaviours, such as writing tests, explaining trade-offs and owning incidents.

---

## 5. Night-before cheat sheet

| Term | One-line definition |
|---|---|
| Driver | Runs your program, builds the plan, coordinates tasks |
| Executor | Worker process that runs tasks over partitions |
| Lazy evaluation | Transformations only build a plan; actions run it |
| Transformation | Returns a new DataFrame, lazy (`filter`, `join`) |
| Action | Triggers execution (`count`, `collect`, `write`) |
| Narrow transformation | Each output partition depends on one input partition; no shuffle |
| Wide transformation | Needs rows with the same key together; causes a shuffle |
| Shuffle | Redistributes data across executors; costly I/O and network |
| Partition | Unit of parallel work; one task per partition |
| `repartition` | Full shuffle; can raise or lower partition count |
| `coalesce` | Lowers partition count without a full shuffle |
| Broadcast join | Sends the small table to every executor; no shuffle of the big side |
| Sort-merge join | Shuffles and sorts both sides by key, then merges |
| Data skew | One key holds far more rows; one task straggles |
| AQE | Runtime re-optimisation: coalesce, join switch, skew split |
| Cache / persist | Keeps a reused DataFrame in memory or disk |
| Pandas UDF | Vectorised Python UDF using Arrow batches |
| Spark Connect | Client-server Spark over gRPC; no SparkContext or RDDs |
| Delta transaction log | `_delta_log` JSON commits and periodic Parquet checkpoints |
| Time travel | Read an older version with `VERSION AS OF` or `TIMESTAMP AS OF` |
| MERGE | Upsert: `WHEN MATCHED` update, `WHEN NOT MATCHED` insert |
| Multiple source rows matched | MERGE error when one target row matches several source rows |
| Schema enforcement | Default: writes with a mismatched schema fail |
| Schema evolution | Opt-in (`mergeSchema`, `WITH SCHEMA EVOLUTION`) to add columns |
| OPTIMIZE | Compacts small files into larger ones; creates a version |
| Z-ORDER | Co-locates values for data skipping; manual, older approach |
| Liquid clustering | `CLUSTER BY`; replaces partitioning and Z-ORDER for new tables |
| VACUUM | Deletes unreferenced files older than retention (7 days default) |
| Change Data Feed | Row-level change records between versions (`_change_type`) |
| SCD Type 1 | Overwrite attribute in place; no history |
| SCD Type 2 | Expire old row, insert new with validity dates and `is_current` |
| Deletion vector | Marks rows deleted in a bitmap instead of rewriting files |
| Optimistic concurrency | Commit fails on conflict; retry the job |
| Unity Catalog | Governance: `catalog.schema.object`, grants, lineage, audit |
| Volume | Governed location for files in UC (`/Volumes/...`) |
| Lakeflow Declarative Pipelines | Declarative pipelines with expectations (formerly DLT) |
| Expectation | Data quality rule: warn, drop row, or fail update |
| Auto Loader | Incremental file discovery with checkpoint and schema location |
| Asset Bundle | Jobs and resources as YAML, deployed by the Databricks CLI |
| Serverless compute | Managed compute, no cluster config, billed on usage |
| Idempotency | Running twice gives the same result as running once |
| Star schema | Fact table surrounded by dimension tables |
| Late-arriving data | Event arrives after later events; compare on event time |
| Data contract | Agreed schema, semantics and delivery between producer and consumer |
| Service principal | Non-human Entra ID identity for apps and CI |
| Managed identity | Azure-managed identity with no credential to rotate |
| Private endpoint | Private IP for a PaaS service via Azure Private Link |
| VNet injection | Databricks compute deployed into your own VNet |
| Key Vault-backed scope | Databricks secret scope that reads from Azure Key Vault |
| Bicep | Azure DSL compiling to ARM; no separate state file |
| Terraform | Multi-cloud IaC with a state file, plan and apply |
