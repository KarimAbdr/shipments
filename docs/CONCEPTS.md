# Concepts

Short explanations of the Spark, Delta and Databricks ideas that the TODOs use. Each entry ends with a **Where in this repo** line that points to the files, functions and TODOs where the idea appears. Where I am not sure a behaviour is the same on Databricks Free Edition serverless, I say so. The TODO list is in [`docs/TODO.md`](TODO.md).

## Spark driver and executors

Spark runs your program in two roles. The driver holds your Python code, builds the query plan and schedules the work. Executors run the tasks, each on a slice of the data, and hold data in memory or on local disk while they do so. On serverless compute you do not manage either: Databricks provides them, so you only see their effect in the plan and the timing.

**Where in this repo:** the whole pipeline runs through the driver; the shuffle that the executors do is visible in TODO-13 (`explain`) and TODO-04 (window).

## Spark Connect and serverless

Serverless notebooks and jobs use Spark Connect. Your notebook sends an unresolved plan to a remote Spark server over gRPC, and the server runs it. The consequence for this repo: `spark.sparkContext`, RDDs, DBFS paths and cluster configs are not available, so everything is written with the DataFrame API, Spark SQL, Unity Catalog names and `/Volumes/...` paths. Local tests use classic PySpark, which is why the code in `src/` must work in both.

**Where in this repo:** `src/shipments_pipeline/config.py` (`volume_path`, `table`), `tests/conftest.py` (local `local[1]` session versus the active Databricks session). Serverless behaviour for these is not yet verified by the scaffold author on Free Edition.

## Lazy evaluation

Transformations such as `select`, `filter` and `withColumn` do not run when you call them. They extend a plan. Spark runs the plan only when an action asks for a result. This is why a notebook can define a whole pipeline and only do work at the first `count()` or `write`.

**Where in this repo:** `bronze.read_batch` only builds a plan; the counts printed in `10_bronze` and `20_silver` are the actions that run it. TODO-01 asks you to look at the plan with `explain()`.

## Transformations vs actions

Transformations return a new DataFrame and are lazy. Actions, such as `count()`, `collect()`, `show()`, `display()` and writes, trigger execution. A common mistake is calling `count()` inside a loop, which runs the whole plan once per iteration. Cache or write an intermediate table if the same result is used many times.

**Where in this repo:** the row-count prints in `20_silver` and `40_quality_checks` are actions; the transformations are in `quality.py`, `silver.py` and `gold.py`.

## Narrow vs wide transformations

A narrow transformation (filter, select, withColumn) needs data from one input partition to build one output partition, so it moves no data between executors. A wide transformation (groupBy, join, distinct, orderBy, a window with `partitionBy`) needs rows with the same key to meet, so Spark has to shuffle. Wide transformations are where most of the time goes.

**Where in this repo:** narrow: `quality.py` rules. Wide: `gold.build_region_part` (groupBy) and TODO-04 `dedup_latest` (window with `partitionBy("shipment_id")`).

## Shuffle

A shuffle redistributes rows across executors by key. It writes intermediate data to disk and sends it over the network, so it is the most expensive step in most Spark jobs. In a physical plan, a shuffle appears as an `Exchange` node.

**Where in this repo:** TODO-13 asks you to find the `Exchange` in the gold query and say why it is there. The note goes in `docs/NOTES.md`.

## Partitions

A partition is the unit of parallel work: one task processes one partition. Too few partitions leave executors idle; too many small partitions add scheduling overhead. The number of shuffle partitions is controlled by `spark.sql.shuffle.partitions`. Test sessions set it to 1 so that tests are fast and predictable.

**Where in this repo:** `tests/conftest.py` sets `spark.sql.shuffle.partitions=1` for the local session. File layout on disk is a separate topic; see OPTIMIZE and small files.

## Query plans and explain

Spark builds a logical plan, optimises it, and then chooses a physical plan. `df.explain()` prints the physical plan; `df.explain("formatted")` prints it with details. Read it from the bottom up: the leaves are the file scans, and the top is the final result. Look for `Exchange` (shuffle), `BroadcastHashJoin` or `SortMergeJoin` (join strategy), and `Filter` or `PushedFilters` (whether a filter reached the file scan).

**Where in this repo:** TODO-01 (`spark.table(cfg.table("gold_region_part")).explain()`), TODO-13 (gold query), TODO-17 (join comparison). Plan text on serverless may differ from classic Spark; not yet verified by the scaffold author on Free Edition.

## Broadcast join

A broadcast hash join copies a small table to every executor, so the large table is not shuffled for the join. Spark chooses it automatically when a table is below the `spark.sql.autoBroadcastJoinThreshold` (10 MB by default in open-source Spark). You can request it with `F.broadcast(df)`. The benefit is the avoided shuffle of the big side, not a guaranteed speed-up on small data.

**Where in this repo:** TODO-17 joins silver with `dim_supplier` in `30_gold`, with and without `F.broadcast`. Serverless join behaviour is not yet verified by the scaffold author on Free Edition.

## Adaptive Query Execution

Adaptive Query Execution (AQE) lets Spark change the plan while it runs, using the actual sizes of shuffle outputs. It can merge small shuffle partitions, switch a sort-merge join to a broadcast join when one side turns out to be small, and split skewed partitions. In recent Spark versions and Databricks runtimes it is on by default, but on Free Edition serverless you should check the setting rather than assume it.

**Where in this repo:** TODO-17 asks you to read about AQE and see whether it changes the join plan. Not yet verified by the scaffold author on Free Edition.

## Delta Lake and the transaction log

A Delta table is a set of Parquet data files plus a `_delta_log` folder. The log is an ordered list of commits, each describing which files were added or removed, and it is periodically summarised in checkpoints. A reader builds the table from the log, so it sees a consistent snapshot, and a writer adds a new commit rather than rewriting history.

**Where in this repo:** `DESCRIBE HISTORY` in `50_delta_ops` and in TODO-11 lists the commits (WRITE, MERGE, DELETE, RESTORE, and so on).

## ACID

ACID means atomicity, consistency, isolation and durability. Delta gets atomic commits by writing the log entry as one unit: either the new commit is visible or it is not, so a failed write leaves the table as it was. Concurrent writers are checked against the log before they commit. Partially written data files are not visible until their commit succeeds.

**Where in this repo:** every write to `silver_shipments`, `dim_supplier` and `quarantine_shipments` is a single commit. A failed run should leave the previous state readable.

## Time travel and RESTORE

`SELECT ... VERSION AS OF n` (or `TIMESTAMP AS OF`) reads the table as it was at an earlier commit. `RESTORE TABLE t TO VERSION AS OF n` makes the current table equal to that version, and it does so by adding a new commit, so the bad writes stay in the history. Time travel depends on old data files still existing, which is controlled by retention.

**Where in this repo:** `50_delta_ops` reference cell (`VERSION AS OF 0` against the current count), TODO-11 (`EXCEPT`, a simulated `DELETE`, then `RESTORE`).

## MERGE and upsert

`MERGE INTO target USING source ON key` updates matching rows, inserts unmatched ones, and can delete. Each clause can have a condition, so an update can apply only when the source is newer. The key requirement is that each target row matches at most one source row; if two source rows match the same target row, the MERGE fails.

**Where in this repo:** `dims.upsert_dim_supplier` is the reference, and its comments explain each clause. TODO-08 `merge_into_silver` uses the `s.event_ts > t.event_ts` guard. TODO-16 `upsert_dim_supplier_scd2` is the stretch version.

## Idempotency

An operation is idempotent when running it twice gives the same result as running it once. For ingestion this means knowing which files have already been loaded; for a MERGE it means that re-applying the same update changes nothing. Without idempotency, a retry after a failure silently duplicates data.

**Where in this repo:** `bronze.list_new_files` (TODO-07) skips files already ingested; the `event_ts` guard in `merge_into_silver` (TODO-08) makes a second run a no-op. The `unprocessed_bronze` reference in `silver.py` uses the same file-based idea.

## Schema enforcement vs evolution

By default Delta rejects a write whose columns do not match the table schema. That is schema enforcement: it protects downstream readers from unexpected data. Schema evolution is the deliberate opt-in to add new columns, for example with `mergeSchema` on a write or with schema evolution in MERGE. Enforcement is the default; evolution should be switched on only for the write that needs it.

**Where in this repo:** day 3 adds `carrier`. The append in `10_bronze` fails until TODO-10 is done, and TODO-10 asks you to allow the new column with `mergeSchema` (bronze) and schema evolution (silver MERGE).

## OPTIMIZE and small files

Each write adds files. Many small files mean many file opens and many tasks for a small amount of data. `OPTIMIZE` rewrites small files into larger ones. It does not remove the old files; `VACUUM` does that later, once they are no longer needed for time travel.

**Where in this repo:** TODO-12 runs `OPTIMIZE` on silver and compares `numFiles` before and after.

## Z-ORDER vs liquid clustering

`OPTIMIZE ... ZORDER BY (cols)` co-locates rows with similar values of the listed columns in the same files, so filters on those columns can skip more files. Liquid clustering is set with `CLUSTER BY (cols)`; the table then keeps its data clustered as new data arrives, and `OPTIMIZE` works incrementally. You cannot use both on the same table. Liquid clustering is the newer option, and the Databricks docs recommend it for new tables.

**Where in this repo:** TODO-12 `ALTER TABLE silver CLUSTER BY (region, event_date)`. Liquid clustering on Free Edition is not yet verified by the scaffold author.

## VACUUM

`VACUUM` deletes data files that are no longer referenced by the current table version and are older than the retention period. The default retention is seven days. Shortening it with `RETAIN 0 HOURS` removes the files that time travel, `RESTORE` and concurrent readers still need. This repo never runs that command.

**Where in this repo:** a warning in `50_delta_ops` and in TODO-11. Running `VACUUM` is not part of the core TODOs.

## Medallion architecture

Medallion architecture names three layers. Bronze keeps the raw data, as it arrived, plus metadata about the load. Silver holds cleaned, validated and deduplicated data. Gold holds aggregates for a specific use. Each layer has one job, so a problem can be traced back to the layer that introduced it.

**Where in this repo:** `bronze_shipments` (bronze.py), `silver_shipments` (silver.py), `gold_region_part` and `gold_supplier_scorecard` (gold.py). The diagram in the README shows the flow.

## Quarantine pattern

Rows that fail a rule are not dropped. They go to a quarantine table together with the list of reasons, so you can count them, explain them, and fix the source. Silver receives only the rows with no reason.

**Where in this repo:** `quality.split_valid_quarantine` (reference) writes the two sides; `quarantine_shipments` holds `_dq_reasons`. TODO-02 adds the remaining rules.

## Data quality checks

Row-level rules (quality.py) decide whether a single row is valid. Table-level checks look at the whole table: uniqueness of a key, no empty output, or reconciliation between layers (no shipment lost). Each check has a severity. A critical failure should stop the job, so bad data cannot flow on quietly, and results are written to a table so you can track them over time.

**Where in this repo:** `checks.check_no_shipment_lost` (reference), `results_to_df` writes to `dq_results`, TODO-09 adds more checks and `raise_on_critical`.

## SCD type 1 vs type 2

A slowly changing dimension is a table of attributes (here, supplier name, country and rating) that change over time. Type 1 overwrites the old value, so history is lost. Type 2 keeps each version as its own row with `valid_from`, `valid_to` and `is_current`, so a report can ask what was true on a given date.

**Where in this repo:** `dims.upsert_dim_supplier` is SCD1 (reference). TODO-16 builds SCD2 and expects 28 rows after v1 and v2, with 25 current rows.

## Unity Catalog names and volumes

Unity Catalog names tables with three levels: `catalog.schema.table`. A volume is governed storage for files, addressed as `/Volumes/<catalog>/<schema>/<volume>/...`. This repo keeps raw CSV batches in a volume and tables in `workspace.shipments`, and every name is built by one config object, so the names can change in one place.

**Where in this repo:** `config.PipelineConfig` (`catalog`, `schema`, `volume`, `volume_path`, `table`), `00_generate_batches` (`CREATE VOLUME`). Volumes: https://docs.databricks.com/aws/en/volumes/

## File metadata column

Spark exposes a hidden `_metadata` column when you read files. It includes fields such as `file_path`, `file_name` and `file_modification_time`. Selecting `_metadata.file_path` at read time tells you which file each row came from, which is what makes file-based incremental loads possible. Select it explicitly, because it is not part of `SELECT *` by default.

**Where in this repo:** `bronze.read_batch` adds `_source_file` from `_metadata.file_path`; TODO-07 uses it. The exact string format of the path on your runtime is not yet verified by the scaffold author on Free Edition. Docs: https://docs.databricks.com/aws/en/ingestion/file-metadata-column

## Databricks Jobs and task dependencies

A Databricks job is a set of tasks with dependencies. A task runs only after the tasks it `depends_on` have succeeded, and tasks can receive parameters such as `run_id`. Together these form a directed acyclic graph, and the UI and the bundle YAML both show it.

**Where in this repo:** `resources/shipments_job.yml`: `generate` → `bronze` today, and TODO-14 adds silver, gold and quality tasks. Dynamic values such as `{{job.run_id}}` are described at https://docs.databricks.com/aws/en/jobs/dynamic-value-references. Running jobs is not verified by the scaffold author on Free Edition.

## Asset Bundles

Databricks Asset Bundles describe a project's resources (jobs, notebooks, settings) as YAML in the repository, so changes can be reviewed and tested like code. The Databricks CLI can validate the bundle, deploy it, and run it. This project uses only `databricks bundle validate`: it never deploys.

**Where in this repo:** `databricks.yml` and `resources/shipments_job.yml`. Validation may need CLI authentication to a workspace; not yet verified by the scaffold author. Docs: https://docs.databricks.com/aws/en/dev-tools/bundles/

## Git folders

A Git folder in a Databricks workspace is a folder linked to a Git repository, so notebooks and `src/` come from a repository you control. This is how you bring the fork into Free Edition. Changes can be committed and pulled from the workspace UI. Keep the repository the source of truth, so that notebook edits are not the only copy of your work.

**Where in this repo:** README step 3 ("Workspace → Create → Git folder"). Docs: https://docs.databricks.com/aws/en/repos/

## Unit-testing PySpark

Put logic in functions that take a DataFrame and return a DataFrame. Then a test can build a small DataFrame with `spark.createDataFrame`, run the function, and check the result. A fixture starts one Spark session for the whole test run, so the tests stay fast. Logic that does not need Spark (the generator) is tested as plain Python.

**Where in this repo:** `tests/conftest.py` (`spark` fixture), `tests/test_quality.py` (`test_missing_supplier_rule`, TODO-03), `tests/test_silver.py` (TODO-05), `tests/test_generator.py` (pure Python). Running these needs Java 17 for local Spark.

## Monitoring and cost awareness

A pipeline should record what it did: rows read, rows written, which task ran, and whether it succeeded. The `pipeline_runs` table is that record. Cost is the other side: serverless compute is billed by usage, and the `system` catalog has billing tables that show usage by SKU.

**Where in this repo:** `audit.log_run` (reference) writes to `pipeline_runs`; `60_monitoring` shows its last rows. TODO-13 adds the logging to every task, and TODO-18 queries `system.billing.usage`. Whether system tables are available on Free Edition is not yet verified; the docs are at https://docs.databricks.com/aws/en/admin/system-tables/billing
