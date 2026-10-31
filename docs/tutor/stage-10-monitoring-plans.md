# Stage 10: Audit log and reading the query plan (TODO-13)

Tutor note: the learner is an absolute beginner. Explain in Russian. Keep code, identifiers and commands in English.

## 1. Goal

Log every task of the pipeline into one audit table, `pipeline_runs`, and read a physical query plan to find the shuffle (the `Exchange` step) in the gold aggregate, then explain why it is there.

**Interview sentence the learner should be able to say:** "Every task writes one audit row with its run id and row count, so I can answer 'what ran and how much did it touch' with SQL. I read the physical plan with explain('formatted') and found the Exchange that the groupBy causes, which is a shuffle between executors."

## 2. Before you start

- Stages 00-09 are done. `10_bronze` already writes audit rows (the reference), so the learner has seen one before.
- `workspace.shipments.pipeline_runs` exists (created by the first `log_run` call in `10_bronze`).
- Files to open: `src/shipments_pipeline/audit.py` (`log_run`), `notebooks/10_bronze.py` (the call site), `notebooks/20_silver.py`, `notebooks/30_gold.py`, `notebooks/40_quality_checks.py`, `notebooks/60_monitoring.py`, `src/shipments_pipeline/gold.py` (`build_region_part`), `docs/TODO.md#todo-13`.
- Ask first: "Does `pipeline_runs` have rows from `10_bronze`? Which run id did you use?"

## 3. Concepts to teach first

### 3.1 The audit table

- **Plain explanation:** an audit table records what happened: which task, which table, how many rows, status, and when. You query it with SQL instead of reading notebook output.
- **Analogy:** the logbook of a ship. Every watch writes one line: who, where, what, how much.
- **Where in this repo:** `audit.log_run(spark, table_fqn, run_id, task, target_table, rows, status="SUCCESS")`. It builds a one-row DataFrame, adds `logged_at`, and appends it. `RUN_LOG_SCHEMA` lists the columns. The call in `10_bronze` looks like this (reference, not a TODO):
  ```python
  audit.log_run(spark, cfg.table(config.PIPELINE_RUNS), run_id, "bronze", bronze_table, rows_written)
  ```
- **Tiny example (read the audit table, read only):**
  ```sql
  SELECT run_id, task, target_table, rows, status, logged_at
  FROM workspace.shipments.pipeline_runs
  ORDER BY logged_at DESC
  LIMIT 20;
  ```
  Ask: which task wrote the most recent row? This is the same query as `60_monitoring` section 1.

### 3.2 `run_id`: which run is this row from?

- **Plain explanation:** `run_id` groups all rows from one run. Jobs pass it in as `{{job.run_id}}`. In a notebook it comes from a widget and defaults to `manual`.
- **Analogy:** a shipment number on every box of one delivery.
- **Where in this repo:** `config.get_widget(dbutils, "run_id", "manual")` in each notebook's setup cell.

### 3.3 The physical plan and `explain`

- **Plain explanation:** Spark turns your code into a plan of steps. `explain()` prints the plan. `explain("formatted")` prints the same plan with numbered operators and details. Read it from the bottom up: scans at the bottom, the final result at the top.
- **Analogy:** a recipe with steps numbered from the pantry (bottom) to the plate (top).
- **Tiny example (toy data, scratch cell):**
  ```python
  from pyspark.sql import functions as F
  demo = spark.range(10).withColumn("g", F.col("id") % 3)
  demo.groupBy("g").count().explain("formatted")
  ```
  Look for the word `Exchange`. Ask: which operator comes right after the first `HashAggregate`, and what key does it partition by?
- **Note:** `explain` prints to the cell output and returns `None`. `x = df.explain()` followed by `print(x)` prints `None`, which surprises many beginners.

### 3.4 `Exchange` is a shuffle

- **Plain explanation:** an `Exchange` step moves rows between executors so that rows with the same key end up together. That is a shuffle, and it is the expensive step, because data is written, sent over the network and read again.
- **Analogy:** re-sorting letters between post offices by city. Every office sends letters to the office for each city. It takes time, but you need it to count per city.
- **Where in this repo:** `gold.build_region_part` groups silver by `region` and `part_type`, so the aggregate causes an `Exchange`. TODO-04 (window) also causes one.
- **Trap:** `spark.table(gold_region_part).explain()` reads the already-built gold table. Its plan is a file scan with no `Exchange`. To see the shuffle you must explain the query that builds the aggregate from silver. TODO-01 uses the gold table, which is a useful first look but not the TODO-13 answer.

### 3.5 Spark UI and query profile (serverless)

- **Plain explanation:** the Spark UI shows jobs, stages and tasks. On serverless, the UI may be replaced by a query profile attached to the cell or query history.
- **Analogy:** a dashboard on the car. Some gauges may be in a different place on a different model.
- **Status:** expected, not verified by the scaffold author on Free Edition. Look for a link to the query profile or Spark UI next to a finished cell. If you cannot find it, write that in your notes; do not invent what it shows.

### Say it like this (RU)

- "Exchange в плане — это место, где данные переезжают между машинами, как письма, которые пересортировывают между почтовыми отделениями по городам. Дорого, поэтому его ищем первым."
- "Таблица `pipeline_runs` — это бортовой журнал: каждая задача пишет одну строку — кто, что, сколько. Тогда вопрос «что случилось вчера» решается SQL-запросом, а не чтением логов."
- "План читаем снизу вверх: снизу берём сырые данные, сверху получаем результат. Как рецепт, где первый шаг — это продукты из кладовой."

## 4. Step-by-step plan

1. **Read the audit table.** Run the query from section 3.1.
   - check: you can name the task of the latest row and its `rows` value.
2. **Read the reference call.** Open `10_bronze` and find the `log_run` call. Name each argument.
   - check: the learner can say which argument is the count and why it is the count that was written (not the count of the input).
3. **Add logging to `20_silver`.** Put one `log_run` call at the end of the silver section with task name `silver`, and log the number of rows that were actually written.
   - check: `SELECT * FROM workspace.shipments.pipeline_runs WHERE task = 'silver' ORDER BY logged_at DESC LIMIT 1` shows a row with the expected count.
4. **Add logging to `30_gold` and `40_quality_checks`.** Use task names `gold` and `dq`. For `dq`, log the number of results.
   - check: after one full run (00, 10, 20, 30, 40), there is one row per task for that run id: `SELECT task, COUNT(*) FROM workspace.shipments.pipeline_runs WHERE run_id = '<your run id>' GROUP BY task`.
5. **Explain the aggregate, not the table.** Build the same DataFrame that `gold.build_region_part` builds from silver, and call `explain("formatted")`.
   - check: the output contains `Exchange`. Name the key in its `hashpartitioning(...)`, and the groupBy that causes it.
6. **Write the note.** In `docs/NOTES.md`, write where the shuffle is, what key it partitions by, and why it is needed for a groupBy.
   - check: the note refers to a line of the plan, not a guess.
7. **Compare with the table scan.** Run `explain("formatted")` on the gold table itself, and compare.
   - check: the learner sees that the gold table scan has no `Exchange`, and can explain why.

## 5. Hint ladder per TODO

### TODO-13 (log every task, explain the gold query, find the shuffle)

- **Level 0 (goal):** Every task (silver, gold, dq) writes one audit row per run. You write down where the shuffle is in the gold query.
- **Level 1 (concept):** The audit row records what the task wrote, not what it read. The plan shows which step moves data between machines, and that step is usually where the time goes.
- **Level 2 (tool):** `audit.log_run` (already in `audit.py`; `10_bronze` shows the call). `DataFrame.explain("formatted")`. Look for `Exchange` in the output and for the `groupBy` that caused it.
- **Level 3 (skeleton with blanks):**
  ```python
  # at the end of the silver section
  audit.log_run(spark, cfg.table(config.PIPELINE_RUNS), run_id, "___", ___, ___)

  # in the gold section: the aggregate's plan, built from silver
  gold.___(spark.table(silver_table)).explain("___")
  ```
  Notes for the learner: the first argument after `run_id` is the task name; the target table is the table this task wrote; the rows value is an integer count that you computed from the table you wrote.
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "one log line per task, with the rows actually written, and one plan of the aggregate that you read bottom-up to find the Exchange". Then write your own calls and retype any fixes yourself. Afterwards give a variation: "Add a `status` of `FAILED` in a `try/except` around one task. What would a query on `pipeline_runs` show?"

## 6. Common mistakes and error messages

| Symptom | Likely cause | What to ask or suggest |
|---|---|---|
| `rows` looks like the input count, not the output | The count was taken before the filter or dedup | "Which DataFrame did you count? What did this task write?" |
| Every row has `run_id = manual` | The widget was not set when the notebook ran | "Did you pass `run_id` as a widget or job parameter?" |
| No `Exchange` in the plan | Explained the gold table, or the aggregate has no groupBy | "Is this the table or the query that builds the table?" |
| `explain` output is `None` | `print(df.explain())` or `x = df.explain()` | "`explain` prints. Call it on its own line." |
| Mode error from `explain` | Typo in the mode name | "Check the spelling of `formatted`." (exact error wording varies, expected) |
| `log_run` fails on the first call | The schema of `pipeline_runs` or the Python types do not match what `log_run` expects | "Look at `RUN_LOG_SCHEMA`. Is `rows` an int?" |
| The silver log row is missing after a failed run | The `log_run` call sits after the step that failed | "Where is the call in the cell? Should a failed run leave a row? (Discuss: a status of FAILED would need a try/except.)" |
| The Spark UI link is missing | Serverless UI differs | Expected, not verified. Write what you see. |
| `TABLE_OR_VIEW_NOT_FOUND` for `pipeline_runs` | The audit table does not exist yet (no `log_run` has run) | "Did `10_bronze` run? Does `60_monitoring` say the table does not exist?" |

## 7. DONE WHEN

Copied from `docs/TODO.md`:

> **DONE WHEN:** pipeline_runs has a row per task per run; note in docs/NOTES.md where the shuffle is

Tutor check: after a full run, the grouping query in step 4 returns one row per task (silver, gold, dq, plus bronze). Then read `docs/NOTES.md` and check that the shuffle is named with its key.

Note: Databricks-only check. The scaffold author has not verified the plan text on serverless. Classic Spark plans may look different.

## 8. Check-your-understanding questions

1. What does one row in `pipeline_runs` mean? Good answer: one task, in one run, with the rows it wrote and its status.
2. Why log the rows the task wrote, not the rows it read? Good answer: the audit should match what reached the target table.
3. What is an `Exchange` in a Spark plan? Good answer: a shuffle, where rows move between executors by key.
4. Which operator in the gold query causes the shuffle? Good answer: the groupBy on `region` and `part_type`.
5. Why does the gold table scan show no `Exchange`? Good answer: it only reads files that are already grouped; the shuffle happened when the table was built.
6. Why is reading the plan bottom-up? Good answer: data flows from the scans at the bottom to the result at the top.
7. Why should you not claim a speed-up from one small run? Good answer: a small table is not a benchmark; the plan is the evidence.

## 9. Interview link

From `docs/INTERVIEW_PREP.md`:

- **Q:** "How do you read a query plan (`explain`) and find `Exchange`, `BroadcastHashJoin` and `SortMergeJoin`?" [PROJECT] Mid
- **Q:** "What is a shuffle, and why is it expensive?" [GENERAL] Mid
- **Q:** "How do you read the Spark UI, and what is different on serverless?" [GENERAL] Mid
- **Q:** "How do you monitor Databricks jobs and data quality?" [GENERAL] Mid

One sentence the learner can use: "Each task writes a row to pipeline_runs with the rows it wrote. I read the gold aggregate's physical plan, found the Exchange from the groupBy, and wrote down which key it partitions by."

## 10. Wrap-up

- Tick `- [ ] Done` under `TODO-13` in `docs/TODO.md`.
- Tick `TODO-13 Audit everywhere + query plan` in the README checklist.
- Suggested commit (the learner runs it, after checking `git status`):
  ```
  git add -A && git commit -m "feat(todo-13): audit log per task and shuffle note from the gold plan"
  ```
  Do not push without a deliberate decision.
- **Next stage:** `stage-11-bundle-and-ci.md` (TODO-14 and TODO-15). The learner will describe the job as YAML and add a test job to CI. Remember: `validate` only, never deploy.
