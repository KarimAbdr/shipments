# Stage 07: Table-level data quality checks (TODO-09)

Tutor note: the learner is an absolute beginner. Explain in Russian. Keep code, identifiers and commands in English.

## 1. Goal

Implement three functions in `checks.py` that look at a whole table (not one row), record the result in `dq_results`, and stop the job when a critical check fails.

**Interview sentence the learner should be able to say:** "Row rules catch bad rows; table checks catch problems no single row shows, like duplicate keys or an empty output. Each check has a severity, results are stored every run, and a critical failure stops the job."

## 2. Before you start

- Stages 00-06 are done. In particular TODO-02 (rules), TODO-04 (dedup) and TODO-08 (MERGE) matter here, because silver is only unique after dedup.
- `workspace.shipments.silver_shipments`, `bronze_shipments` and `dq_results` exist (`20_silver` and `40_quality_checks` ran at least once).
- Files to open: `src/shipments_pipeline/checks.py`, `notebooks/40_quality_checks.py`, `docs/TODO.md#todo-09`.
- Ask first: "Which stage are you on? Did `40_quality_checks` run cleanly last time?"

## 3. Concepts to teach first

Teach one concept per message. Ask "did that make sense?" after each.

### 3.1 Row-level rules vs table-level checks

- **Plain explanation:** a row rule asks "is this row OK?" (quantity above zero). A table check asks "is the whole table OK?" (is each shipment id present only once?).
- **Analogy:** a row rule is a cashier checking each banknote for a fake. A table check is the end-of-day count: does the cash match the sales?
- **Where in this repo:** row rules are in `quality.py` (`ALL_RULES`, `with_quality_flags`). Table checks are in `checks.py` (`check_no_shipment_lost` is the reference, `CheckResult` is its return type).
- **Tiny example (scratch cell):**
  ```python
  from shipments_pipeline.checks import CheckResult
  r = CheckResult(name="demo", severity="critical", passed=False, observed="3", expected="0")
  print(r.passed, r.severity)
  ```
  Ask: what does `passed=False` with `severity="critical"` mean for the job?

### 3.2 Severity and fail-fast

- **Plain explanation:** "critical" means the job must stop. "warning" means the result is only recorded. Failing fast means you stop at the first bad signal instead of loading bad data downstream.
- **Analogy:** a smoke alarm (critical, stops the building) versus a note on the fridge that the milk is nearly out (warning).
- **Where in this repo:** `CheckResult.severity` in `checks.py`; `raise_on_critical` (TODO-09); `40_quality_checks` section 4 calls it last.

### 3.3 Exceptions in Python (`raise`, `try`/`except`)

- **Plain explanation:** `raise` stops the code and says why. `try`/`except` catches a named error so the code can continue.
- **Analogy:** `raise` is a fire alarm. `except` is the person who decides whether this particular alarm is a drill.
- **Where in this repo:** `raise NotImplementedError(...)` marks every unfinished TODO. `quality.active_rules` and `40_quality_checks` catch `NotImplementedError` to skip unfinished work.
- **Tiny example:**
  ```python
  def boom():
      raise RuntimeError("critical check failed: demo")

  try:
      boom()
  except RuntimeError as e:
      print("caught:", e)
  ```
  Ask: what would happen if the notebook caught `Exception` instead of `RuntimeError` in the raise cell? (Answer later under mistakes.)

### 3.4 `groupBy` and `count` (finding duplicates)

- **Plain explanation:** `groupBy(col)` puts rows with the same value into one group, `count()` counts each group. Groups with a count above 1 are duplicates.
- **Analogy:** sorting a box of letters into piles by postcode, then counting each pile.
- **Where in this repo:** `gold.build_region_part` uses `groupBy`. TODO-09 `check_unique_key` will use it on `shipment_id`.
- **Tiny example (toy data, not the project):**
  ```python
  demo = spark.createDataFrame([("SHP1", 1), ("SHP1", 2), ("SHP2", 3)], "shipment_id string, n int")
  demo.groupBy("shipment_id").count().show()
  ```
  Ask: which `shipment_id` would a uniqueness check complain about, and how would you keep only those?

### 3.5 Writing evidence (`dq_results`)

- **Plain explanation:** every run writes its results to a table, so you can see yesterday's and today's checks.
- **Analogy:** a flight recorder. It is there even when the flight crashed.
- **Where in this repo:** `checks.results_to_df` adds `run_id` and `checked_at`; `40_quality_checks` section 3 appends to `dq_results` before section 4 can raise. That order is deliberate.

### Say it like this (RU)

- "Проверка на уровне строки — это кассир, который смотрит каждую купюру. Проверка на уровне таблицы — это итог за день: сошлась ли касса в целом."
- "Severity `critical` — это пожарная сигнализация: она останавливает здание. `warning` — это записка на холодильнике: её видно, но работа не стопорится."
- "`dq_results` — это журнал полёта: он пишется, даже если рейс закончился аварией. Поэтому сначала пишем результаты, потом поднимаем ошибку."

## 4. Step-by-step plan

1. **Read the reference.** Open `checks.py` and read `check_no_shipment_lost`. Say in your own words what it compares.
   - check: learner names the two sides being compared (bronze distinct shipments versus silver plus quarantine).
2. **Run `40_quality_checks` as it is.** The TODO cells print "skipped".
   - check: `SELECT check_name, passed FROM workspace.shipments.dq_results ORDER BY checked_at DESC LIMIT 5` shows `no_shipment_lost`.
3. **Implement `check_unique_key`.** Keep the `CheckResult` shape and `observed` as a string.
   - check: in a scratch cell, `checks.check_unique_key(spark.table("workspace.shipments.silver_shipments"), "shipment_id", "t").passed` is `True` after TODO-04 and `False` before it. Say which one you expect and why before you run it.
4. **Implement `check_not_empty`.**
   - check: `checks.check_not_empty(spark.createDataFrame([], "shipment_id string"), "t").passed` is `False`, and the same check on silver is `True`.
5. **Implement `raise_on_critical`.** It must do nothing when every result passes and raise `RuntimeError` when a critical result failed.
   - check: `checks.raise_on_critical([CheckResult("x", "critical", True, "0", "0")])` prints nothing; the same call with `passed=False` raises `RuntimeError`.
6. **Run `40_quality_checks` end to end.**
   - check: `SELECT COUNT(*) FROM workspace.shipments.dq_results WHERE run_id = 'manual'` grows by at least 3 for this run (the default `run_id` is `manual` when no widget is set).
7. **Break a check on purpose, then restore it.** Bronze has duplicate `shipment_id` values by design (day 1 has 1,260 rows and 1,245 distinct shipment ids), so `check_unique_key` on bronze must fail. Do it in a scratch cell first, not by editing the notebook.
   - check: the scratch call raises `RuntimeError`. If you also edit the notebook for the DONE WHEN, run `git diff` afterwards and confirm you reverted it.
8. **Tick and commit.** See section 10.

## 5. Hint ladder per TODO

### TODO-09 (three functions in `checks.py`, plus the two notebook cells)

- **Level 0 (goal):** Add two table checks (unique key, not empty) and make a failed critical check stop the notebook. The results must be saved first.
- **Level 1 (concept):** A table check asks one yes/no question about the whole table. "Is `shipment_id` unique?" "Is the table not empty?" "Critical" means stop. Results are evidence, so they are written before anything stops the job.
- **Level 2 (tool):** `CheckResult(name, severity, passed, observed, expected)` from `checks.py`. `DataFrame.groupBy(...).count()` and `.filter(...)` for duplicates. `DataFrame.isEmpty()` or `.count()` for emptiness. `raise RuntimeError(...)` with a message that lists the failed names. A list comprehension (`[x.name for x in results if ...]`) collects the names.
- **Level 3 (skeleton with blanks, pseudo-code only):**
  ```
  def check_unique_key(df, key, name):
      # 1. groupBy ___ and count; keep groups whose count is ___
      # 2. observed = number of such groups, as a str
      # 3. return CheckResult(name=name, severity="critical", passed=___, observed=___, expected="0")

  def check_not_empty(df, name):
      # return CheckResult(... passed = ___ ...)

  def raise_on_critical(results):
      # 1. failed = names of results where severity is "critical" and passed is ___
      # 2. if failed is not empty: raise RuntimeError(___)
  ```
- **Level 4 (solution, only on explicit request after trying):** One sentence: walk through the three functions line by line, then retype them yourself and run the DONE WHEN check. Afterwards give a variation: make `check_not_empty` a `warning` and predict what `raise_on_critical` does.

## 6. Common mistakes and error messages

| Symptom | Likely cause | What to ask or suggest |
|---|---|---|
| `check_unique_key` always passes | The check ran on silver before TODO-04, or on a deduplicated DataFrame | "Which DataFrame did you pass? Which one should contain duplicates?" Bronze does, by design. |
| The notebook never fails | `raise_on_critical` only prints, or the notebook catches `Exception` around it | "Which exception does the notebook catch? Would a `RuntimeError` be caught by `except NotImplementedError`?" |
| Type error when building `dq_results` | `observed` or `expected` is an int, but `RESULT_SCHEMA` expects a string | "Look at `RESULT_SCHEMA` in `checks.py`. What type does `observed` need?" Suggest `str(...)`. |
| `[TABLE_OR_VIEW_NOT_FOUND]` for `silver_shipments` | Notebook 40 ran before 20 | "Has `20_silver` created the silver table yet?" Run 20 first. |
| `dq_results` has duplicate rows across runs | Expected: the table is appended on each run | Explain that history is the point. Use `run_id` to separate runs. |
| Break test does not stop the notebook | The raise cell is skipped by an earlier `try/except`, or the check passed by accident | Print `[r.name for r in results]` and `passed` before the raise. |

Mistake to watch for: a `try/except` that hides the failure is worse than no check. Ask the learner what message they expect to see in the notebook when a critical check fails.

## 7. DONE WHEN

Copied from `docs/TODO.md`:

> **DONE WHEN:** dq_results has 3+ rows per run; temporarily break a check → notebook fails with RuntimeError

Tutor check: run `40_quality_checks`, then the counts query from step 6. Then run the break test from step 7 and confirm the RuntimeError, then restore the code.

Note: this is a Databricks-only check. The scaffold author has not verified it on Free Edition. The local part (`raise_on_critical` behaviour) can be checked in a scratch cell.

## 8. Check-your-understanding questions

1. What is the difference between a row-level rule and a table-level check? Good answer: a row rule decides one row; a table check looks at the whole table, for example uniqueness or row counts across layers.
2. What does `severity="critical"` change in the pipeline? Good answer: `raise_on_critical` raises, so the job stops after results are written.
3. Why are results written to `dq_results` before the raise? Good answer: a failed run still leaves evidence.
4. Why does bronze fail a unique-key check on `shipment_id` by design? Good answer: bronze keeps every event; day 1 has 1,260 rows and 1,245 distinct shipments.
5. Why must `check_unique_key` be run on silver after TODO-04 and not before? Good answer: before dedup, silver still holds the duplicates from the 15 later events.
6. Why is `except Exception` around the raise dangerous? Good answer: it hides every failure, including the one you wanted.
7. What is `groupBy(...).count()` doing, in words? Good answer: it puts rows with the same key into groups and counts each group.

## 9. Interview link

From `docs/INTERVIEW_PREP.md`:

- **Q:** "How do you monitor Databricks jobs and data quality?" [GENERAL] Mid
- **Q:** "What are the main data quality dimensions, and how does the project address them?" [PROJECT] Junior
- **Q:** "How do you test a data pipeline at each level?" [GENERAL] Mid

One sentence the learner can use: "Our pipeline has row rules in quality.py and table checks in checks.py. Results go to dq_results every run, and a critical failure raises before the job reports success."

## 10. Wrap-up

- Tick `- [ ] Done` under `TODO-09` in `docs/TODO.md`.
- Tick `TODO-09 More DQ checks` in the README checklist.
- Suggested commit (the learner runs it, after checking `git status`):
  ```
  git add -A && git commit -m "feat(todo-09): table-level dq checks and fail-fast"
  ```
  Do not push without a deliberate decision.
- **Next stage:** `stage-08-schema-evolution.md` (TODO-10). The learner will meet a new column on day 3 and see Delta refuse it on purpose.
