# Stage 02: Quality rules and tests (TODO-02, TODO-03)

> Tutor: read `docs/tutor/README.md` first. Explain in Russian; keep code in English. The learner writes four small rule
> functions in `src/shipments_pipeline/quality.py`, then four pytest tests in `tests/test_quality.py`. Local pytest needs
> **Java 17** and PySpark. If `java -version` fails on the learner's machine, use `notebooks/99_run_tests` on Databricks
> (expected, not verified by the scaffold author).

## 1. Goal in one sentence

The learner implements the four remaining quality rules (quantity, unit cost, date, status) as column expressions, so that the
day-1 quarantine grows from 12 to 45 rows with five reason values, and writes a test for each rule that fails if the rule breaks.

**Interview afterwards:** "Each rule is a column expression that returns a reason or NULL. I handle NULL on purpose, read bad
values as text and cast with `try_cast`, and I test both the failing and the passing case."

## 2. Before you start

- Stage 01 done: the day-1 reference run works (bronze 1,260, silver 1,248, quarantine 12).
- Local tests: `pip install -r requirements-dev.txt` in a virtual environment, Java 17 available, then `pytest -q` from the
  repo root. Ask the learner to show `java -version` first. When these guides were written, `java -version` on the learner's Mac
  reported no Java runtime, and the project's `.venv` had no `pyspark` installed. Check again before the test steps.
- Open together: `src/shipments_pipeline/quality.py` (whole file, about 100 lines), `tests/test_quality.py`,
  `tests/conftest.py`, `docs/CONCEPTS.md` sections "Quarantine pattern", "Data quality checks", and the top of
  "Unit-testing PySpark".
- Ask one warm-up question: "What does `rule_missing_supplier` return for a good row?" (Answer: NULL, because `F.when` has no
  `otherwise`.)

## 3. Concepts to teach first

### 3.1 Column expression versus value

**Plain explanation.** `F.col("quantity")` is not a number. It is an instruction Spark applies to every row. A value like `5`
is a fixed number. A comparison such as `F.col("quantity") > 0` is another instruction that produces true, false or NULL per row.

**Analogy.** A recipe card ("for each row, take quantity and check it") versus the actual number on the scale.

**Tiny example** (scratch cell, predict the output first):
```python
from pyspark.sql import functions as F
toy = spark.createDataFrame([(1, 0), (2, 5)], "id int, score int")
toy.select("id", (F.col("score") > 2).alias("is_high")).show()
```
Expected: row 1 false, row 2 true.

### 3.2 `F.when` and NULL when nothing matches

**Plain explanation.** `F.when(condition, value)` gives `value` where the condition is true. Without `.otherwise(...)`, every other
row gets NULL. The repo relies on this: a rule returns a reason text for a bad row and NULL for a good row.

**Analogy.** A sticky note that is written only when something is wrong. A clean desk has no note.

**Tiny example:**
```python
toy.select("id", F.when(F.col("score") > 2, F.lit("high")).alias("label")).show()
```
Expected: row 1 NULL, row 2 `high`. Ask: "why `F.lit`?" (It wraps a plain Python string as a column value.)

**Where in this repo.** `quality.rule_missing_supplier()` (reference). `with_quality_flags()` joins the reasons with
`F.concat_ws("; ", ...)`, which skips NULLs and gives an empty string `""` for a clean row.

### 3.3 NULL is not true and not false

**Plain explanation.** `NULL > 2` is neither true nor false. A filter keeps only rows where the condition is true, so a NULL
row falls out of both `filter(cond)` and `filter(~cond)`.

**Analogy.** A form field left blank. "Is it bigger than 2?" has no answer, so the row cannot pass the test.

**Tiny example:**
```python
nulls = spark.createDataFrame([(None,), (3,)], "score int")
nulls.filter(F.col("score") > 2).show()      # keeps the 3
nulls.filter(~(F.col("score") > 2)).show()   # keeps nothing: the NULL row vanished
```
The lesson: a rule like "quantity must be positive" must also say what to do with NULL.

### 3.4 Python `&`, `|`, `~` versus `and`, `or`, `not`

**Plain explanation.** Column conditions combine with `&` (and), `|` (or) and `~` (not). Each condition needs its own parentheses,
because `|` binds tighter than `==` in Python. Python `and`/`or` raise an error on Columns.

**Tiny example:**
```python
toy.filter((F.col("id") == 1) | (F.col("score") > 4)).show()   # keeps id 1 and id 2
```

### 3.5 `try_cast`: bad text becomes NULL

**Plain explanation.** Raw CSV values are text. `cast` turns them into numbers or dates. In ANSI mode (on serverless and in the
tests), a bad `cast` stops the job. `try_cast` returns NULL instead, and a rule then reports the NULL.

**Analogy.** A scanner that marks an unreadable barcode as "unknown" instead of stopping the conveyor belt.

**Tiny example:**
```python
spark.sql("SELECT try_cast('2026-10-01' AS DATE) AS good, try_cast('not-a-date' AS DATE) AS bad").show()
```
Expected: `2026-10-01` and NULL. Then say: plain `CAST('not-a-date' AS DATE)` raises an error under ANSI mode (expected wording
`[CAST_INVALID_INPUT]`).

**Where in this repo.** `quality.cast_types()` (reference) and `_CAST_RULES`. The four typed columns are `event_date` (DATE),
`event_ts` (TIMESTAMP), `quantity` (INT), `unit_cost_usd` (DECIMAL(12,2)).

### 3.6 pytest: test functions, `assert`, and the `spark` fixture

**Plain explanation.** A pytest test is a function named `test_...` that uses `assert` to say what must be true. If an assertion
is false, the test fails and shows both sides.

**Decorator.** A line that starts with `@` above a function changes how it is used. `@pytest.fixture` in `tests/conftest.py` makes
`spark` a value that pytest creates once and hands to every test that lists `spark` as a parameter.

**Tiny example** (not in the repo, just to see the shape):
```python
def test_addition():
    assert 1 + 1 == 2
```
Run: `pytest -q`. Show `pytest -q -rs`, which prints the reason for each skipped test.

**Where in this repo.** `tests/conftest.py` (`spark` fixture: active session, then Databricks runtime, then `local[1]`),
`tests/test_quality.py` (`test_missing_supplier_rule`, reference; four skipped TODO-03 tests).

## 4. Step-by-step plan

1. **Read `rule_missing_supplier`** in `quality.py`, line by line.
   *Check:* the learner says what `F.when`, `F.col`, `.isNull()`, `F.trim` and `F.lit` each do.
2. **Read `cast_types`** and `_CAST_RULES`.
   *Check:* the learner lists the four typed columns and the SQL type of each.
3. **Run the four toy cells in 3.1 to 3.5** in a scratch cell. Predict the output first.
   *Check:* the learner's predictions match, or they explain the difference.
4. **Implement the first rule, `rule_invalid_quantity`.** Test it in a scratch cell with `print(quality.rule_invalid_quantity())`.
   *Check:* it prints a Column, not an error.
5. **Implement the other three rules** (unit cost, date, status), one at a time, with the same check.
   *Check:* no `NotImplementedError` remains in `quality.py`.
6. **Run `20_silver`** (`USE_MERGE = False`).
   *Check:* the four `skipping rule_...` lines are gone. `QUARANTINE rows: 45`. Grouping by `_dq_reasons` shows five values
   with counts 12 (`missing_supplier`), 10 (`invalid_quantity`), 8 (`invalid_unit_cost`), 6 (`invalid_date`) and 9
   (`invalid_status`). Silver is 1,215 rows before dedup (1,200 valid plus 15 duplicate events); this is derived from
   `docs/SCAFFOLD_SPEC.md`, not verified by running.
7. **Write `test_invalid_quantity_rule`** in `tests/test_quality.py` (remove its skip; use the reference test's shape).
   *Check:* `pytest -q tests/test_quality.py -rs` shows it passed, not skipped.
8. **Write the other three tests** the same way.
   *Check:* `pytest -q tests/test_quality.py` shows 6 passed (2 reference tests plus your 4) and no skips.
9. **Optional, to prove a test can fail:** change one threshold in a scratch copy, see the test fail, then undo it.
   *Check:* the learner can say what the failure message shows.

## 5. Hint ladder per TODO

### TODO-02: remaining quality rules

| Level | What you give |
|---|---|
| 0 Goal | "Each bad row gets one reason text, the good rows get NULL. Day 1 yields 45 quarantined rows and five reason values." |
| 1 Concept | "A rule is a question asked of each row, with a label for the 'yes'. A missing value must be a 'yes' too, because a comparison with NULL gives NULL." |
| 2 Tool | "`F.when(condition, F.lit("reason"))`, `Column.isNull()`, `Column.isin(...)`, `F.trim`. Copy the shape of `rule_missing_supplier`. Reason names are in `docs/SCAFFOLD_SPEC.md` §2 (the `quality.py` section)." |
| 3 Skeleton | `def rule_invalid_quantity() -> Column:` / `return F.when(___ | ___, F.lit(___))`, where the first blank is "is missing" and the second is "is not positive". Same shape for the other three; for the status rule, a list of allowed values replaces "not positive". |
| 4 Solution | Described only: "each rule checks NULL explicitly, then the bad-value test, then returns the reason string." Ask the learner to write the condition and retype it. |

Variation after level 4 (only if they asked for it): "Make the cost rule also reject values above 10,000. What changes in the count?"

### TODO-03: tests for rules

| Level | What you give |
|---|---|
| 0 Goal | "Four tests, one per rule, each with a row that must be flagged and a row that must pass." |
| 1 Concept | "A test builds three or four tiny rows, runs the rule, and compares the reasons to what you expect. Each row must be valid in every column except the one under test, or another rule fires." |
| 2 Tool | "`spark.createDataFrame(rows, schema=QUALITY_SCHEMA)`, `quality.with_quality_flags(df).collect()`, `quality.REASONS_COL`. For the date rule, `None` stands for a date that could not be parsed. Valid rows get `""`, not NULL." |
| 3 Skeleton | `def test_invalid_quantity_rule(spark):` then `# rows: one bad quantity (___), one good quantity (___), all other columns valid` / `df = spark.createDataFrame(rows, schema=QUALITY_SCHEMA)` / `reasons = {...}` (copy the reference test) / `assert reasons == {___: "invalid_quantity", ___: ""}`. |
| 4 Solution | Described only: "the test feeds a bad and a good row, builds the reason dictionary, and asserts it." |

## 6. Common mistakes and error messages

| Symptom | Cause | What to ask or suggest |
|---|---|---|
| `ValueError: Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.` | Python `or` or `and` used between Columns. | "Which operator did you use? Put parentheses around each condition and use `|`." |
| Quarantine still 12 after the edit | The output still prints `skipping rule_...`, so a rule still raises `NotImplementedError`, or the rule returns NULL for bad rows. | "Read the printed lines. Which function is still skipped?" |
| Quarantine is 1,260 rows | A rule returns a non-empty string for good rows (for example `"ok"` through `.otherwise`). | "What does a clean row's reason look like? It must be `""`." |
| Quarantine is 45 but a row goes to silver with a bad value | A NULL check is missing (for example `quantity` NULL passes `quantity <= 0`). | "What does `NULL <= 0` give? Does a filter keep it?" |
| A status row passes | `~col.isin(...)` is NULL for a NULL status, so NULL rows slip through. | "Add an explicit NULL check for status." |
| `[CAST_INVALID_INPUT]`-style error in a test (expected wording) | The test casts a raw string with `F.col(...).cast("date")`. | "Use `cast_types` as the reference test does, or pass a real date." |
| `AssertionError` shows two dictionaries | Expected and actual differ. | "Read the left side (actual) and right side (expected). Which event_id differs, and which rule fired?" |
| Test passes but for the wrong reason | Another rule fires on that row, or the assert checks only one key. | "Make the other columns valid and assert on every event_id." |
| Output shows `s` or `1 skipped` | Skip not removed. | "Delete the `pytest.skip(...)` line in that test." |
| `ModuleNotFoundError: No module named 'shipments_pipeline'` | Pytest run from another folder, or venv not active. | "Run pytest from the repo root. `pyproject.toml` already adds `src` to the path." |
| `fixture 'spark' not found` | Test file or conftest in the wrong place. | "Is the file in `tests/`? Is the parameter called exactly `spark`?" |
| `PySparkRuntimeError: [JAVA_GATEWAY_EXITED]` or "Java gateway process exited before sending its port number" (expected wording) | Java 17 missing or wrong version. | "Run `java -version`. Install Java 17, or run `99_run_tests` on Databricks." |

## 7. DONE WHEN

From `docs/TODO.md`, copied exactly:

TODO-02 (copied exactly):

> after rerun of 20_silver (day 1): quarantine 45 rows, 5 distinct reason values; after TODO-03, the 4 rule tests pass (not skipped)

TODO-03 (copied exactly):

> `pytest -q` shows the 4 tests passed, not skipped

Note for the tutor: the four rule tests are local and do not need Databricks. The 20_silver check needs Databricks
(expected, not verified by the scaffold author).

## 8. Check-your-understanding questions

1. What does `F.when` return for a row that matches no condition, and why does that matter for `_dq_reasons`? *Key points: NULL;
   `concat_ws` skips NULL and returns `""` for a clean row.*
2. Why must `rule_invalid_quantity` handle NULL separately from `<= 0`? *Key points: `NULL <= 0` is NULL, not true; a filter drops it.*
3. Why does the project read bronze as text and cast later? *Key points: bronze keeps the data as it arrived; typing happens in
   `cast_types`; bad values become NULL instead of stopping the job.*
4. Why does `try_cast` matter under ANSI mode? *Key points: plain `cast` raises an error and stops the job.*
5. Why does each test row need to be valid except for the rule under test? *Key points: other rules would fire; the assertion
   would fail for an unrelated reason.*
6. What does a passing test prove, and what does it not prove? *Key points: it proves the rule on those rows; it does not prove
   the whole table is clean.*
7. Why use `|` and parentheses instead of `or`? *Key points: Column objects are not Python booleans; `|` builds a new expression.*
8. When is a test "not skipped"? *Key points: no `pytest.skip`, and the test body runs its assertions.*

## 9. Interview link

- "Why does the project read bronze as strings and use `try_cast` under ANSI mode?" [PROJECT] Mid
- "How do NULLs affect comparisons, `NOT IN` and aggregates?" [GENERAL] Stretch
- "What are the main data quality dimensions, and how does the project address them?" [PROJECT] Junior
- "How do pytest fixtures work, and how does the project use one for Spark?" [PROJECT] Junior

Sentence for the learner: "Each quality rule is a column expression that returns a reason or NULL. Bad rows go to quarantine with
their reasons, and each rule has a test with a bad row and a good row."

## 10. Wrap-up

- Tick TODO-02 and TODO-03 in `docs/TODO.md`, and the matching checkboxes in `README.md` "My implementations", only after both
  DONE WHEN checks passed.
- Commit (the learner runs it): `git add -A && git commit -m "feat(todo-02): remaining quality rules and rule tests"`. Use a
  separate commit for TODO-03 if they prefer: `feat(todo-03): tests for quality rules`.
- Next stage: `docs/tutor/stage-03-dedup.md` (TODO-04 and TODO-05).

**Say it like this (RU):**

> "Column — это не число, а инструкция: «для каждой строки возьми quantity и проверь». Число получаем, когда Spark
> применит инструкцию к строке."

> "NULL — это пустая графа в анкете. На вопрос «больше ли 2?» ответа нет, поэтому строка не проходит ни в один из
> фильтров. Значит, пустоту надо проверять отдельно."

> "Тест — это чек-лист на каждое изменение правила. Нужны две строки: одна плохая, одна хорошая. Если тест проходит только на
> хорошей, он ничего не доказывает."
