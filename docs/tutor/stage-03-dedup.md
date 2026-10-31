# Stage 03: Dedup to the latest event (TODO-04, TODO-05)

> Tutor: read `docs/tutor/README.md` first. Explain in Russian; keep code in English. The learner writes `dedup_latest` in
> `src/shipments_pipeline/silver.py` and one test in `tests/test_silver.py`. This stage is about **window functions** and the
> **shuffle** they cause.

## 1. Goal in one sentence

The learner implements `dedup_latest` so that silver keeps exactly one row per shipment, the one with the latest event, and
writes a three-row test that proves the newest event wins.

**Interview afterwards:** "I number the events of each shipment with `row_number` over a window ordered by time, with `event_id`
as a tie-break, and keep number 1. The window needs a shuffle, so I know where the cost is."

## 2. Before you start

- Stage 02 done (ideally). TODO-04 works without TODO-02, but the silver count of 1,200 needs TODO-02 too (see section 4).
- Open together: `src/shipments_pipeline/silver.py` (the TODO-04 block and `unprocessed_bronze`), `quality.py`
  (`split_valid_quarantine`, for the DataFrame style), `tests/test_silver.py`, `docs/CONCEPTS.md` sections "Narrow vs wide
  transformations" and "Shuffle".
- Ask one warm-up question: "What does a DataFrame method return: rows, or a new DataFrame?" (A new DataFrame, lazy.)

## 3. Concepts to teach first

### 3.1 Window function: a folder per key

**Plain explanation.** `Window.partitionBy("shipment_id")` splits rows into groups by shipment. `orderBy(...)` sets the order
inside each group. A window function such as `F.row_number()` then gives each row a number inside its group. The rows are not
merged; each keeps its own row and gains a column.

**Analogy.** One folder per shipment. Inside each folder, papers are numbered from newest to oldest. Number 1 is the latest paper.

**Tiny example** (a toy, not the silver data). Predict the numbers first.
```python
from pyspark.sql import Window, functions as F
toy = spark.createDataFrame(
    [("SHP1", "2026-10-01 08:00:00", "E1", "IN_TRANSIT"),
     ("SHP1", "2026-10-01 20:00:00", "E2", "DELIVERED"),
     ("SHP2", "2026-10-01 09:00:00", "E3", "DELAYED")],
    "shipment_id string, event_ts string, event_id string, status string")
w = Window.partitionBy("shipment_id").orderBy(F.col("event_ts").desc(), F.col("event_id").desc())
toy.withColumn("rn", F.row_number().over(w)).show()
```
Expected: E2 gets `rn` 1 and E1 gets `rn` 2 (both SHP1); E3 gets `rn` 1 (SHP2). Note: the toy uses strings to stay short. The real
pipeline uses a TIMESTAMP column from `quality.cast_types`.

**Important.** Do not add a filter to the toy. The filter is the learner's job.

**Where in this repo.** `silver.dedup_latest` (TODO-04). The reference style is `quality.split_valid_quarantine`.

### 3.2 Why the tie-break matters

**Plain explanation.** Two events can have the same `event_ts`. Without a second sort key, the winner can change from run to run.
`event_id` as a second key makes the order deterministic.

**Analogy.** Two letters postmarked at the same minute. You need a second rule, such as the sequence number on the envelope.

**Tiny example.** Change two rows in the toy to the same `event_ts` and run `.show()` twice, then add `event_id` to the order.
Ask: which row is number 1 when `event_id` is not used? (The answer is "not guaranteed".)

### 3.3 Wide transformation and shuffle

**Plain explanation.** A window with `partitionBy` must bring all rows with the same key together. Spark moves rows between machines
to do this. That movement is a shuffle. In a plan it appears as `Exchange`.

**Analogy.** Sorting mail by postcode across several post offices: every letter for one postcode must reach the same office.

**Tiny example** (plan text is expected to vary on serverless, not verified by the scaffold author):
```python
toy.withColumn("rn", F.row_number().over(w)).explain()
```
Look for a window step and an `Exchange` step above the scan.

**Pitfall to explain.** `Window.orderBy(...)` with no `partitionBy` puts all rows in one partition. It is correct but slow, and
Spark prints a warning about it.

### 3.4 Filtering on a row number is a two-step job

**Plain explanation.** A window expression cannot be used directly in `filter` for this purpose. First add the number as a column,
then filter on that column. This is a common Spark pattern.

**Where in this repo.** The reference `split_valid_quarantine` shows the same two-step idea with `_dq_reasons`.

## 4. Step-by-step plan

1. **Read `split_valid_quarantine`** in `quality.py`.
   *Check:* the learner names the input, the two outputs, and the column dropped from the valid side.
2. **Run the toy in 3.1** and predict the `rn` column.
   *Check:* the learner predicts correctly. If not, explain `orderBy(...desc())` in one sentence.
3. **Add the tie-break** (3.2) to the toy, using two rows with equal timestamps.
   *Check:* the learner can say why the second key is needed.
4. **Write `dedup_latest`** in `silver.py`. Build the window, add the number, keep row 1, drop the helper column.
   *Check:* calling `silver.dedup_latest(df)` in a scratch cell no longer raises `NotImplementedError`.
5. **Run `20_silver`** (`USE_MERGE = False`). Expected counts depend on TODO-02:
   - With TODO-02 done: `SILVER rows` 1,200 (silver has 1,215 before dedup).
   - With TODO-04 only (TODO-02 not done): 1,233 rows. This is derived from `docs/SCAFFOLD_SPEC.md` section 1, not verified by running.
   *Check:* `count(distinct shipment_id) = count(*)` in a scratch SQL cell.
6. **Check `SHP000002`, not only `SHP000001`.** The DONE WHEN check for `SHP000001` is weak: its original status is also DELIVERED,
   so it passes with or without dedup. `SHP000002` has base status DELAYED and a newer duplicate event with status DELIVERED.
   *Check:* `SHP000002` appears once, with status DELIVERED.
7. **Write `test_dedup_latest_keeps_newest_event`** in `tests/test_silver.py` (remove the skip).
   *Check:* `pytest -q tests/test_silver.py -rs` shows it passed.

## 5. Hint ladder per TODO

### TODO-04: dedup latest event

| Level | What you give |
|---|---|
| 0 Goal | "One row per shipment: its newest event. Ties are broken by the larger `event_id`." |
| 1 Concept | "Put each shipment's events in its own folder, number them newest first, and keep paper number 1." |
| 2 Tool | "`Window.partitionBy(...).orderBy(...)`, `F.col(...).desc()`, `F.row_number().over(window)`. A filter on the new column comes in a second step." |
| 3 Skeleton | `w = Window.partitionBy(___).orderBy(___, ___)` / `numbered = df.withColumn(___, F.row_number().over(w))` / `return numbered.filter(___).drop(___)`. Ask: which value of the row number keeps one row per shipment? Which two columns, in which direction? |
| 4 Solution | Described only: "a window per shipment ordered by time then id, a row number, keep row one, drop the helper column." The learner writes it and explains each line. |

Variation after level 4 (only if they asked for it): "Now keep the oldest event instead. Which two things change?"

### TODO-05: test dedup

| Level | What you give |
|---|---|
| 0 Goal | "Three events, two shipments. The test proves that the newer event for shipment A wins, and that shipment B stays." |
| 1 Concept | "The test is a tiny story: two versions of one shipment and one other shipment. Assert the winner and the row count." |
| 2 Tool | "`spark.createDataFrame(rows, schema=...)` with a timestamp column, `silver.dedup_latest(df).collect()`, dictionary of shipment to status. Use `datetime` for timestamps." |
| 3 Skeleton | `def test_dedup_latest_keeps_newest_event(spark):` / `rows = [ ___ , ___ , ___ ]  # A old, A new, B` / `df = spark.createDataFrame(rows, schema=___)` / `result = silver.dedup_latest(df).collect()` / `assert len(result) == ___` / `assert {___} == {___}` |
| 4 Solution | Described only: "build three rows, run the function, assert on the count and on the surviving status." |

Remember: the test needs `TODO-04` done first, or it raises `NotImplementedError`.

## 6. Common mistakes and error messages

| Symptom | Cause | What to ask or suggest |
|---|---|---|
| Silver has the same shipment twice | The window is missing, or dedup is not applied. `20_silver` prints `WARNING: TODO-04 not done yet - skipping dedup.` | "Is that warning still printed? Then the function still raises." |
| The wrong status wins | `orderBy` is ascending, so the oldest event gets number 1. | "Which direction did you sort? What does `.desc()` do?" |
| Results change between runs | No tie-break on `event_id`. | "What happens when two events have the same `event_ts`?" |
| `AnalysisException: [UNRESOLVED_COLUMN...]` naming `rn` or the helper column | The column is used in a later step but was dropped too early. | "Which step created the column? Which step dropped it?" |
| Error about a window expression inside `filter` (exact wording varies) | Filter applied directly on the window expression. | "Add the row number as a column first, then filter on that column." |
| Warning `WindowExec: No Partition Defined for Window operation! Moving all data to a single partition` (expected wording) | `orderBy` without `partitionBy`. | "Which key should the window be split by?" |
| Test error about `NotImplementedError: TODO-04` | The function is not written yet. | Do TODO-04 first. |
| Test fails with a timestamp error | The test passes strings, and the function compares them in an unexpected order, or the column type does not match the function. | "Use `datetime` objects, as the pipeline's TIMESTAMP column does." |
| Silver row count is 1,233 and the learner expects 1,200 | TODO-02 is not done, so 33 other bad rows still pass. | Explain the count and point to stage 02. |
| The `SHP000001` check passes, but dedup seems broken | The check is weak (see step 6). | Check `SHP000002` instead. |

## 7. DONE WHEN

From `docs/TODO.md`, copied exactly:

TODO-04 (copied exactly):

> (with TODO-02 done) silver 1,200 rows, `count(distinct shipment_id) = count(*)`; SHP000002 status DELIVERED (its older day-1 event was DELAYED)

TODO-05 (copied exactly):

> test passes

Note for the tutor: the silver counts are a Databricks check (expected, not verified by the scaffold author). The test is local.
Also use `SHP000002` as an extra check, as described in step 6.

## 8. Check-your-understanding questions

1. Why is `row_number` used instead of `DISTINCT`? *Key points: `DISTINCT` compares whole rows; a window can keep one row by key
   and a chosen order.*
2. What does `partitionBy` decide, and what does `orderBy` decide? *Key points: partitionBy = which rows are compared; orderBy =
   which one is first.*
3. Why does the tie-break exist? *Key points: equal timestamps make the winner unstable without a second key.*
4. Where does the shuffle come from in this function, and where would you see it? *Key points: the window partition by
   shipment_id; `Exchange` in `explain()`.*
5. Why can't you put the window expression directly in `filter`? *Key points: the number must exist as a column first; two steps.*
6. What does the test prove that the notebook count does not? *Key points: the exact surviving row, on a small known input.*
7. Why does `SHP000001` make a weak check? *Key points: its original status is DELIVERED too, so both outcomes look the same.*

## 9. Interview link

- "How does silver deduplicate to the latest event, and why does the tie-break matter?" [PROJECT] Mid
- "How do you deduplicate with `ROW_NUMBER`, and how does that differ from `DISTINCT`?" [PROJECT] Mid
- "What are window functions, and how do `PARTITION BY` and `ORDER BY` shape them?" [GENERAL] Mid
- "What is a shuffle, and why is it expensive?" [GENERAL] Mid

Sentence for the learner: "I keep the latest event per shipment with a window ordered by event time and a deterministic
tie-break, and the window is a shuffle, which I can see in the plan."

## 10. Wrap-up

- Tick TODO-04 and TODO-05 in `docs/TODO.md` and `README.md` "My implementations", after their DONE WHEN checks passed.
- Commit (the learner runs it): `git add -A && git commit -m "feat(todo-04): dedup latest event per shipment"`, then
  `git commit -m "feat(todo-05): test dedup keeps newest event"` if they commit separately. Check `git status` first.
- Next stage: `docs/tutor/stage-04-gold-sql.md` (TODO-06).

**Say it like this (RU):**

> "Window — это папка на каждую посылку. Внутри папки бумаги нумеруются от новой к старой, и нам нужна бумага номер один."

> "Tie-break — это второе правило на случай равенства. Если два события пришли в одну секунду, без второго правила победитель
> может меняться от запуска к запуску."

> "Shuffle — это когда письма для одного почтового индекса надо собрать в одном отделении. Письма едут между отделениями, и это
> дорого. Найти его можно в плане: ищем Exchange."
