# Stage 12: Stretch (SCD2 dimension, broadcast join, cost awareness; TODO-16, TODO-17, TODO-18)

Tutor note: the learner is an absolute beginner. Explain in Russian. Keep code, identifiers and commands in English. This stage is optional and harder. Do the three TODOs in order, and let the learner stop after any of them.

## 1. Goal

Keep history of supplier changes in a new SCD2 table (TODO-16), compare two join plans and write down why they differ (TODO-17), and look at billing data for the pipeline, or record why it cannot be read on Free Edition (TODO-18).

**Interview sentence the learner should be able to say:** "I kept supplier history with an SCD2 table: the old row gets an end date and a new current row is added, so I can answer what was true on a given date. I compared a broadcast join with a sort-merge join in the plans, and I wrote down what the timings do and do not prove."

## 2. Before you start

- Stages 00-11 are done. The core TODOs are in place, and `workspace.shipments.silver_shipments` and `workspace.shipments.dim_supplier` exist.
- `dim_supplier` is the SCD1 table from the reference (`dims.upsert_dim_supplier`). It has no `valid_from`, `valid_to` or `is_current` columns. That is why TODO-16 writes to a new table.
- Files to open: `src/shipments_pipeline/dims.py` (`upsert_dim_supplier` and the TODO-16 stub), `notebooks/20_silver.py` (the TODO-16 cell), `notebooks/30_gold.py` (the TODO-17 cell), `notebooks/60_monitoring.py` (the TODO-18 cell), `docs/CONCEPTS.md` (broadcast join and AQE sections), `docs/TODO.md#todo-16`, `docs/TODO.md#todo-17`, `docs/TODO.md#todo-18`.
- Ask first: "Which of the core TODOs are done? Do you want all three stretch TODOs, or one at a time?"

## 3. Concepts to teach first

### 3.1 SCD type 1 versus type 2

- **Plain explanation:** a slowly changing dimension is a table of things that change over time, here suppliers. Type 1 overwrites the old value, so history is lost. Type 2 keeps every version as its own row with `valid_from`, `valid_to` and `is_current`.
- **Analogy:** an address book (type 1) versus a passport with a history of stamps and addresses (type 2). The passport keeps the old page and adds a new one.
- **Where in this repo:** `dims.upsert_dim_supplier` is SCD1 (the reference MERGE). TODO-16 adds SCD2 into `dim_supplier_scd2`.
- **Tiny example (toy data, filter only):**
  ```python
  demo = spark.createDataFrame(
      [("SUP001", 2, False), ("SUP001", 5, True)],
      "supplier_id string, rating int, is_current boolean",
  )
  demo.filter("is_current").show()
  ```
  Ask: why does the filter give one row, and what would a report show on a date before the change?

### 3.2 Closing a row and inserting a new one

- **Plain explanation:** when a supplier changes, two things happen. The current row gets an end date and `is_current` becomes false. A new row is added with the new values, a new start date and `is_current` true.
- **Analogy:** a lease on a flat. The old lease gets an end date. The new lease starts the next day.
- **Why it is tricky:** a changed supplier needs two actions: close the old current row and insert the new current row. Ask how a single MERGE could do both. Two statements (an UPDATE and an INSERT) are also an acceptable design, if you can keep them consistent.
- **Trap:** if you run the same version twice, rows must not be added again. The MERGE must see "no change" the second time.

### 3.3 Counting SCD2 rows by hand

Use the scaffold's data contract (`docs/SCAFFOLD_SPEC.md` section 1):

- After v1: 24 suppliers, all current, so 24 rows.
- v2 changes the rating of SUP001, SUP002 and SUP003, and adds SUP025. Each changed supplier gets one expired row and one new current row: 3 expired rows and 3 new current rows. SUP025 adds one current row.
- Total: 24 + 3 + 1 = 28 rows. Current rows: 24 + 1 = 25. Expired rows: 3. SUP001 has 2 rows.

Ask the learner to derive these numbers before running anything.

### 3.4 Join strategies: broadcast versus sort-merge

- **Plain explanation:** a join matches rows on a key. A **broadcast hash join** copies the small table to every executor, so the big table does not move. A **sort-merge join** shuffles both tables by the key, so matching rows meet.
- **Analogy:** a small price list copied to every shop (broadcast), versus sending every receipt and every price list to a central sorting office (sort-merge).
- **Where in this repo:** `30_gold` TODO-17 cell (silver joined with `dim_supplier`, no join exists in the reference yet). The DataFrame broadcast hint is `F.broadcast(df)`.
- **Tiny example (toy data, scratch cell):**
  ```python
  from pyspark.sql import functions as F
  big = spark.range(100000).withColumnRenamed("id", "k")
  small = spark.createDataFrame([(1, "a"), (2, "b")], "k long, label string")
  big.join(F.broadcast(small), "k").explain()
  ```
  Look for `BroadcastHashJoin` or `BroadcastExchange`. Then remove `F.broadcast` and look again. Ask: what changed in the plan?

### 3.5 Adaptive Query Execution (AQE) and skew

- **Plain explanation:** AQE lets Spark change the plan while the job runs, using the real sizes of the shuffle output. **Skew** is when one key holds far more rows than the others, so one task does most of the work.
- **Analogy:** a checkout that opens a second lane when one queue gets too long.
- **Where in this repo:** there is no real skew. `supplier_id` is `i % 24` (see `generator._shipment`), so suppliers are spread evenly. AQE on serverless is expected, not verified. Try reading the setting with `spark.conf.get`. It may fail on serverless because the config is not available there; if so, record the error in `docs/NOTES.md` and move on. Do not try to change it.

### 3.6 Cost data: `system.billing.usage`

- **Plain explanation:** `system.billing.usage` is a Databricks system table that lists usage per SKU (a billable product line) and per day, in DBUs (Databricks units).
- **Analogy:** the electricity bill with a line for each appliance.
- **Where in this repo:** `60_monitoring` section 3 (TODO-18 cell). Free Edition may not allow access; expected, not verified.

### Say it like this (RU)

- "SCD2 — это как паспорт с историей адресов: старая запись не стирается, а получает дату окончания, и рядом появляется новая. Поэтому отчёт может ответить, что было на любую дату."
- "Broadcast join — это как раздать каждому магазину маленький прайс-лист, чтобы не гонять по почте всю большую книгу чеков. Но это работает только для маленького прайса."
- "Тайминги одного маленького запуска — не бенчмарк. Честно записываем, что показывают планы, а что нет."

## 4. Step-by-step plan

1. **Derive the SCD2 numbers.** Using section 3.3, write down the expected totals before running anything.
   - check: the learner gets 28, 25 and 3, and SUP001 with 2 rows.
2. **TODO-16: design, then code.** Describe on paper which rows must be closed and which must be inserted. Then write `upsert_dim_supplier_scd2` in `dims.py`, and add the 20_silver cell that runs version 1 and then version 2 into the new table.
   - check: after v1 the table has 24 rows, all current. (In a scratch cell, count `is_current = true` rows.)
3. **TODO-16: run v2 and count.** Run the cell so v2 follows v1.
   - check: `SELECT COUNT(*) FROM workspace.shipments.dim_supplier_scd2` returns 28; `WHERE is_current` returns 25; `WHERE supplier_id = 'SUP001'` returns 2.
4. **TODO-16: rerun for idempotency.** Run the cell again.
   - check: the counts stay at 28 and 25.
5. **TODO-17: read the join with and without the hint.** In `30_gold`, join silver with `dim_supplier`, first without `F.broadcast`, then with it. Call `explain()` for each.
   - check: the learner names the join operator in each plan. The broadcast version should show `BroadcastHashJoin`; the other one may show `SortMergeJoin`, or Spark may choose broadcast anyway (expected, not verified on serverless).
6. **TODO-17: read about AQE.** Try to read the setting with `spark.conf.get` (read only). If it fails on serverless, record the error in `docs/NOTES.md` and move on.
   - check: either the learner can say whether AQE is on, or the error is recorded and the learner can say what the AQE docs say it changes.
7. **TODO-17: write the comparison.** In `docs/NOTES.md`, write a table with: join type, where the shuffle is (or is not), and the reason. Add one sentence on what timing does and does not show.
   - check: the note refers to plan operators, and does not claim a speed-up from a small run.
8. **TODO-18: probe the system table.** Run a single-row probe first to see if it is readable.
   - check: either rows come back, or an error comes back. Both are valid outcomes.
9. **TODO-18: query or record the error.** If the probe works, write a grouped query over the last seven days with a blank to fill. If not, copy the exact error text into `docs/NOTES.md`.
   - check: `docs/NOTES.md` contains either the grouped result or the exact error text.

## 5. Hint ladder per TODO

### TODO-16 (`upsert_dim_supplier_scd2`, a new table, and the v1 then v2 cell)

- **Level 0 (goal):** `dim_supplier_scd2` keeps every version of every supplier. After v1 then v2 it has 28 rows, 25 current, and SUP001 has 2 rows. Running v2 again changes nothing.
- **Level 1 (concept):** A changed supplier needs two actions: close the old version and add the new one. Ask how a single MERGE could do both.
- **Level 2 (tool):** `upsert_dim_supplier` in `dims.py` is the SCD1 pattern (temp view, `MERGE INTO`, `WHEN MATCHED`, `WHEN NOT MATCHED`). Add the three columns `valid_from`, `valid_to`, `is_current`. The first load creates the table, as the reference does. Docs: the MERGE page linked in TODO-16.
- **Level 3 (outline with blanks, no SQL):**
  ```
  first load:  create dim_supplier_scd2 from the source; every row current; valid_to empty
  later loads:
    1. ___          : find the suppliers whose values differ from their current row
    2. ___          : build the MERGE source (think about how each changed supplier can be matched and inserted)
    3. WHEN MATCHED ___  -> close the current row (set ___ and ___)
    4. WHEN NOT MATCHED ___ -> insert the new current row
  ```
  Ask the learner to fill each blank and to say why the second copy must not match.
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "close the changed current row, insert the new current row through a source row that cannot match, and make a second run a no-op". Then write your own MERGE and retype any fixes yourself. Afterwards give a variation: "Add a column `source_file`. Which rows should get the value, and does the count change?"

### TODO-17 (broadcast join versus sort-merge, AQE, a written comparison)

- **Level 0 (goal):** You can point to the join operator in both plans, say why they differ, and write it in `docs/NOTES.md`.
- **Level 1 (concept):** A broadcast join sends the small table to every executor, so the big table does not move. A sort-merge join moves both tables by key. Spark may pick one by itself.
- **Level 2 (tool):** `F.broadcast(df)`, `DataFrame.explain()`, `spark.conf.get(...)` for reading AQE. The links to `broadcast`, `explain` and AQE are in TODO-17.
- **Level 3 (skeleton with blanks):**
  ```python
  # without the hint
  silver_df.join(___, on=___).explain()
  # with the hint
  silver_df.join(F.broadcast(___), on=___).explain()
  # then compare the operator names in the two outputs
  ```
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "compare the two plans operator by operator, say what the hint changed, and say what the timing does not show". Then write the comparison yourself. Afterwards give a variation: "Make the dimension bigger (say, a union of itself) and predict when Spark stops choosing broadcast."

### TODO-18 (`system.billing.usage` for seven days, or the recorded error)

- **Level 0 (goal):** Either a seven-day cost query by SKU works, or the exact error is written down. Both count as done.
- **Level 1 (concept):** A system table holds metadata about your account. Access depends on the workspace and the account, and Free Edition may not allow it.
- **Level 2 (tool):** The billing system table docs in TODO-18, the Free Edition limitations page, and a `try/except` around the query so the error prints instead of stopping the cell.
- **Level 3 (skeleton with blanks):**
  ```sql
  -- probe: is the table readable at all?
  SELECT * FROM system.billing.usage LIMIT 5;

  -- if the probe works: fill the blanks yourself
  SELECT ___, SUM(___) FROM system.billing.usage WHERE ___ GROUP BY ___;
  ```
  Ask the learner which column holds the SKU name and which holds the quantity. Read the table schema with `DESCRIBE` before you guess. Column names are expected, not verified on Free Edition.
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "probe first, group by the SKU name over the last seven days, and if access fails, write the exact message down instead of guessing". Then type your own query.

## 6. Common mistakes and error messages

| Symptom | Likely cause | What to ask or suggest |
|---|---|---|
| `dim_supplier_scd2` has 25 rows, not 28 | The changed suppliers were updated in place, not closed and inserted | "Did the old rating row get closed, and did a new row get added?" |
| 28 rows, but `is_current` counts 28 | The old rows were not closed | "What value does the old row's `is_current` have?" |
| Running v2 again adds 3 rows | The MERGE is not idempotent | "What should the second run see? Is the change check comparing against the current row?" |
| The MERGE says one target row matches multiple source rows (`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`) | The changed supplier appears twice in the source with the same key, and the MERGE needs one match per target row | "Which source rows share the key? Is the key for the insert copy deliberately different?" |
| The plans look identical | The hint was not applied, or Spark chose broadcast anyway | "Did you call the broadcast function on the small side? Compare the operator names." |
| Someone quotes a timing as a benchmark | Small data, one run | "What does that number include? Does it prove the plan is better?" |
| `TABLE_OR_VIEW_NOT_FOUND` for `system.billing.usage` | The system table is not visible in this workspace (expected on some tiers, not verified) | "Record the exact message in `docs/NOTES.md`. That is a valid outcome." |
| A permission error on the system table | Access is restricted to some roles (wording varies, expected, not verified) | "Who is allowed to read it? Record the error." |
| The learner tries to change a Spark setting to force broadcast | Some settings cannot be changed on serverless | "Read the setting only. Note that it cannot be changed here and move on." |

## 7. DONE WHEN

Copied from `docs/TODO.md`:

> **DONE WHEN (TODO-16):** dim_supplier_scd2 after v1 then v2: 28 rows, 25 with is_current=true, SUP001 has 2 rows

> **DONE WHEN (TODO-17):** written comparison in docs/NOTES.md

> **DONE WHEN (TODO-18):** query returns rows OR error documented in docs/NOTES.md

Tutor check: for TODO-16, run the three count queries from step 3 and the idempotency rerun from step 4. For TODO-17, read the note. For TODO-18, read the note and confirm it has either rows or the exact error.

Note: all three are Databricks-only checks. The scaffold author has not verified them on Free Edition.

## 8. Check-your-understanding questions

1. What is the difference between SCD1 and SCD2? Good answer: SCD1 overwrites and loses history; SCD2 keeps each version as a row with validity fields.
2. Why do 28 rows and 25 current rows both hold after v2? Good answer: three suppliers are expired, and three new current rows plus SUP025 are added; 24 + 3 + 1 = 28, and 28 minus 3 expired = 25 current.
3. Why does one MERGE struggle to close and insert the same key? Good answer: each target row matches at most one source row, so the new version needs a source row that does not match the existing key.
4. What does a broadcast join avoid? Good answer: moving the big table across the network for the join.
5. What does AQE change, and why must you read rather than force its settings on serverless? Good answer: it adapts the plan at runtime; some settings are not allowed to change on serverless.
6. Why is `supplier_id` in the reference data not a skew example? Good answer: suppliers are spread evenly by the generator.
7. What does `system.billing.usage` measure, and why might a Free Edition learner not see it? Good answer: usage in DBUs per SKU per day; access depends on the account and tier.

## 9. Interview link

From `docs/INTERVIEW_PREP.md`:

- **Q:** "What is SCD Type 1 versus Type 2?" [PROJECT] Mid
- **Q:** "How do joins work in Spark, and what does a broadcast join do?" [GENERAL] Mid
- **Q:** "What is Adaptive Query Execution (AQE), and what does it do?" [GENERAL] Mid
- **Q:** "What is data skew, and how do you handle it (salting, AQE skew join)?" [GENERAL] Stretch
- **Q:** "What are the limits of Free Edition that matter for this project?" [GENERAL] Stretch

One sentence the learner can use: "The stretch work gave me SCD2 history for suppliers, a written comparison of broadcast and sort-merge joins from the plans, and a billing query or a documented access error, depending on what Free Edition allowed."

## 10. Wrap-up

- Tick `- [ ] Done` under `TODO-16`, `TODO-17` and `TODO-18` in `docs/TODO.md`, and in the README checklist.
- Suggested commits, one per TODO, run by the learner after checking `git status`:
  ```
  git add -A && git commit -m "feat(todo-16): supplier SCD2 dimension with history"
  git add -A && git commit -m "feat(todo-17): broadcast vs sort-merge join comparison"
  git add -A && git commit -m "feat(todo-18): cost query or documented access error"
  ```
  Do not push without a deliberate decision.
- **What next (end of the whole course):**
  1. Rehearse the pitch in `docs/INTERVIEW_PREP.md` section 1.1 out loud, in English, timed at about one minute. Then answer two or three questions from section 3 without notes.
  2. Update your CV bullets honestly. Use only what you ran yourself: the local pytest results, what you saw on Free Edition, and what you wrote as "expected, not verified". Do not claim a result you did not produce, and do not claim Azure deployment, streaming or CI deploys, because this project has none of them.
