# Stage 08: Schema enforcement and schema evolution (TODO-10)

Tutor note: the learner is an absolute beginner. Explain in Russian. Keep code, identifiers and commands in English.

## 1. Goal

Make day 3 load cleanly. Day 3 adds a new column `carrier`. The learner first sees Delta refuse the write, then allows the new column on purpose, in three places: the bronze append, the quarantine append, and the silver MERGE.

**Interview sentence the learner should be able to say:** "Delta enforces the table schema by default, so a surprise column fails the write. I allow evolution on the specific write that needs it, per statement, not as a session-wide switch, and I check that old rows show NULL for the new column."

## 2. Before you start

- Stages 00-07 are done. TODO-07 matters most: without it, re-running `10_bronze` appends day 1 and day 2 again, and the counts stop matching.
- TODO-08 (`merge_into_silver` in `silver.py`) is done, and `USE_MERGE = True` is set in `20_silver`. Otherwise the silver MERGE part does not run.
- Days 1 and 2 are loaded: `00_generate_batches` with `days=1`, then `10` and `20`; then `days=2`, then `10` and `20`.
- Files to open: `notebooks/10_bronze.py` (TODO-10 comment block and the append), `notebooks/20_silver.py` (the quarantine append line), `src/shipments_pipeline/silver.py` (`merge_into_silver`, and the TODO-10 comment at the bottom), `docs/TODO.md#todo-10`.
- Ask first: "Are days 1 and 2 loaded now? Is `USE_MERGE` True?"

## 3. Concepts to teach first

### 3.1 A schema is the list of columns and their types

- **Plain explanation:** the schema is the header of the table: column names and types, such as `shipment_id` (text) and `quantity` (integer).
- **Analogy:** a paper form with printed boxes. A filled form with an extra box that is not on the template gets rejected at the counter.
- **Where in this repo:** the raw CSV columns are in `generator.COLUMNS`; day 3 adds `carrier` in `generator.COLUMNS_DAY3`. Bronze keeps every column as text (`bronze.read_batch`).
- **Tiny example (scratch cell):**
  ```python
  df = spark.createDataFrame([(1, "a")], "id int, label string")
  df.printSchema()
  ```
  Ask: what would you expect `printSchema` to show for a third column named `carrier`?

### 3.2 Schema enforcement

- **Plain explanation:** when you append to a Delta table, Delta compares the incoming columns with the table schema. A mismatch stops the write. Nothing is half-written, because the commit fails as a whole.
- **Analogy:** a strict bank teller who only accepts deposits in the form the bank prints. The protection is for the people who read the account later.
- **Where in this repo:** `10_bronze` section 3 appends each file to `bronze_shipments` with `mode("append")`, and there is no `mergeSchema` yet. On day 3 this is the first error.
- **Tiny example (throwaway table, expected to fail on purpose):**
  ```python
  spark.createDataFrame([(1, "a")], "id int, label string") \
       .write.mode("overwrite").saveAsTable("workspace.shipments.tmp_demo")
  spark.createDataFrame([(2, "b", "x")], "id int, label string, carrier string") \
       .write.mode("append").saveAsTable("workspace.shipments.tmp_demo")
  ```
  Expected: the second cell fails. Read the error and find the one line that says the schema does not match. Then drop the demo table: `spark.sql("DROP TABLE IF EXISTS workspace.shipments.tmp_demo")`.

### 3.3 Schema evolution, opted in per write

- **Plain explanation:** evolution means you tell Delta, for one specific write, "accept the new column". The table schema then grows. Old rows get NULL in the new column.
- **Analogy:** adding a new box to the form for one batch of forms, with a note on the batch. Not printing a new template for every form that ever comes in.
- **Where in this repo:** `docs/CONCEPTS.md#schema-enforcement-vs-evolution` (reference), and the TODO-10 comment in `10_bronze`. The option is named in the TODO-10 WHAT and HINTS, so look there. Read the Delta docs page for it.
- **Note:** a session-wide setting exists (`spark.databricks.delta.schema.autoMerge.enabled`). The TODO says to prefer the per-write form, because a session setting also allows changes you did not intend.

### 3.4 MERGE with schema evolution, `UPDATE SET *` and `INSERT *`

- **Plain explanation:** `UPDATE SET *` means "copy every column of the source row that has a matching name". `INSERT *` does the same for new rows. A MERGE can allow schema evolution for its own statement with the phrase `WITH SCHEMA EVOLUTION`.
- **Analogy:** a photocopier set to copy the whole page, not just the boxes you typed. `*` is the "whole page" setting. Evolution is the setting that lets the copier add a new box to the target form.
- **Where in this repo:** `silver.merge_into_silver` (your TODO-08 code), `dims.upsert_dim_supplier` (the SCD1 MERGE reference, with clause comments).
- **Note:** `*` copies every source column with a matching name, including `_source_file`, `_ingested_at` and `carrier`. Check which columns the target has before you rely on it.

### 3.5 Old rows get NULL for a new column

- **Plain explanation:** a column that did not exist when a row was written has no value for that row, so the value is NULL.
- **Analogy:** a new column added to an old spreadsheet is empty for the rows typed last year.
- **Where in this repo:** the day-3 DONE WHEN says `carrier` must be NULL for older rows. `quality.cast_types` leaves `carrier` untouched, so it is text and passes through.

### Say it like this (RU)

- "Delta — как строгий кассир: новую колонку без разрешения он не примет. Но если мы сами решили её добавить, мы говорим это явно, для одной операции, а не отключаем проверку на всю сессию."
- "`UPDATE SET *` — это ксерокс, который копирует всю страницу. Поэтому сначала проверяем, какие колонки есть в целевой таблице."
- "Старые строки получают NULL в новой колонке — как в старой таблице, где новый столбец пустой для записей прошлого года. Это правильно, а не ошибка."

## 4. Step-by-step plan

1. **See the error on purpose.** Run `00_generate_batches` with `days=3`, then `10_bronze` with the TODO-10 code still unchanged. Expected: the append stops with a schema mismatch.
   - check: the learner can point to the line of the error that names the mismatch, and says that the new column is `carrier`. Expected wording is "schema mismatch" plus a hint about `mergeSchema`; the exact text may differ on your runtime (expected, not verified).
2. **Explain why this is correct behaviour.** Ask: "If the table accepted any new column silently, what could a downstream report do with it?"
   - check: learner says that readers could break or misread the data, so the decision must be explicit.
3. **Allow evolution for the bronze append.** Change only that one write, in `10_bronze`.
   - check: `10_bronze` runs, and `SELECT COUNT(*) FROM workspace.shipments.bronze_shipments` is 1,665 (after TODO-07).
4. **Quarantine append (only if `USE_MERGE = True`).** Run `20_silver` and look at the quarantine line in the MERGE branch. The day-3 quarantine DataFrame is empty but still has the `carrier` column in its schema, so the append needs mergeSchema (expected behaviour, not verified).
   - check: `20_silver` runs without a schema error on the quarantine write. If it fails, read the error and go to the quarantine line.
5. **Make the silver MERGE evolve.** Change the MERGE statement in `merge_into_silver` to the per-statement form and use `UPDATE SET *` and `INSERT *`. Keep the `s.event_ts > t.event_ts` guard.
   - check: before the change, the MERGE on day 3 fails with an error about a column the target does not have (expected wording varies, not verified). After the change it runs.
6. **Run the DONE WHEN path.** Start from a clean state for the scenario: `00` with `days=3` (after days 1 and 2), then `10`, then `20`.
   - check: bronze 1,665, silver 1,350, and `DESCRIBE TABLE workspace.shipments.silver_shipments` lists `carrier`.
7. **Check the NULLs.** Query old and new rows.
   - check: before running it, the learner works out the expected split. Every day-3 row has a carrier, and day 3 touches 100 shipments (50 updates and 50 new). So after the day-3 MERGE, `carrier IS NOT NULL` should give 100 and `carrier IS NULL` should give 1,250 (1,350 silver rows minus those 100). If the numbers differ, ask which rows were updated and why.

## 5. Hint ladder per TODO

### TODO-10 (day-3 `carrier` column: bronze, quarantine, silver MERGE)

- **Level 0 (goal):** Day 3 loads without errors. Bronze and silver contain `carrier`. Older rows show NULL. Nothing else changes.
- **Level 1 (concept):** Delta is strict by default. You allow the new column explicitly, for the one write that needs it, not for the whole session.
- **Level 2 (tool):** a `mergeSchema` option on the DataFrame writer for the appends (bronze, quarantine). For the MERGE, the statement starts with `MERGE WITH SCHEMA EVOLUTION INTO`, and the clauses use `UPDATE SET *` and `INSERT *`. Look at the MERGE and schema evolution docs linked in TODO-10.
- **Level 3 (skeleton with blanks):**
  ```
  bronze:    df.write.mode("append").option("___", "___").saveAsTable(___)
  quarantine: the same pattern, with the quarantine table name
  silver:    MERGE WITH SCHEMA EVOLUTION INTO ___ AS t
             USING ___ AS s
             ON ___
             WHEN MATCHED ___ THEN UPDATE SET ___
             WHEN NOT MATCHED THEN INSERT ___
  ```
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "allow the new column per write, on the three writes that need it, keep the timestamp guard, and never turn on a session-wide setting". Then walk through your own code line by line and retype the fixed lines yourself. Afterwards give a variation: "What changes if day 3 also added a column that is a number, not text?"

## 6. Common mistakes and error messages

| Symptom | Likely cause | What to ask or suggest |
|---|---|---|
| Day 3 append fails with a schema mismatch | Expected. This is the starting point, not a bug in the data. | "What does Delta say is different? Which column is new?" |
| The fix works in bronze, then `20_silver` fails on the quarantine write | The quarantine append also needs the option (only in the `USE_MERGE = True` branch) | "Which table is the failing write for? Did you add the option there too?" |
| The MERGE fails about a column that does not exist in the target | `UPDATE SET *` without the `WITH SCHEMA EVOLUTION` phrase | "Is the phrase in the statement? Is it before `INTO`?" (exact wording varies, expected, not verified) |
| Counts jump: bronze is well above 1,665 after day 3 | TODO-07 is not done, so every file is re-appended on each run | "Does `10_bronze` still print the fallback warning? Then TODO-07 is not done." |
| Silver shows `carrier` right away with no MERGE change | `USE_MERGE = False` runs the full refresh path, which overwrites silver with `overwriteSchema` | "Which path ran? The MERGE path is the one this TODO is about. Set `USE_MERGE = True` to test it." |
| Old rows have a carrier value they should not have | `UPDATE SET *` copied a value from a later event into an earlier row, or the guard was removed | "Does the MERGE still have `s.event_ts > t.event_ts`?" |
| Someone reaches for `overwriteSchema` on the bronze append | Wrong tool: bronze must keep history, and `overwriteSchema` belongs to overwrite writes | "Are you appending or overwriting? Bronze is append-only." |
| Session-wide setting makes everything pass | It hides which write changed the schema | Suggest reverting to the per-write option and explaining the difference. |

## 7. DONE WHEN

Copied from `docs/TODO.md`:

> **DONE WHEN:** 00 days=3 → 10 → 20: bronze 1,665, silver 1,350, `carrier` column exists, NULL for older rows

Tutor check: run the three notebooks in that order (after days 1 and 2 are loaded and TODO-07 and TODO-08 are done), then the count queries from steps 3, 6 and 7.

Note: Databricks-only check. The scaffold author has not verified schema evolution on Free Edition.

## 8. Check-your-understanding questions

1. What is a schema, in one sentence? Good answer: the names and types of the columns of a table.
2. Why does Delta reject a day-3 append by default? Good answer: enforcement protects readers from unexpected columns, and the decision to accept a column should be explicit.
3. Why is a per-write option better than a session-wide setting? Good answer: it changes only the write you meant to change.
4. What does `UPDATE SET *` copy, and why check the target columns first? Good answer: every source column with a matching name, so unexpected columns come along.
5. Why does `carrier` show NULL for older rows? Good answer: those rows were written before the column existed.
6. Why must the `s.event_ts > t.event_ts` guard stay in the MERGE? Good answer: an old event replayed later must not overwrite a newer status.
7. Why does TODO-07 matter for this stage? Good answer: without it, re-running files doubles bronze and the expected counts fail.

## 9. Interview link

From `docs/INTERVIEW_PREP.md`:

- **Q:** "What is the difference between schema enforcement and schema evolution?" [GENERAL] Junior
- **Q:** "What is a data contract, and why does it matter?" [GENERAL] Mid
- **Q:** "What does idempotency mean, and why does it matter for pipelines?" [PROJECT] Junior

One sentence the learner can use: "When day 3 added a column, Delta refused the write by default. I allowed it per write, on bronze, quarantine and the silver MERGE, and checked that old rows show NULL."

## 10. Wrap-up

- Tick `- [ ] Done` under `TODO-10` in `docs/TODO.md`.
- Tick `TODO-10 Schema evolution` in the README checklist.
- Suggested commit (the learner runs it, after checking `git status`):
  ```
  git add -A && git commit -m "feat(todo-10): schema evolution for day-3 carrier column"
  ```
  Do not push without a deliberate decision.
- **Next stage:** `stage-09-delta-ops.md` (TODO-11 and TODO-12). The learner will look at Delta's transaction log, go back in time, and work with file layout.
