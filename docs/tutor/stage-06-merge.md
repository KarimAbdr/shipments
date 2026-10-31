# Stage 06: Silver MERGE upsert (TODO-08)

> Tutor: read `docs/tutor/README.md` first. Explain in Russian; keep SQL and identifiers in English. The learner writes
> `merge_into_silver` in `src/shipments_pipeline/silver.py`, and then flips `USE_MERGE = True` in `notebooks/20_silver.py`.
> The reference MERGE is `dims.upsert_dim_supplier`. Teach the learner to read it line by line, then write their own.

## 1. Goal in one sentence

The learner implements `merge_into_silver` so that day-2 status updates change existing shipments instead of adding duplicates,
the newest status wins, and running the step again changes nothing.

**Interview afterwards:** "I use Delta MERGE as an upsert on `shipment_id`, with a timestamp guard so an older event never
overwrites a newer one, and the load is safe to run again."

## 2. Before you start

- Stages 01 to 05 done. Specifically: TODO-02 (quarantine 45 on day 1), TODO-04 (dedup), TODO-07 (incremental bronze).
  Without TODO-04 the MERGE fails on duplicate source rows (section 6).
- The target numbers need days 1 and 2 loaded: `00 days=2` writes only the day-2 file, and bronze keeps day 1.
- Open together: `src/shipments_pipeline/dims.py` (`upsert_dim_supplier`, the whole function), `src/shipments_pipeline/silver.py`
  (`merge_into_silver` and `unprocessed_bronze`), `notebooks/20_silver.py` (sections 2 and 5), `docs/CONCEPTS.md` section
  "MERGE and upsert".
- Ask one warm-up question: "What is a primary key, and why does a MERGE need one?" (A column that identifies a row; the match needs it.)

## 3. Concepts to teach first

### 3.1 MERGE as a decision per row

**Plain explanation.** `MERGE INTO target USING source ON key` checks each source row against the target. If the key exists, the
`WHEN MATCHED` clause runs. If not, the `WHEN NOT MATCHED` clause runs. A clause can have its own condition with `AND`.

**Analogy.** Updating a contact list. The same phone number: update the address only if your new record is more recent. A new
number: add a contact.

**Where in this repo.** `dims.upsert_dim_supplier`, lines that start with `MERGE INTO`, `WHEN MATCHED AND`, `WHEN NOT MATCHED`. The
comments there name each clause.

### 3.2 The timestamp guard

**Plain explanation.** A MERGE can use `s.event_ts > t.event_ts` in the `WHEN MATCHED` condition. Then a source row updates the
target only when it is newer. Replaying an old file then changes nothing.

**Analogy.** An old letter arriving after a new one. You keep the one with the later date stamp.

**Tiny example** (plain SELECT, not MERGE: the guard is just a comparison). Use real timestamps, as the pipeline does:
```python
from datetime import datetime
tgt = spark.createDataFrame([("SHP150", "IN_TRANSIT", datetime(2026, 10, 1, 0, 2, 30))],
                            "shipment_id string, status string, event_ts timestamp")
src = spark.createDataFrame([("SHP150", "DELIVERED", datetime(2026, 10, 2, 8, 0, 1))],
                            "shipment_id string, status string, event_ts timestamp")
tgt.createOrReplaceTempView("toy_tgt")
src.createOrReplaceTempView("toy_src")
spark.sql("""
  SELECT s.shipment_id, s.event_ts > t.event_ts AS newer_wins
  FROM toy_src AS s JOIN toy_tgt AS t ON s.shipment_id = t.shipment_id
""").show()
```
Expected: `newer_wins` true. Then swap the two timestamps and ask: what would the guard say now? (False: the target keeps its value.)

### 3.3 The target must be a Delta table

**Plain explanation.** A MERGE target is a real Delta table (`workspace.shipments.silver_shipments`). A temporary view can be the
source, but not the target. The repo registers the source DataFrame as a temporary view with `createOrReplaceTempView` and
then names it in the SQL.

**Analogy.** You can read a notice from the wall (source). You cannot stamp the wall as the filing cabinet (target).

### 3.4 First load versus later loads

**Plain explanation.** On the first run the target does not exist, so the source becomes the table with `saveAsTable`. Later runs
use MERGE. `spark.catalog.tableExists(name)` tells you which case you are in.

**Where in this repo.** `dims.upsert_dim_supplier` lines 46 to 48. Note: silver already exists from the reference path, so the
MERGE branch is the one you will see first.

### 3.5 Why the source must have one row per key

**Plain explanation.** Each target row may match at most one source row. Two source rows with the same `shipment_id` and different
values make MERGE unable to decide, and it fails. That is why dedup (TODO-04) runs before MERGE.

**Analogy.** Two new phone numbers for the same contact on one import sheet. The clerk cannot pick one.

**Where in this repo.** `notebooks/20_silver.py` section 4 (dedup) runs before section 5 (write), and `_dedup` returns the unchanged
DataFrame only if TODO-04 is not done.

## 4. Step-by-step plan

1. **Read `dims.upsert_dim_supplier` line by line** (the whole function, about 35 lines).
   *Check:* the learner names the first-load branch, the source view, the `WHEN MATCHED` clause, and the `WHEN NOT MATCHED` clause.
2. **Read `merge_into_silver`'s signature and its TODO-08 block** in `silver.py`. Note the parameter `updates` is a DataFrame and
   `target_table` is a full name.
   *Check:* the learner says what each parameter is and what the function must return (`None`).
3. **Run the toy in 3.2.** Predict `newer_wins`, then swap the timestamps and predict again.
   *Check:* the learner can explain both results with the word "newer".
4. **Write the first-load branch** (table missing: write the source as the table, then return).
   *Check:* the learner can say when this branch runs. Test it when a fresh table name is used (optional).
5. **Write the MERGE branch.** Register a temp view with a name of your choice, then run `spark.sql` with a `MERGE INTO`, a
   `WHEN MATCHED AND` guard, and a `WHEN NOT MATCHED` insert. Columns: the same list as the reference.
   *Check:* in a scratch cell, `spark.table(cfg.table(config.SILVER)).count()` still returns 1,200 when you call the function with
   the same day-1 data (no change expected on a rerun).
6. **Load day 2 through the MERGE path.** Start from the day-1 state (silver and quarantine from the reference run, bronze
   1,260). Set `USE_MERGE = True` in `20_silver`. Then run `00_generate_batches` with `days = 2`, `10_bronze`, and `20_silver`.
   *Check:* the `20_silver` output shows silver 1,300 and quarantine 50.

   Note: do not run `20_silver` with `USE_MERGE = False` in between. The reference path rebuilds silver from all of bronze, so it
   would hide whether the MERGE works. With `USE_MERGE = True`, `20_silver` processes only the new file (`unprocessed_bronze`).
7. **Check the day-2 update.** In a scratch SQL cell, query `SHP000150`.
   *Check:* status `DELIVERED`. Its original status was IN_TRANSIT.
8. **Rerun `20_silver` with `USE_MERGE = True`, and do not load a new file.**
   *Check:* silver stays 1,300, quarantine stays 50, `SHP000150` stays DELIVERED. The rerun changes nothing.

## 5. Hint ladder per TODO

### TODO-08: silver MERGE upsert

| Level | What you give |
|---|---|
| 0 Goal | "Silver has one row per shipment. Day-2 updates replace an older status. A rerun changes nothing. Expected: silver 1,300, quarantine 50, SHP000150 DELIVERED." |
| 1 Concept | "For each incoming row: is the shipment already in silver? If yes, replace it only when the incoming event is newer. If no, add it. The first run has no table, so create it." |
| 2 Tool | "`spark.catalog.tableExists(name)`, `DataFrame.write.saveAsTable(name)`, `DataFrame.createOrReplaceTempView(name)`, `spark.sql(...)` with `MERGE INTO`, `WHEN MATCHED AND`, `WHEN NOT MATCHED THEN INSERT`. The reference is `dims.upsert_dim_supplier`. Docs link in TODO-08." |
| 3 Skeleton | Prose question first: "What should happen if the target table does not exist yet? Look at how `dims.upsert_dim_supplier` handles it." Then the MERGE part as a skeleton: `___.createOrReplaceTempView("___")` / `spark.sql(f"""` / `  MERGE INTO {___} AS t` / `  USING ___ AS s` / `  ON t.___ = s.___` / `  WHEN MATCHED AND ___ THEN UPDATE SET ___` / `  WHEN NOT MATCHED THEN INSERT ___` / `""")`. Ask the learner to fill each blank from the reference, and to decide the guard and the column list. |
| 4 Solution | Described only: "the first-load branch as in the reference; the MERGE matches on shipment_id, updates only when the source event time is later, and inserts unmatched shipments." The learner writes the code and explains every clause. |

Variation after level 4 (only if asked): "Replay day 1 with the guard removed. Which shipments change, and why does the guard stop it?"

## 6. Common mistakes and error messages

| Symptom | Cause | What to ask or suggest |
|---|---|---|
| `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` (expected class name; message wording varies by runtime) | Two source rows match one target row, usually because dedup (TODO-04) was skipped. | "Does the batch have two rows for one shipment? Is the `TODO-04 not done yet` warning printed?" |
| Error about the MERGE target not being a Delta table | A temp view was used as the target. | "Which name is the target? A temp view can only be the source." |
| `[TABLE_OR_VIEW_NOT_FOUND]` for the temp view | The view name in `createOrReplaceTempView` and in `USING` differ. | "Compare the two names character by character." |
| Older status overwrites a newer one after a replay | No timestamp guard in `WHEN MATCHED`. | "What should win when the incoming event is older? Where does that rule go?" |
| Rerun changes values, or silver grows | The guard is `>=` (ties rewrite the same values), or the file filter is missing, so old files are processed again. | "Which operator did you use? Is `USE_MERGE = True` before the rerun?" |
| `NotImplementedError: TODO-08: see docs/TODO.md#todo-08` from `20_silver` | `USE_MERGE = True` while the function still raises. The notebook does not catch this error. | "Set `USE_MERGE` back to `False` until the function works." |
| `NameError: name 'USE_MERGE' is not defined` | The flag cell did not run, or it was run out of order. | "Run the cells from the top, in order." |
| `ModuleNotFoundError: No module named 'delta'` | The learner installed `delta-spark`, which this project does not use. | "Remove it. Use `spark.sql` with MERGE, as the reference does." |
| Silver 1,300 expected, but 1,215 or 1,250 appears | Day 2 was not loaded, or TODO-07 loaded the wrong files. | "Check `Files to load` in `10_bronze`, and the bronze count per `_source_file`." |
| `SHP000150` shows IN_TRANSIT | The day-2 file was not loaded, or the MERGE branch did not run (`USE_MERGE` still `False`). | "Is `USE_MERGE = True` in the notebook you ran?" |
| Quarantine is 12 after day 2, not 50 | TODO-02 is not done, so the day-1 rules miss 33 bad rows and the five `LOST` rows of day 2 pass into silver. | "What is the day-1 quarantine count now? Which reasons appear?" (Stage 02 fixes it.) |

## 7. DONE WHEN

From `docs/TODO.md`, copied exactly:

> (with TODO-02 and TODO-04 done) 00 days=2 → 10 → 20: silver 1,300, quarantine 50; SHP000150 status DELIVERED; rerun 20 → no change

Note for the tutor: this is a Databricks check (expected, not verified by the scaffold author on Free Edition).

## 8. Check-your-understanding questions

1. What are the two clauses in a MERGE, and when does each run? *Key points: WHEN MATCHED (key exists); WHEN NOT MATCHED (key new).*
2. Why does `s.event_ts > t.event_ts` make a replay harmless? *Key points: older rows never overwrite newer; the same run gives the same result.*
3. Why does MERGE fail on duplicate source keys? *Key points: each target row may match only one source row; the engine cannot choose.*
4. Why must dedup run before MERGE? *Key points: dedup makes one row per key in the source.*
5. Why can't a temp view be the MERGE target? *Key points: the target must be a Delta table with a log; a view is only a name.*
6. What is the difference between an upsert and an append? *Key points: upsert updates matching keys and inserts new ones; append only adds rows.*
7. What is the SCD1 reference in `dims.py` doing differently from silver? *Key points: SCD1 keeps no history, and it updates when any value changed; silver keeps one current row too, but the guard is on time.*

## 9. Interview link

- "How does MERGE work, and what happens with duplicate source keys?" [GENERAL] Mid
- "Why add an `event_ts` guard to a MERGE update?" [PROJECT] Mid
- "What is SCD Type 1 versus Type 2?" [PROJECT] Mid (answer links to TODO-16)
- "What does idempotency mean, and why does it matter for pipelines?" [PROJECT] Junior

Sentence for the learner: "Silver is an upsert on shipment_id with a time guard, so the newest status wins and a replay changes
nothing."

## 10. Wrap-up

- Tick TODO-08 in `docs/TODO.md` and `README.md` "My implementations", only after the DONE WHEN check passed.
- Confirm `USE_MERGE = True` in `notebooks/20_silver.py` is the value the learner keeps.
- Commit (the learner runs it): `git add -A && git commit -m "feat(todo-08): silver merge upsert with event_ts guard"`. Check
  `git status` first.
- Next stage: `docs/tutor/stage-07-data-quality-checks.md` (TODO-09).

**Say it like this (RU):**

> "MERGE — это обновление контактов в телефоне: тот же номер — обновить, если новая запись свежее; нового номера — добавить."

> "Guard по времени — это проверка даты на конверте. Пришло старое письмо — оно не перезаписывает новое."

> "Почему MERGE падает на дубликатах? Потому что одна строка в таблице не может выбрать, какой из двух одинаковых пришедших
> вариантов взять. Поэтому сначала дедупликация, потом MERGE."
