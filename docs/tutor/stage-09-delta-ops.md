# Stage 09: Delta operations, time travel and file layout (TODO-11, TODO-12)

Tutor note: the learner is an absolute beginner. Explain in Russian. Keep code, identifiers and commands in English.

## 1. Goal

Understand how a Delta table keeps its history (the transaction log), then use that history: go back to an earlier version, undo a bad write with `RESTORE`, and reduce the number of small files with `OPTIMIZE` and liquid clustering.

**Interview sentence the learner should be able to say:** "A Delta table is data files plus a transaction log. Every write is a commit, so I can read an earlier version with time travel and undo a bad write with RESTORE, which adds a new commit. I used OPTIMIZE and liquid clustering on silver and recorded the file counts before and after."

## 2. Before you start

- Stages 00-08 are done. `workspace.shipments.silver_shipments` exists and has history (several versions from earlier runs).
- The reference cells in `50_delta_ops` have run at least once (`DESCRIBE HISTORY` and `VERSION AS OF 0`).
- Files to open: `notebooks/50_delta_ops.py` (reference cell and the TODO-11 and TODO-12 cells), `docs/CONCEPTS.md` (sections on the transaction log, time travel and VACUUM), `docs/TODO.md#todo-11`, `docs/TODO.md#todo-12`.
- Ask first: "Did `50_delta_ops` run its reference cells? Which silver row count did you see last time?"
- Important: do not run `20_silver` between the DELETE and the RESTORE in TODO-11. The full-refresh path overwrites silver, which changes the versions you are trying to compare.

## 3. Concepts to teach first

### 3.1 The transaction log

- **Plain explanation:** a Delta table is a folder of Parquet data files plus `_delta_log`, a list of numbered commits. Each commit says which files were added and which were removed. Readers rebuild the current table from the list.
- **Analogy:** `git log` for a table. Each commit is a change you can read. Nothing is erased from the log by a normal write.
- **Where in this repo:** the reference cell in `50_delta_ops` section 1 runs `DESCRIBE HISTORY` on silver. Every write in notebooks 10, 20 and 30 adds an entry.
- **Tiny example (throwaway table, SQL cell):**
  ```sql
  CREATE OR REPLACE TABLE workspace.shipments.tmp_demo AS
  SELECT * FROM VALUES (1, 'Midwest'), (2, 'Gulf Coast') AS t(id, region);

  INSERT INTO workspace.shipments.tmp_demo VALUES (3, 'Northeast');

  DESCRIBE HISTORY workspace.shipments.tmp_demo;
  ```
  Ask: how many versions do you see, and which operation names appear? Look at the `operation` column.

### 3.2 Time travel

- **Plain explanation:** `VERSION AS OF n` reads the table as it was at commit n. Old versions are available while their data files still exist.
- **Analogy:** the photo album of the table. You can look at any page as long as nobody tore it out.
- **Where in this repo:** `50_delta_ops` section 1 counts silver at `VERSION AS OF 0`. TODO-11 compares two versions.
- **Tiny example (same demo table):**
  ```sql
  SELECT * FROM workspace.shipments.tmp_demo VERSION AS OF 0;
  ```
  Ask: why does this show only two rows?

### 3.3 `RESTORE` is a new commit

- **Plain explanation:** `RESTORE TABLE ... TO VERSION AS OF n` makes the current table look like version n. It does this by writing a new commit. The bad commit stays in the history.
- **Analogy:** an undo button that records "undone" in the log, rather than deleting the mistake from the log.
- **Where in this repo:** TODO-11 uses it on silver. The docs link is in TODO-11.
- **Tiny example (demo table, a different bad write from the TODO):**
  ```sql
  INSERT INTO workspace.shipments.tmp_demo VALUES (1, 'Midwest');  -- a duplicate, a "bad" write
  DESCRIBE HISTORY workspace.shipments.tmp_demo;
  ```
  Ask: which version number should you write down before the bad write, and why?

### 3.4 `EXCEPT`: comparing two versions

- **Plain explanation:** `EXCEPT` returns rows in the first query that are not in the second.
- **Analogy:** comparing two shopping lists and keeping what is only on the first one.
- **Tiny example (toy values):**
  ```sql
  SELECT * FROM VALUES (1), (2), (3) AS a(x)
  EXCEPT
  SELECT * FROM VALUES (2) AS b(x);
  ```
  Expected: rows 1 and 3.

### 3.5 OPTIMIZE and small files

- **Plain explanation:** each write adds files. Many small files make reads slower. `OPTIMIZE` rewrites small files into larger ones. The old files stay until `VACUUM` removes them, once no version needs them.
- **Analogy:** a shelf full of pocket notes that you copy onto a few sheets. The notes are still there until you throw them out later.
- **Where in this repo:** `CONCEPTS.md` section "OPTIMIZE and small files". TODO-12 runs OPTIMIZE on silver.

### 3.6 Liquid clustering and `DESCRIBE DETAIL`

- **Plain explanation:** `CLUSTER BY (col, ...)` tells Delta which columns to group rows by when it writes. `DESCRIBE DETAIL` shows table facts, including `numFiles` (how many data files) and `clusteringColumns`.
- **Analogy:** sorting a box of receipts by date and shop, so you find the one you need without emptying the box. Liquid clustering keeps the sort going as new receipts arrive.
- **Where in this repo:** TODO-12 `ALTER TABLE ... CLUSTER BY` on silver. Docs: the liquid clustering link in TODO-12.
- **Tiny example (throwaway table, different column from the TODO):**
  ```sql
  CREATE OR REPLACE TABLE workspace.shipments.tmp_demo_cluster (id INT, label STRING) CLUSTER BY (id);
  DESCRIBE DETAIL workspace.shipments.tmp_demo_cluster;
  ```
  Ask: find `clusteringColumns` in the output. Which columns do you see?

### 3.7 VACUUM, and why retention is a safety rail

- **Plain explanation:** `VACUUM` deletes data files that no current version needs and that are older than the retention period (7 days by default). `DRY RUN` lists the files without deleting anything.
- **Analogy:** clearing the old backups of a project. Clearing backups from last week is fine. Clearing them while you still need last month's version is how you lose work.
- **Where in this repo:** the warning in `50_delta_ops` (markdown cell) and in TODO-11. The repo never runs `VACUUM ... RETAIN 0 HOURS`.
- **Safe example (dry run only):**
  ```sql
  VACUUM workspace.shipments.tmp_demo DRY RUN;
  ```
  Do not type `RETAIN 0 HOURS`, even on a demo table. The habit is the risk. Delta also has a safety check that refuses very short retention by default; do not disable it.

### 3.8 Clean up demo tables

```sql
DROP TABLE IF EXISTS workspace.shipments.tmp_demo;
DROP TABLE IF EXISTS workspace.shipments.tmp_demo_cluster;
```

### Say it like this (RU)

- "Лог транзакций Delta — это `git log` для таблицы: каждая запись — коммит, и обычная запись ничего не стирает из истории."
- "`RESTORE` — это не стирание ошибки, а новый коммит, который возвращает старое состояние. История остаётся видна, и это плюс, а не минус."
- "`VACUUM` с `RETAIN 0 HOURS` — это как удалить и старые ветки, и файлы, на которые ещё ссылаются другие. Поэтому в этом проекте эту команду не пишем вообще."

## 4. Step-by-step plan

1. **Read the history.** Run `DESCRIBE HISTORY` on silver and name three operations you see.
   - check: the learner names at least two of WRITE, MERGE, OPTIMIZE, RESTORE, DELETE, or CREATE OR REPLACE, and says which notebook caused them.
2. **Practise on the demo table.** Run the section 3.1 to 3.4 examples.
   - check: the learner can explain why `VERSION AS OF 0` still shows the old rows after an INSERT.
3. **TODO-11: write down the numbers before the bad delete.** Count silver rows, and note the latest version number from `DESCRIBE HISTORY`. Write both in `docs/NOTES.md` (the learner creates this file).
   - check: the learner has a count and a version number written down before any DELETE.
4. **TODO-11: diff, delete, restore.** Use `EXCEPT` to compare an earlier version with the current table. Then delete the rows the TODO names, and restore to the version you wrote down.
   - check: silver count is back to the number you wrote down, and `DESCRIBE HISTORY` shows both the DELETE and the RESTORE.
5. **TODO-12: write down numFiles.** Run `DESCRIBE DETAIL` on silver and note `numFiles` before any change.
   - check: learner has a `numFiles` value written down.
6. **TODO-12: cluster and optimise.** Add clustering on silver, then run OPTIMIZE, then `DESCRIBE DETAIL` again.
   - check: `clusteringColumns` lists both columns from the TODO. Write down the new `numFiles`. If the table is small, the count may not change much; say so in your notes instead of inventing a result.
7. **Dry run VACUUM (optional, read only).** Run `VACUUM ... DRY RUN` on the demo table only.
   - check: the learner can say what would be deleted and why nothing is deleted in a dry run.
8. **Clean up.** Drop the demo tables from the section 3.8 block.
   - check: `SHOW TABLES IN workspace.shipments` no longer lists `tmp_demo` or `tmp_demo_cluster`.

## 5. Hint ladder per TODO

### TODO-11 (diff two versions, DELETE Midwest rows, RESTORE)

- **Level 0 (goal):** Show what changed between two versions, make a bad delete on silver, and bring silver back to the count from before the delete. The history must show both operations.
- **Level 1 (concept):** Every write is a commit with a number. Note the number before the bad write. A restore goes back to that number and is itself a new commit.
- **Level 2 (tool):** `DESCRIBE HISTORY`, `VERSION AS OF`, `EXCEPT`, `DELETE FROM ... WHERE ...`, `RESTORE TABLE ... TO VERSION AS OF`. The reference cell in `50_delta_ops` already uses `DESCRIBE HISTORY` and `VERSION AS OF`.
- **Level 3 (skeleton with blanks):**
  ```sql
  -- before anything: note the count and the version number
  SELECT * FROM ___ VERSION AS OF ___ EXCEPT SELECT * FROM ___;
  DELETE FROM ___ WHERE ___ = ___;
  RESTORE TABLE ___ TO VERSION AS OF ___;
  DESCRIBE HISTORY ___;
  ```
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "note the version, delete on purpose, restore to the noted version, and read the history to prove it". Then walk through your own statements line by line and retype any fixes yourself. Afterwards give a variation: "Restore to the version before the last OPTIMIZE instead. Does the count change? Why or why not?"

### TODO-12 (clustering, OPTIMIZE, compare file counts)

- **Level 0 (goal):** Silver is clustered on region and event date. You have file counts before and after OPTIMIZE.
- **Level 1 (concept):** Clustering chooses how rows are grouped in files. OPTIMIZE rewrites small files. The clustering columns are a table property you can read in `DESCRIBE DETAIL`.
- **Level 2 (tool):** `ALTER TABLE ... CLUSTER BY (...)`, `OPTIMIZE`, `DESCRIBE DETAIL`. Look at the liquid clustering docs for the rule that a table uses clustering or ZORDER, not both.
- **Level 3 (skeleton with blanks):**
  ```sql
  -- before: DESCRIBE DETAIL ___  -> write down numFiles
  ALTER TABLE ___ CLUSTER BY (___, ___);
  OPTIMIZE ___;
  -- after: DESCRIBE DETAIL ___  -> write down numFiles and clusteringColumns
  ```
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "record before, cluster, optimise, record after, and report what the numbers do and do not show". Then retype your fixed statements yourself. Afterwards give a variation: "Run `DESCRIBE HISTORY` and find the OPTIMIZE and CLUSTER entries. Which version number did each one create?"

## 6. Common mistakes and error messages

| Symptom | Likely cause | What to ask or suggest |
|---|---|---|
| The RESTORE target count does not match | The version number was read after the DELETE, so it points at the wrong commit | "Which version did you write down, and was it before or after the delete?" |
| `VERSION AS OF` says the version is missing or its files were removed | Retention or a VACUUM removed the files that version needs (exact message varies, expected, not verified) | "Which version? Did anyone run VACUUM?" Never answer this by reaching for RETAIN 0 HOURS. |
| The history shows no RESTORE | RESTORE did not run, or the statement failed silently in a cell you did not read | "Read the output cell of the RESTORE line. Did it error?" |
| `clusteringColumns` is missing or empty | The ALTER did not run, or a later run of `20_silver` replaced the table | "Did the ALTER succeed? Did you re-run `20_silver` afterwards?" The fallback path writes with `overwriteSchema`, which may reset table metadata (expected, not verified). Re-check after each run. |
| `numFiles` is the same before and after OPTIMIZE | Normal on a small table | "Say that in your notes. Do not invent a difference." |
| ZORDER and CLUSTER BY error on the same table | A table uses one or the other | "Which of the two is on the table now?" |
| `TABLE_OR_VIEW_NOT_FOUND` for silver | `20_silver` has not created silver yet | "Has `20_silver` run on this schema?" |
| The learner types `VACUUM ... RETAIN 0 HOURS` | Dangerous, and the repo forbids it | Stop. Ask what they want to achieve (free space? clean demo?). Explain that the default retention protects time travel and concurrent readers. Never disable the safety check. |

## 7. DONE WHEN

Copied from `docs/TODO.md`:

> **DONE WHEN (TODO-11):** silver count back to pre-delete value; history shows DELETE and RESTORE ops

> **DONE WHEN (TODO-12):** `clusteringColumns` shows both columns; write down numFiles before/after

Tutor check: compare the silver count with the number written down before the delete. Read `DESCRIBE HISTORY` and find the DELETE and RESTORE rows. For TODO-12, read `clusteringColumns` and the two `numFiles` values from `docs/NOTES.md`.

Note: Databricks-only check. Liquid clustering, OPTIMIZE and RESTORE on Free Edition are expected to work, but the scaffold author has not verified them.

## 8. Check-your-understanding questions

1. What is in a Delta table folder? Good answer: data files (Parquet) and `_delta_log`, the ordered list of commits.
2. Why does `VERSION AS OF` still work after an INSERT? Good answer: the old version is still described in the log, and its files still exist.
3. What does RESTORE actually do to the history? Good answer: it adds a new commit; the bad commits stay visible.
4. Why must you write down the version number before the bad write? Good answer: after the write, the number you want is one of the older ones, and it is easy to read the wrong one.
5. What does OPTIMIZE change, and what does it not remove? Good answer: it rewrites small files into larger ones; the old files stay until VACUUM removes them.
6. What is the difference between ZORDER and liquid clustering? Good answer: ZORDER is a one-off layout in OPTIMIZE; liquid clustering is set with CLUSTER BY and maintained incrementally; a table uses one of them.
7. Why is `VACUUM ... RETAIN 0 HOURS` dangerous? Good answer: it removes files that time travel, RESTORE and concurrent readers still need, so history can no longer be read.
8. Why is a small table's `numFiles` not a benchmark? Good answer: a few files show little; say what the numbers do and do not show.

## 9. Interview link

From `docs/INTERVIEW_PREP.md`:

- **Q:** "How does the transaction log work, and what are checkpoints?" [GENERAL] Mid
- **Q:** "What is time travel, and what limits it?" [GENERAL] Junior
- **Q:** "How would you recover from a bad `DELETE` or a bad write?" [PROJECT] Mid
- **Q:** "What does `VACUUM` do, and why is `RETAIN 0 HOURS` dangerous?" [GENERAL] Mid
- **Q:** "Z-ORDER versus liquid clustering: which would you use and why?" [GENERAL] Mid

One sentence the learner can use: "I recorded the version before a bad delete, restored to it with RESTORE, and showed the history. RESTORE is a new commit, so nothing disappears from the audit trail."

## 10. Wrap-up

- Tick `- [ ] Done` under `TODO-11` and `TODO-12` in `docs/TODO.md`.
- Tick `TODO-11 Time travel + RESTORE` and `TODO-12 OPTIMIZE + liquid clustering` in the README checklist.
- Suggested commits, one per TODO (the learner runs them after checking `git status`). Commit TODO-11 when its DONE WHEN passes, then TODO-12:
  ```
  git add -A && git commit -m "feat(todo-11): time travel diff and RESTORE on silver"
  git add -A && git commit -m "feat(todo-12): liquid clustering and OPTIMIZE on silver"
  ```
  Do not push without a deliberate decision.
- **Next stage:** `stage-10-monitoring-plans.md` (TODO-13). The learner will log each run in `pipeline_runs` and read a physical plan to find the shuffle.
