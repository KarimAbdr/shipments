# Stage 05: Incremental bronze and idempotency (TODO-07)

> Tutor: read `docs/tutor/README.md` first. Explain in Russian; keep code in English. The learner writes `list_new_files` in
> `src/shipments_pipeline/bronze.py`. The notebook `notebooks/10_bronze.py` already reads the list, so the learner usually
> needs no notebook change. Most of this stage is about **idempotency**: running twice must not change the result.

## 1. Goal in one sentence

The learner implements `list_new_files` so that re-running `10_bronze` loads only files that bronze has not seen yet, and bronze
stays at 1,260 rows after day 1, however many times the notebook runs.

**Interview afterwards:** "My bronze load is idempotent. It records which source file each row came from and only loads
files it has not seen, so a retry never duplicates data."

## 2. Before you start

- Stage 01 done (bronze 1,260 after one run). Stage 02 to 04 are not needed for this TODO.
- Check the bronze state first. If `10_bronze` was run twice before TODO-07, bronze has 2,520 rows. Ask the learner before
  deleting anything. Recovering is their decision (section 6).
- Open together: `src/shipments_pipeline/bronze.py` (`read_batch` and the TODO-07 block), `notebooks/10_bronze.py`
  (the "Choose which files to load" cell and the TODO-07 comment), `docs/CONCEPTS.md` sections "Idempotency" and
  "File metadata column".
- Ask one warm-up question: "If a network error stops a job halfway, what happens to a retry without a file check?" (Duplicates.)

## 3. Concepts to teach first

### 3.1 Idempotency

**Plain explanation.** An operation is idempotent when running it twice gives the same result as running it once. For a
file load, that means "a file already loaded is skipped".

**Analogy.** A lift button. Pressing it twice does not send the lift twice. A lift that counts each press would be broken.

**Where in this repo.** `bronze.list_new_files` (TODO-07) and the `ingested` set in `10_bronze`. Stage 06 adds the same idea to
MERGE: `s.event_ts > t.event_ts`.

### 3.2 Incremental load

**Plain explanation.** An incremental load processes only what is new since the last run. A full refresh processes everything every
time. Bronze here is append-only: new files add rows, and old rows are not rewritten.

**Analogy.** Reading only the new messages in a chat, not the whole history on every visit.

**Where in this repo.** `10_bronze` builds `volume_files` (all CSVs in the volume), builds `ingested` (file names already in
bronze), and calls `list_new_files(volume_files, ingested)`.

### 3.3 Files as the unit of loading: `_source_file`

**Plain explanation.** `bronze.read_batch` adds `_source_file` from `_metadata.file_path`. That is a hidden file metadata column
that Spark provides when it reads files. Every row remembers its file, so the pipeline can ask "which files are already in bronze?"

**Analogy.** A stamp on every parcel that says which delivery it came in.

**Tiny example** (Databricks check, expected output format not verified by the scaffold author):
```python
spark.read.option("header", True).csv("/Volumes/workspace/shipments/raw/shipments_2026-10-01.csv") \
    .select("_metadata.file_path").distinct().show(truncate=False)
```
Expected: one full path ending in `shipments_2026-10-01.csv`. Ask: why `distinct()`? (One value per file, not per row.)

### 3.4 Filtering a list with a comprehension

**Plain explanation.** A list comprehension is a compact loop that keeps the items for which a condition is true: `[x for x in items if condition]`. The order of the input is kept. Membership tests (`in`, `not in`) are fast on sets.

**Tiny example** (a different condition, so the learner transfers the idea):
```python
names = ["a.csv", "notes.txt", "c.csv"]
print([n for n in names if n.endswith(".csv")])   # ['a.csv', 'c.csv']
```
Ask: what would the output be if the condition were `not n.endswith(".csv")`? (`['notes.txt']`.) Then ask how the same loop shape would change if the condition were a membership test against a set of names.

**Basenames.** `os.path.basename("/Volumes/workspace/shipments/raw/a.csv")` gives `a.csv`. The notebook already reduces both sides
to file names, so `list_new_files` compares plain names. The learner should not add path conversion.

### 3.5 Why the notebook has a fallback

`10_bronze` catches `NotImplementedError` and loads every file with a warning. That keeps day 1 working before TODO-07. The warning
also tells the learner that reruns will duplicate data until the function exists.

## 4. Step-by-step plan

1. **Read the 10_bronze "Choose which files to load" cell.** Note how `ingested` is built.
   *Check:* the learner says why `os.path.basename` is applied to `_source_file` values.
2. **Read the TODO-07 comment block in `bronze.py`.**
   *Check:* the learner states the function's input (all names, already-ingested names) and output (new names, in order).
3. **Record the current state.** Run in a scratch cell: `spark.table(cfg.table(config.BRONZE)).count()` and the group-by of
   `_source_file`.
   *Check:* the count is 1,260 (one run) or 2,520 (two runs). Write it down.
4. **Run `10_bronze` once more without the fix.** Observe the warning and the count. Skip this step if the count is already 2,520.
   *Check:* the learner sees the duplicate counts. This is the bug TODO-07 fixes.
5. **Write `list_new_files`** in `bronze.py`. Keep the original order.
   *Check:* in a scratch cell, `bronze.list_new_files(["x.csv", "y.csv"], {"x.csv"})` returns `['y.csv']`.
6. **Reset bronze only with the learner's explicit decision** (section 6), then load day 1 once.
   *Check:* bronze is 1,260 and the printout lists `Files to load: ['shipments_2026-10-01.csv']`.
7. **Run `10_bronze` a second time.**
   *Check:* `Files to load: []`, `Bronze rows written in this run: 0`, and bronze is still 1,260.

## 5. Hint ladder per TODO

### TODO-07: incremental bronze

| Level | What you give |
|---|---|
| 0 Goal | "Return only file names that bronze has not loaded yet, in their original order. Bronze stays at 1,260 after day 1." |
| 1 Concept | "A file list minus a 'seen' set, keeping order. Think of the lift button: a file already loaded is skipped." |
| 2 Tool | "A set membership test (`in` / `not in`) over the file list, keeping the original order. The file metadata column is explained in the docs (link in TODO-07)." |
| 3 Skeleton | `result = []` / `for name in ___:` / `    if ___:` / `        result.append(name)` / `return result`. Ask: which condition means "not loaded yet"? |
| 4 Solution | Described only: "loop over the file names, keep those not in the seen set, return the kept list." The learner writes the condition and explains it. |

Variation after level 4 (only if asked): "What if the seen set held full paths instead of names? What would you change, and where?"

## 6. Common mistakes and error messages

| Symptom | Cause | What to ask or suggest |
|---|---|---|
| Bronze doubles on every run | `list_new_files` is missing or raises `NotImplementedError`, so the fallback loads all files. The printed warning says so. | "Is the `WARNING: TODO-07 not done yet` line still printed?" |
| `Files to load: []` on the first run | The condition is inverted (`in` instead of `not in`), or the seen set is wrongly filled. | "Print `volume_files` and `ingested`. Which one is empty?" |
| Files are never loaded because `ingested` is always empty | `bronze_shipments` does not exist yet, or the basenames do not match. | "Does the table exist? Print `ingested` and compare with the file names." |
| Warning still shows after the function is written | The notebook still runs the old code (autoreload did not pick up the edit). | Detach and re-attach the notebook, or restart Python (README step 10; not yet verified on Free Edition). |
| The order of files changes between runs | The function returns a set. | "Sets have no order. Use a list comprehension or a loop with append." |
| Bronze count is 2,520 and the learner panics | Known result of two runs before TODO-07. | Explain it. Ask before resetting. |
| Learner deletes the bronze table without asking | Destructive action on their own learning data. | Say what will be lost (the loaded history). Ask for explicit confirmation first. |

## 7. DONE WHEN

From `docs/TODO.md`, copied exactly:

> running 10_bronze twice keeps bronze at 1,260 (day 1)

Note for the tutor: this is a Databricks check (expected, not verified by the scaffold author). The local check
`list_new_files(["x.csv", "y.csv"], {"x.csv"})` is only a sanity test, not the DONE WHEN.

## 8. Check-your-understanding questions

1. What does idempotent mean, in one sentence a manager would understand? *Key points: a second run changes nothing.*
2. Why does bronze store `_source_file` on every row? *Key points: it is the record of which files are loaded.*
3. Why is `mode("append")` safe only together with a file check? *Key points: append adds rows every time; without the check,
   every run adds the same rows.*
4. Why does the function return a list and not a set? *Key points: order is kept; the notebook loops in order.*
5. What is the fallback in `10_bronze` for, and what does its warning tell the learner? *Key points: day 1 works before the TODO;
   reruns duplicate until the function exists.*
6. Why does the repo not need to convert path formats? *Key points: both sides already hold file names.*
7. What would happen to a retry of a failed job with no file check? *Key points: duplicates.*

## 9. Interview link

- "What does idempotency mean, and why does it matter for pipelines?" [PROJECT] Junior
- "What are incremental loads, and how do they differ from full refreshes?" [PROJECT] Junior
- "How would you extract a file name in Spark, and why does bronze need it?" [PROJECT] Mid
- "How do you pass parameters into notebooks in a job, and how does the project do it?" [PROJECT] Junior

Sentence for the learner: "Bronze is idempotent: it records the source file on every row and loads only files it has not seen,
so a retry does not duplicate shipments."

## 10. Wrap-up

- Tick TODO-07 in `docs/TODO.md` and `README.md` "My implementations", only after the DONE WHEN check passed.
- Commit (the learner runs it): `git add -A && git commit -m "feat(todo-07): incremental bronze load skips files already ingested"`.
  Check `git status` first.
- Next stage: `docs/tutor/stage-06-merge.md` (TODO-08).

**Say it like this (RU):**

> "Идемпотентность — как кнопка лифта: нажал два раза, лифт поедет один раз. Повторный запуск не должен дублировать поставки."

> "`_source_file` — это штамп на каждой посылке: из какой поставки она пришла. По штампам мы знаем, что уже загружено."

> "Список «новых» файлов — это разность двух множеств с сохранением порядка. Мы берём файлы из папки и вычитаем те, что уже
> загружены."
