# Stage 00: Setup (Free Edition, workspace, first notebook)

> Tutor: read `docs/tutor/README.md` first (learner profile, hint ladder, style rules). Explain in Russian; keep UI labels,
> code and technical terms in English. This stage has **no TODO**. Its job is to get the learner to a working notebook and to
> teach the minimum Python and SQL needed for stages 01 to 06. Every UI label below may differ on the learner's screen:
> describe the goal, then let them find the control. Free Edition behaviour is **not verified by the scaffold author**.

## 1. Goal in one sentence

The learner has a Databricks Free Edition workspace, a Git folder that points at **their own fork**, a notebook attached to
**Serverless** compute that runs cells, and enough Python and SQL to read the stage guides.

**Interview afterwards:** "This project runs on Databricks serverless compute, its notebooks come from a Git folder, and it
never deploys anything."

## 2. Before you start

- The learner has a GitHub account and has forked the repository (README "Run on Databricks Free Edition", step 2). Ask:
  "Is your fork public or private?" A private fork needs a Git credential in the Databricks UI.
- Nothing needs to be installed on the laptop for this stage. Local Python, Java and pytest come later (stage 02).
- Open together: `README.md` section "Run on Databricks Free Edition" (steps 1 to 5); `docs/CONCEPTS.md` sections
  "Spark Connect and serverless", "Unity Catalog names and volumes", "Git folders".
- Workspace state expected: empty. The schema `workspace.shipments` and the volume `raw` are created by `00_generate_batches`
  in stage 01, so they should not exist yet.
- Do not ask for or store passwords, tokens or API keys. If the learner pastes one in chat, tell them to rotate it.

## 3. Concepts to teach first

### 3.1 Workspace, notebook, cell

**Plain explanation.** The workspace is your online project room. A notebook is a document made of cells. A cell is one block
of code (Python by default, or SQL) or Markdown text. You run one cell at a time, or "Run all".

**Analogy.** A lab notebook: each block is one experiment step you can repeat without redoing the whole experiment.

**Where in this repo.** Every notebook is a `.py` file in `notebooks/`. It starts with `# Databricks notebook source`, cells are
separated by `# COMMAND ----------`, and Markdown lines start with `# MAGIC %md`. Look at `notebooks/00_generate_batches.py`.

**Tiny example** (a Python cell):
```python
print("hello from a cell")
```
A first line of `%sql` makes that cell run as SQL. `%md` makes it text. Explain these as "magic commands"; the learner only
needs `%sql` in this project.

### 3.2 Serverless compute

**Plain explanation.** Compute is the machine that runs the code. "Serverless" means Databricks owns and sizes the machines.
The learner does not create a cluster. Free Edition offers serverless only (README, TODO notes). Limits are on the official
limitations page, which is linked in `docs/TODO.md` (TODO-18).

**Analogy.** A taxi instead of owning a car: you ask for a ride when you need one, and you do not maintain it.

**Where in this repo.** `resources/shipments_job.yml` has no compute block, so its tasks run on serverless. The notebooks say
"Run on: Serverless" in their header.

**Tiny example** (needs serverless attached):
```python
spark.range(5).show()
```
Expected output: a small table with `id` values 0 to 4 (expected, not verified by the scaffold author on Free Edition).

### 3.3 The `spark` object and Spark Connect

**Plain explanation.** Each notebook has a ready variable `spark`. It is your connection to the Spark engine. On serverless,
Spark Connect sends your plan to a remote server, and the server runs it (`docs/CONCEPTS.md`, "Spark Connect and serverless").

**Analogy.** A phone line to a call centre: you describe what you want, the centre does the work.

**Where in this repo.** `tests/conftest.py` gets the session in three ways. The repo never uses `spark.sparkContext`, RDDs or
`dbfs:/` paths (`docs/SCAFFOLD_SPEC.md` section 0).

**Tiny example** (wrong on purpose, to show the error type; expected wording, not verified):
```python
spark.sparkContext
```
Expected: an error saying an attribute that depends on the JVM is not supported in Spark Connect. Say: "this repo avoids it on
purpose."

### 3.4 Unity Catalog names and volumes

**Plain explanation.** A table name has three parts: `catalog.schema.table`, for example `workspace.shipments.silver_shipments`.
A **volume** is governed storage for files, addressed as `/Volumes/<catalog>/<schema>/<volume>/...`.

**Analogy.** A building (catalog), a floor (schema), a room (table). A volume is a shared filing cabinet for paper files.

**Where in this repo.** `src/shipments_pipeline/config.py`: `PipelineConfig.table()` builds the table name and
`PipelineConfig.volume_path` builds the path. The defaults are `workspace`, `shipments`, `raw`.

**Tiny example:**
```python
spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema").show()
```
Expected on a fresh workspace: catalog `workspace` (expected, not verified by the scaffold author).

### 3.5 Widgets

**Plain explanation.** A widget is a parameter box at the top of a notebook. Its value is read in code with `dbutils.widgets.get`.
Jobs can pass the same values in.

**Analogy.** A form field: you change the value on screen, the code stays the same.

**Where in this repo.** `config.get_widget()` creates a text widget with a default if it does not exist yet, and
`config.from_widgets()` reads `catalog`, `schema` and `volume`. Each notebook has `days`, `supplier_version` or `run_id`.

**Tiny example:**
```python
dbutils.widgets.text("days", "1")
print(dbutils.widgets.get("days"))
```
Change the value in the top bar, run the cell again, and the printed value changes. Widgets keep their last value, which is a
common source of confusion (see section 6).

### 3.6 Git folders

**Plain explanation.** A Git folder is a workspace folder linked to a Git repository. You pull and commit from the UI. The
repository on GitHub stays the source of truth, so notebook edits are not the only copy of the work.

**Analogy.** A shared drive that syncs with a version-controlled archive.

**Where in this repo.** README step 3. Docs: https://docs.databricks.com/aws/en/repos/ (link from `docs/TODO.md`).

### 3.7 Python the learner needs for stages 01 to 06

Teach only these, one per message, each with a one-line example in a scratch cell:

- **Variable:** `days = 1` (a labelled box).
- **Function:** `def double(x):` then an indented `return x * 2`. A function is a reusable recipe.
- **Import:** `from pyspark.sql import functions as F` (borrow a toolbox; `F` is a short nickname).
- **f-string:** `f"wrote {n} rows"` puts a value into text.
- **Collections:** list `[1, 2]`, tuple `(1, 2)` (fixed, unpackable), dict `{"a": 1}`, set `{1, 2}` (no duplicates, fast `in`).
- **Loop and list comprehension:** `[n * 10 for n in range(3)]` gives `[0, 10, 20]`.
- **`None`:** Python's "no value". Spark's equivalent is `NULL`.
- **Tuple unpacking:** `valid, quarantine = split(df)` assigns two returned values at once.
- **`try` / `except`:** run code and catch an error (the repo uses it for `NotImplementedError`).
- **`assert`:** "this must be true, or stop with an error" (used in tests).

### 3.8 SQL the learner needs

- `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `ORDER BY`, aggregates `COUNT`, `SUM`, `AVG`, and aliases with `AS`.
- Example, which runs without any table:
```sql
SELECT id % 2 AS parity, COUNT(*) AS n
FROM range(10)
GROUP BY id % 2
ORDER BY parity
```
Expected: two rows, parity 0 with n = 5 and parity 1 with n = 5.

## 4. Step-by-step plan

1. **Sign up** for Databricks Free Edition (search for "Databricks Free Edition", as the README says). Use your own account.
   The sign-up screens change often: describe the goal, not the buttons.
   *Check:* you can open a workspace home page.
2. **Look at the sidebar.** Find Workspace, Catalog, Jobs and Pipelines (or Workflows), and a SQL area. Do not run any job.
   *Check:* the learner names the four areas and what each is for.
3. **Confirm the fork** opens in a browser under their GitHub account.
   *Check:* the repo page shows `notebooks/` and `src/`.
4. **Create a Git folder:** Workspace, then Create, then Git folder. Paste the fork URL. Accept the defaults. If asked for
   credentials on a private fork, use the Databricks dialog and never paste a token into chat.
   *Check:* the folder shows the repo files, and `notebooks/00_generate_batches.py` opens.
5. **Open the notebook without running it.** Point at the header, a `# MAGIC %md` cell, a code cell, and the widget setup.
   *Check:* the learner can say what a cell is.
6. **Attach Serverless.** Use the compute selector at the top of the notebook.
   *Check:* the notebook shows Serverless as attached, and a cell runs.
7. **Run `print("hello")`, then `spark.range(5).show()`.**
   *Check:* five rows with ids 0 to 4.
8. **Run `display(spark.table("samples.nyctaxi.trips").limit(5))`.** The sample data may be absent (expected, not verified).
   *Check:* five rows. If you get `TABLE_OR_VIEW_NOT_FOUND`, use `display(spark.range(5))` instead and move on.
9. **Run the catalog SQL from 3.4.**
   *Check:* the learner reads the catalog and schema names out loud.
10. **Widget exercise** from 3.5.
    *Check:* the printed value follows the value typed in the top bar.
11. **Mini quiz.** Ask three Python questions (what does `f"{x}"` print, what is `None`, what does `[n for n in range(3)]`
    give) and two SQL questions (what `COUNT(*)` counts, what `GROUP BY` does).
    *Check:* all answers correct, or re-explain with a different example.
12. **Look at Catalog.** Open the Catalog area. The `shipments` schema should not exist yet.
    *Check:* the learner understands this is expected and that stage 01 creates it.

## 5. Hint ladder (no TODO here; use it for setup problems)

| Level | Example (a Git folder or cell that will not work) |
|---|---|
| 0 Goal | "One notebook from your repo runs one cell on Serverless." |
| 1 Concept | "Compute is the machine. A notebook must be attached to one before a cell can run." |
| 2 Tool | "The compute selector is at the top of the notebook. Git folders are created from the Workspace area, under Create." |
| 3 Skeleton | A checklist: (a) fork URL works, (b) Git folder created, (c) notebook opened from inside it, (d) Serverless attached. Ask which box is not ticked. |
| 4 Solution | Only on explicit request: walk the learner through the screen step by step. They click, you read the screen. |

## 6. Common mistakes and error messages

| Symptom | Likely cause | What to ask or suggest |
|---|---|---|
| `NameError: name 'spark' is not defined` | Code runs outside Databricks (for example a local Python file). | "Where are you running this? Is the notebook attached?" |
| Cell waits forever or says no compute | Compute not attached. | Select Serverless in the compute selector, then run again. |
| `ModuleNotFoundError: No module named 'shipments_pipeline'` | The notebook is not inside the Git folder's `notebooks/` folder. The notebooks build the path `../src` from the current folder. | "Which folder is the notebook in? It must be inside the repo's `notebooks/`." |
| `[JVM_ATTRIBUTE_NOT_SUPPORTED]`-style error on `spark.sparkContext` (expected wording) | Spark Connect has no JVM attributes. | "This repo never uses that. Use the DataFrame API." |
| `TABLE_OR_VIEW_NOT_FOUND` for `samples.nyctaxi.trips` | Sample data missing on this workspace (expected, not verified). | Use `spark.range(5)` and continue. |
| Git folder asks for credentials or says authentication failed | Private fork. | Use the Databricks Git credential dialog, or ask whether the fork can be public. The learner decides. |
| Widget shows an old value | Widgets keep their last value. | Change the value in the top bar, then run the cell again. |
| Learner pastes a token or password | Secret leaked to chat. | Stop. Tell them to revoke it and create a new one. |

## 7. DONE WHEN

No TODO in `docs/TODO.md` for this stage. Stage 00 is done when the learner has:

- a notebook from their Git folder open, attached to Serverless;
- `spark.range(5).show()` printing five rows;
- the catalog SQL and the widget exercise working;
- answered the mini quiz.

## 8. Check-your-understanding questions

1. What is the difference between a notebook and a cell? *Key points: a notebook is a file of cells; a cell is one runnable block.*
2. Why does this project have no cluster configuration? *Key points: serverless; Free Edition offers only serverless; less to manage.*
3. What does `workspace.shipments.silver_shipments` mean, part by part? *Key points: catalog, schema, table.*
4. Why use a widget for `days` instead of typing `days = 1` into the code? *Key points: parameters change without editing code; jobs pass the same values.*
5. What does `spark.range(5)` show if you do not call `.show()`? *Key points: a DataFrame object, not rows; Spark is lazy.*
6. Why should the Git folder point at the fork? *Key points: the repo is the source of truth; commits go to the learner's GitHub.*
7. Why avoid `spark.sparkContext`? *Key points: Spark Connect; the repo uses only the DataFrame and SQL API.*
8. What does `f"wrote {n} rows"` produce when `n = 1260`? *Key points: the text `wrote 1260 rows`.*

## 9. Interview link

Questions in `docs/INTERVIEW_PREP.md` the learner can answer after this stage:

- "What is the difference between all-purpose clusters, job clusters and serverless compute?" [GENERAL] Junior
- "What are Git folders, and how do they fit with the project?" [GENERAL] Junior
- "What is the Unity Catalog three-level namespace?" [GENERAL] Junior
- "What are the limits of Free Edition that matter for this project?" [GENERAL] Stretch (answer honestly: most limits are not verified by the scaffold author)

Sentence for the learner: "This project runs on Databricks Free Edition with serverless compute, takes its code from a Git
folder, and never deploys."

## 10. Wrap-up

- No checkbox to tick. Do not commit anything just for this stage.
- Ask the learner to say the goal sentence from section 1 in their own words.
- Next stage: `docs/tutor/stage-01-run-and-read.md` (TODO-01, run the pipeline and read it).

**Say it like this (RU):**

> "Notebook — это как лабораторный журнал: каждая ячейка — отдельный шаг опыта, и её можно запустить снова."

> "Serverless — это как такси: машину подают, когда нужно, и тебе не надо покупать и настраивать автомобиль."

> "Widget — это как поле в форме: значение меняешь сверху, а код остаётся тем же."
