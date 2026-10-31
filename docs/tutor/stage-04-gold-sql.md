# Stage 04: Supplier scorecard in Spark SQL (TODO-06)

> Tutor: read `docs/tutor/README.md` first. Explain in Russian; keep SQL in English. The learner writes
> `build_supplier_scorecard` in `src/shipments_pipeline/gold.py` as one `spark.sql(...)` call with a CTE. This stage shows the
> same logic as a DataFrame pipeline, in SQL.

## 1. Goal in one sentence

The learner writes a Spark SQL query that builds a per-supplier scorecard (volume, delay rate, average cost, rating, and a rank of
delay) from silver and the supplier dimension, so that `gold_supplier_scorecard` has 24 rows and rank 1 is the highest delay rate.

**Interview afterwards:** "I aggregate silver first, then join the dimension onto the aggregate, so the join cannot multiply rows.
I rank with `RANK()` over the delay rate."

## 2. Before you start

- Stages 01 to 03 done. `silver_shipments` exists (day 1, after `20_silver`), and `dim_supplier` exists (`20_silver` creates it
  with supplier version 1).
- Open together: `src/shipments_pipeline/gold.py` (`build_region_part` for the aggregate style, and the TODO-06 block),
  `src/shipments_pipeline/dims.py` (`upsert_dim_supplier`, for the SQL style), `notebooks/30_gold.py` (TODO-06 cell),
  `docs/CONCEPTS.md` section "Narrow vs wide transformations" and "MERGE and upsert" (for later).
- Ask one warm-up question: "Which two inputs does a join need, and which column joins them?" (`supplier_id`.)

## 3. Concepts to teach first

### 3.1 Spark SQL from Python

**Plain explanation.** `spark.sql("...")` runs a SQL string and returns a DataFrame. Table names are full names, so the function
receives `silver_table` and `dim_table` and puts them in the query with an f-string. Then the result can be saved or displayed like
any DataFrame.

**Analogy.** Ordering from a menu in a language you know. The kitchen (Spark) is the same, only the order slip is different.

**Tiny example:**
```python
spark.sql("SELECT 1 + 1 AS two").show()
```
Expected: one row, `two = 2`.

### 3.2 CTE: a named intermediate result

**Plain explanation.** A CTE (common table expression) is a `WITH name AS (...)` block. It is a named sub-query you can use
below. It makes long SQL readable and lets you aggregate first and join second.

**Analogy.** A sticky note with a name on it: "per-supplier totals". You write the note once, then use it in the next step.

**Tiny example** (temp views for the toy):
```python
spark.createDataFrame([(1, "Alpha"), (2, "Beta")], "id int, name string").createOrReplaceTempView("t_names")
spark.createDataFrame([(1, 10), (1, 20), (2, 5)], "id int, amount int").createOrReplaceTempView("t_orders")
spark.sql("""
  WITH totals AS (
    SELECT o.id, SUM(o.amount) AS total
    FROM t_orders AS o
    GROUP BY o.id
  )
  SELECT n.name, t.total
  FROM totals AS t
  JOIN t_names AS n ON n.id = t.id
""").show()
```
Expected: Alpha 30, Beta 5. Explain `createOrReplaceTempView`: it gives a DataFrame a name SQL can see during this session.

### 3.3 Joins

**Plain explanation.** A join matches rows from two inputs on a key. An inner join keeps only matches. A left join keeps every row
from the left side. An anti join keeps rows with no match.

**Analogy.** Matching receipts to customer cards by card number. An inner join keeps receipts that have a card.

**Tiny example:**
```python
spark.sql("""
  SELECT n.name, COUNT(*) AS orders
  FROM t_names AS n
  JOIN t_orders AS o ON n.id = o.id
  GROUP BY n.name
""").show()
```
Expected: Alpha 2, Beta 1.

**Fan-out (the key idea of this TODO).** If the right side has two rows per key, every left row is counted twice. Counts then grow.
Ask: "What happens to Alpha's order count if `t_names` had two rows for id 1?" (It doubles.)

### 3.4 `RANK()` versus `ROW_NUMBER()`

**Plain explanation.** Both are window functions. `ROW_NUMBER()` gives each row a unique number, even when values tie. `RANK()` gives
ties the same rank and then skips numbers.

**Tiny example** (inline values):
```python
spark.sql("""
  SELECT name, score,
         RANK() OVER (ORDER BY score DESC) AS r,
         ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
  FROM VALUES ('a', 10), ('b', 10), ('c', 5) AS t(name, score)
""").show()
```
Expected: `a` and `b` both have `r` 1; `c` has `r` 3. For `rn`, the order between `a` and `b` is not guaranteed.

**Where in this repo.** TODO-06 uses `RANK() OVER (ORDER BY delayed_pct DESC)`, as the spec says. The spec does not add a tie-break.

### 3.5 Why aggregate first, then join the dimension

**Plain explanation.** `dim_supplier` has one row per supplier (SCD1, no history). With `supplier_version = 2` it has 25 suppliers,
but SUP025 has no shipments in silver. Two join shapes behave differently:

- A join that starts `FROM dim_supplier` and keeps all dimension rows (a LEFT or FULL join) gives 25 rows with
  `supplier_version = 2`, because SUP025 is kept with empty shipment counts.
- An inner join gives 24 rows either way, because only suppliers with shipments match.

The scorecard should start from the silver aggregate (one row per supplier that has shipments), then join the dimension onto it.
The dimension then only adds names, countries and ratings, and it does not decide the row count.

The stretch TODO-16 creates `dim_supplier_scd2`, where SUP001 has two rows (one expired, one current). A join to that table without
an `is_current` filter would double SUP001's counts. Mention this as a preview only; TODO-06 uses `dim_supplier`.

**Analogy.** Start from the list of cities that have voters, then look up each city's mayor. A city with no voters does not
appear. If you start from the full list of mayors and keep every row, you get extra rows for mayors with no voters.

### 3.6 Where the repo does this already

`gold.build_region_part` (reference) groups silver by `region` and `part_type` with `groupBy` and `agg`. TODO-06 uses the SQL
equivalent. `dims.upsert_dim_supplier` shows the SQL string style with an f-string.

## 4. Step-by-step plan

1. **Run the toy joins in 3.2 to 3.4.** Predict the output first.
   *Check:* the learner predicts the fan-out example correctly.
2. **Inspect the inputs.** In a scratch cell: `spark.table(cfg.table(config.SILVER)).limit(5).show()` and the same for `dim_supplier`.
   *Check:* the learner names `supplier_id` in both and a column in silver used for "delayed".
3. **Write the CTE on paper first:** which columns come from the aggregate, which come from the dimension, which is the join key.
   *Check:* the learner can name the eight output columns from `docs/TODO.md` and where each comes from.
4. **Write `build_supplier_scorecard`** in `gold.py` with `spark.sql` and the f-string table names. Return the DataFrame.
   *Check:* in a scratch cell, `gold.build_supplier_scorecard(spark, cfg.table(config.SILVER), cfg.table(config.DIM_SUPPLIER)).count()` returns 24.
5. **Check the rank.** In a scratch SQL cell, read the top rows of the result ordered by the rank.
   *Check:* rank 1 has the highest `delayed_pct`.
6. **Run `30_gold`.** The TODO-06 cell writes `gold_supplier_scorecard` and displays it.
   *Check:* the table has 24 rows, and the TODO-06 "not done yet" message is gone.
7. **Optional, stretch link:** ask what would change if the dimension had two current rows for one supplier.
   *Check:* the learner says the count doubles.

## 5. Hint ladder per TODO

### TODO-06: supplier scorecard (SQL)

| Level | What you give |
|---|---|
| 0 Goal | "One row per supplier with its volume, delay rate, average unit cost, rating, and a rank by delay rate. 24 rows on version 1." |
| 1 Concept | "Make a named per-supplier summary from silver first. Then attach the supplier's name, country and rating from the dimension. Then rank." |
| 2 Tool | "`WITH name AS (...)` (CTE), `COUNT(*)`, `AVG(...)`, `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` or `AVG(CASE ...)` for the delay rate, `ROUND(..., 2)`, `JOIN ... ON supplier_id`, `RANK() OVER (ORDER BY ... DESC)`. Docs in the TODO." |
| 3 Skeleton | `spark.sql(f"""` / `WITH agg AS (` / `  SELECT ___ AS supplier_id, COUNT(*) AS shipment_count, ___ AS delayed_pct, AVG(___) AS avg_unit_cost_usd` / `  FROM {___}` / `  GROUP BY ___` / `)` / `SELECT d.___, d.___, d.___, d.___, a.___, a.___, a.___, RANK() OVER (ORDER BY ___ DESC) AS delay_rank` / `FROM agg AS a JOIN {___} AS d ON ___` / `""")` |
| 4 Solution | Described only: "aggregate silver per supplier in a CTE, join the dimension on supplier_id from the aggregate side, rank the delay rate descending." The learner writes the delayed-percentage expression and the join condition, and explains each line. |

Variation after level 4 (only if asked): "Rank by `avg_unit_cost_usd` instead. Which suppliers move to the top?"

## 6. Common mistakes and error messages

| Symptom | Cause | What to ask or suggest |
|---|---|---|
| 25 rows instead of 24 | The query starts `FROM dim_supplier` and keeps all dimension rows (LEFT or FULL join), so SUP025 with no shipments appears (see 3.5). | "Which table do you start `FROM`, and what happens to dimension rows with no match?" |
| Counts double after the dimension changes | A second row per supplier in the join target. Not an issue for SCD1, but it is for TODO-16. | "How many rows per `supplier_id` does the dimension have?" |
| `delay_rank` 1 is not the highest rate | Sorted ascending, or the rate is in a different scale. | "Which direction did you sort? Is `delayed_pct` a percentage or a fraction?" |
| Ties get the same rank and the learner is surprised | `RANK()` gives ties equal ranks by design. | Explain 3.4 with the `a`, `b`, `c` example. |
| `[AMBIGUOUS_REFERENCE]` for `supplier_id` (expected wording) | Both tables have `supplier_id`, and the column is not qualified. | "Use the alias: `a.supplier_id` or `d.supplier_id`." |
| `[MISSING_GROUP_BY]` (expected wording) | A non-aggregated column in `SELECT` with an aggregate. | "Which column is not in an aggregate and not in `GROUP BY`?" |
| `[TABLE_OR_VIEW_NOT_FOUND]` for the dimension | The dim table name is wrong, or `20_silver` did not create it. | "Which name does `cfg.table(config.DIM_SUPPLIER)` produce?" |
| `[UNRESOLVED_COLUMN]` for `rating` or `country` | The column is not selected in the CTE, but it lives in the dimension. Or it is missing from the join. | "Which table has that column? Use its alias." |
| Delay rate is 0 or NULL for every supplier | A `CASE` without `ELSE`, or a comparison against the wrong status text. | "Which status text is delayed? Check `STATUSES` in `generator.py`." |
| `NotImplementedError: TODO-06` | The function still raises. | Read the 30_gold cell. The notebook catches it and prints "not done yet". |

## 7. DONE WHEN

From `docs/TODO.md`, copied exactly:

> gold_supplier_scorecard has 24 rows, delay_rank 1 = highest delayed_pct

Note for the tutor: this is a Databricks check (expected, not verified by the scaffold author). Ask the learner to show the query they
use to check rank 1 against the maximum rate.

## 8. Check-your-understanding questions

1. Why aggregate silver before joining the dimension? *Key points: avoids row multiplication; the dimension side must not set the count.*
2. What does a CTE give you that a subquery in the `FROM` does not? *Key points: a name, readable steps; the same idea, mostly style.*
3. What is fan-out, and when does it happen? *Key points: a key with more than one matching row on the other side.*
4. What is the difference between `RANK()` and `ROW_NUMBER()`? *Key points: ties share a rank; ROW_NUMBER is unique.*
5. Why does this repo use SCD1 for the dimension here? *Key points: one row per supplier; history is TODO-16.*
6. Why is `spark.sql` with a full table name safer than a short name? *Key points: the notebook config names the catalog and schema.*
7. How would you check the scorecard in a notebook without re-running everything? *Key points: a scratch SQL query on the gold table.*

## 9. Interview link

- "What is the difference between inner, left and anti joins?" [GENERAL] Junior
- "What are CTEs, and when do they help?" [GENERAL] Junior
- "What is the difference between `RANK`, `DENSE_RANK` and `ROW_NUMBER`?" [PROJECT] Mid
- "How do `GROUP BY`, `HAVING` and `WHERE` differ?" [GENERAL] Junior

Sentence for the learner: "I aggregate silver per supplier in a CTE, then join the dimension, so the join cannot multiply rows, and
I rank the delay rate with `RANK()`."

## 10. Wrap-up

- Tick TODO-06 in `docs/TODO.md` and `README.md` "My implementations", only after the DONE WHEN check passed.
- Commit (the learner runs it): `git add -A && git commit -m "feat(todo-06): supplier scorecard in spark sql"`. Check `git status` first.
- Next stage: `docs/tutor/stage-05-incremental-bronze.md` (TODO-07).

**Say it like this (RU):**

> "CTE — это черновик с именем: сначала считаем итоги по поставщикам, записываем их на стикер, а потом приклеиваем имена
> поставщиков из справочника."

> "Join — это сверка чеков с картами клиентов. Если у одного клиента две карты, каждый чек посчитается дважды. Поэтому сначала
> считаем, потом присоединяем."

> "RANK — это место в таблице лидеров: при одинаковом результате два человека делят первое место, и следующее место — третье."
