# Stage 11: Jobs as code (Asset Bundle) and CI tests (TODO-14, TODO-15)

Tutor note: the learner is an absolute beginner. Explain in Russian. Keep code, identifiers and commands in English.

**Hard rule for this stage: this project never deploys.** Use `databricks bundle validate` only. Never write, suggest or run `databricks bundle deploy` or `databricks bundle run`. Never add a deploy step, a Databricks token or any cloud credential to CI. Never push without the learner's own decision.

## 1. Goal

Describe the whole pipeline as a YAML job with ordered tasks (TODO-14), check it with `databricks bundle validate`, and add a pytest job to GitHub Actions that runs the same tests as the learner's laptop (TODO-15).

**Interview sentence the learner should be able to say:** "The job is code in the repo: each task has a notebook, parameters and depends_on, and databricks bundle validate checks it before anything is deployed. CI runs the pytest suite on every push and has no deploy step."

## 2. Before you start

- Stages 00-10 are done. The notebooks 00, 10, 20, 30, 40 run in order.
- Files to open: `databricks.yml`, `resources/shipments_job.yml` (the existing `generate` and `bronze` tasks), `.github/workflows/tests.yml`, `requirements-dev.txt`, `docs/TODO.md#todo-14`, `docs/TODO.md#todo-15`.
- The learner needs a terminal on this Mac and the Databricks CLI. The install link is in `docs/TODO.md` under TODO-14: https://docs.databricks.com/aws/en/dev-tools/cli/install. Follow the macOS option on that page.
- Ask first: "Is the Databricks CLI installed? Run `databricks --version`. Do you have your workspace address ready?"

## 3. Concepts to teach first

### 3.1 Job, task, `depends_on`

- **Plain explanation:** a job is a list of tasks. Each task runs one notebook. `depends_on` says "run me only after these tasks succeeded". Together they form a graph with an order.
- **Analogy:** a recipe. "Boil water" must finish before "add pasta". The recipe card lists each step and what it needs.
- **Where in this repo:** `resources/shipments_job.yml`. The `generate` task has no dependencies. The `bronze` task has `depends_on: generate`. TODO-14 adds silver, gold and quality tasks after it.

### 3.2 YAML basics

YAML is a text format for settings. Its rules are about **indentation** (spaces, not tabs).

- **Key and value:** `name: shipments-pipeline` means "the key `name` has the value `shipments-pipeline`".
- **Map (a group of keys):** indented lines under a key belong to it.
- **List:** each item starts with `- ` (dash and a space). The items are at the same indentation.
- **Comments:** a line starting with `#` is ignored.
- **Quotes:** `"1"` is text. Without quotes, `1` may be read as a number.
- **Special text:** `{{job.run_id}}` and `${var.catalog}` are not YAML features. Databricks reads them later and fills in the value.

**Tiny example (a toy job, not the project's):**
```yaml
name: demo-job
tasks:
  - task_key: first
    notebook_task:
      notebook_path: ../notebooks/some_notebook.py
  - task_key: second
    depends_on:
      - task_key: first
    notebook_task:
      notebook_path: ../notebooks/another_notebook.py
```
Ask: which task runs first, and which key tells you that?

### 3.3 Asset Bundle: `databricks.yml` and `include`

- **Plain explanation:** a bundle is the project described as files: `databricks.yml` is the root, and `include: resources/*.yml` pulls in the job files. `variables` hold defaults like `catalog` and `schema`, used as `${var.catalog}`. `targets` describe environments. This project has one target, `dev`.
- **Analogy:** a shipping manifest. It says what goes in the box and where, and you check the manifest before you ship. This project only checks it.
- **Where in this repo:** `databricks.yml` (validate-only comment at the top) and `resources/shipments_job.yml`.

### 3.4 `validate` versus `deploy`

- **`validate`:** reads the YAML, checks the structure and references, and reports errors. Changes nothing in the workspace.
- **`deploy`:** would create or update resources in the workspace. This project never runs it.
- **Analogy:** `validate` is a building inspector checking the blueprint. `deploy` is the construction crew. You only need the inspector.

### 3.5 The Databricks CLI and authentication

- **Plain explanation:** the CLI is a command-line tool `databricks`. To use it against your workspace, you log in once. The login stores credentials on your machine, not in the repo.
- **Commands (the shape only):** `databricks --version` checks the install. `databricks auth login --host <your-workspace-URL>` starts the login. Copy the workspace URL from your browser's address bar, and never paste a token into a file or a chat.
- **Note:** `validate` may need authentication, and the scaffold author has not verified it on Free Edition. If it asks for a host or login, that is the expected next step.

### 3.6 GitHub Actions: workflow, job, step, runner

- **Workflow:** a YAML file in `.github/workflows/`. It says when to run (`on:`).
- **Job:** a group of steps that runs on one machine, the **runner** (`runs-on: ubuntu-latest`).
- **Step:** one action. `uses:` runs a ready-made action (for example `actions/checkout@v4`). `run:` runs a shell command. `with:` gives inputs to an action. A step's `name:` is only a label.
- **Analogy:** a kitchen order. The workflow is the order ticket. Each job is a station. Each step is one instruction.
- **Where in this repo:** `.github/workflows/tests.yml` has one job, `unittest`, with a matrix of Python versions.

**Tiny example (toy workflow):**
```yaml
name: demo
on: push
jobs:
  hello:
    runs-on: ubuntu-latest
    steps:
      - run: echo "hello from CI"
```

### Say it like this (RU)

- "YAML — это оглавление с отступами. Один лишний пробел — и парсер решит, что ключ принадлежит другому разделу. Поэтому после каждой правки проверяем отступы."
- "`validate` — это инспектор, который проверяет чертёж. `deploy` — это стройка. В этом проекте нужен только инспектор."
- "Job в GitHub Actions — это станция на кухне, step — одна инструкция повару. Сам файл — это заказ, который выполняется, когда пришло событие `push`."

## 4. Step-by-step plan

1. **Install and check the CLI.** Follow the TODO-14 install link. Then run `databricks --version`.
   - check: a version number prints. If not, the CLI is not on the PATH (see mistakes).
2. **Read the existing job.** Open `resources/shipments_job.yml` and find the `bronze` task. Name its three parts: the task key, `depends_on`, and `notebook_task` with `base_parameters`.
   - check: the learner can say why `run_id` is passed as `"{{job.run_id}}"`.
3. **Validate the unchanged project.** From the repo root, run `databricks bundle validate`.
   - check: if it prints `Validation OK!`, the baseline is good. If it asks for authentication, do step 4 first.
4. **Log in (only if needed).** Run `databricks auth login --host <your-workspace-URL>` and follow the browser prompt.
   - check: the next `validate` gets further than the login step.
5. **TODO-14: add the silver, gold and quality tasks.** Copy the shape of the `bronze` task for each one. Each task names the previous task in `depends_on`. Pass `run_id` the same way the existing tasks do. Keep the job on serverless (no cluster block).
   - check: a grep for `task_key` shows the five tasks in order (generate, bronze, silver, gold, quality_checks), and each `depends_on` names a task that exists.
6. **Validate again.** Run `databricks bundle validate`.
   - check: the output ends with `Validation OK!`. If not, read the first error line only.
7. **TODO-15: add a pytest job to CI.** Add a new job next to `unittest`. Do not change the existing job. Use Python 3.12 and Java 17, install `requirements-dev.txt`, and run `pytest -q`.
   - check: the YAML parses (your editor's YAML check, or `python -c` with `yaml` if PyYAML is installed; no installs are needed for this check). Then run the same two commands locally in the `.venv`: `pip install -r requirements-dev.txt` and `pytest -q`.
8. **Confirm there are no deploy steps.** Search the workflow file for "deploy" and "bundle".
   - check: no matches.

## 5. Hint ladder per TODO

### TODO-14 (silver, gold and quality tasks in the job, then `bundle validate`)

- **Level 0 (goal):** The job has the whole pipeline in order. `databricks bundle validate` prints `Validation OK!`. Nothing is deployed.
- **Level 1 (concept):** Each task is a notebook with a name (task key), a parent (depends_on) and parameters. A task starts only after its parents finish.
- **Level 2 (tool):** Copy the shape of the `bronze` task in `resources/shipments_job.yml`. The notebook paths are in `notebooks/`. The docs links for bundle resources and dynamic values are in the TODO.
- **Level 3 (skeleton with blanks):**
  ```yaml
          - task_key: ___
            depends_on:
              - task_key: ___
            notebook_task:
              notebook_path: ___
              base_parameters:
                run_id: "{{job.run_id}}"
  ```
  Repeat the block for each new task. Fill in the blanks yourself: the task keys, the parent for each one, and the notebook path for each.
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "one task per notebook, in pipeline order, each depends on the one before it, each receives run_id, and the job stays on serverless". Then type your own task blocks and check each one with validate. Afterwards give a variation: "Add a `retries` setting to one task. Where in the YAML does it go, and how would you check it validates?" (Hint: look at the bundles resources docs.)

### TODO-15 (pytest job in CI, with Java 17 and Python 3.12)

- **Level 0 (goal):** A new CI job runs `pytest -q` on Python 3.12 with Java 17, and the existing job stays as it is. No deploy.
- **Level 1 (concept):** PySpark needs Java on the machine that runs the tests. CI runs the same commands as your laptop.
- **Level 2 (tool):** Ready-made GitHub actions do the setup work. Questions to answer: Which official action checks out the repository? Which one installs Python? Which one installs Java, and what does it need as input (a distribution name and a version)? Find the Java action through the setup-java link in `docs/TODO.md`. The existing job already shows the checkout and Python pattern.
- **Level 3 (structure only, blanks for everything else):**
  ```yaml
    pyspark-tests:
      runs-on: ___
      steps:
        - ___          # get the repository
        - ___          # Python (version ___)
        - ___          # Java (distribution ___, version ___)
        - name: ___
          run: ___     # install the dev requirements from TODO-15 WHAT
        - name: ___
          run: ___     # the test command from TODO-15 WHAT
  ```
  The commands are in the TODO-15 WHAT section. The version values are in the README ("Run tests locally").
- **Level 4 (solution, only on explicit request after trying):** One sentence: the policy is "a second job next to the existing one, with Java 17 and Python 3.12 installed before the dependencies, the same two commands as locally, and no deploy or credential steps".

## 6. Common mistakes and error messages

| Symptom | Likely cause | What to ask or suggest |
|---|---|---|
| `databricks: command not found` | The CLI is not installed, or the terminal was opened before the install | "Did you open a new terminal? Is the CLI on the PATH?" |
| `mapping values are not allowed here` (PyYAML style message; the CLI may word it differently) | Bad indentation, usually a key placed on the same line as a list item | "Count the spaces before `task_key` and before `depends_on`. Do they line up?" |
| `found character '\t' that cannot start any token` | A tab character was typed | "Replace the tab with spaces. YAML does not accept tabs for indentation." |
| `did not find expected key` | A list item or a key is at the wrong level | "Which block is it inside? Look at the line above the error." |
| `validate` reports an unknown task or depends_on error | A typo in `task_key`, or a parent that does not exist | "Does the name in depends_on match a task_key exactly?" |
| `validate` asks for a host or fails with credentials | Not logged in (`databricks auth login` not run) | "Run the login step. Expected, not verified on Free Edition." |
| The learner types `bundle deploy` or `bundle run` | Forbidden in this project | Stop. Explain: validate only. Ask what they want to achieve. |
| CI shows `Input required and not supplied: distribution` | The setup-java step is missing its distribution input (expected GitHub message, not verified here) | "Read the setup-java README for the input names." |
| CI fails at Spark start with `Java gateway process exited before sending its port number` | Java is not installed or not set up on the runner | "Is the setup-java step before the pytest step? Did it succeed?" |
| The existing `unittest` job goes red | The new job edit touched it | "Diff the file. The existing job must be unchanged." |
| A secret or token is added to the workflow | Credentials do not belong in CI for this project | Stop. Remove it. Ask why it was added. |
| Push happens without a decision | Every push runs `tests.yml` | "Do you want to push now? Decide that yourself." |

## 7. DONE WHEN

Copied from `docs/TODO.md`:

> **DONE WHEN (TODO-14):** `databricks bundle validate` prints "Validation OK!"

> **DONE WHEN (TODO-15):** `act`-free check: YAML valid + same commands pass locally

Tutor check: for TODO-14, run `databricks bundle validate` from the repo root and read the last line. For TODO-15, check the YAML and run `pip install -r requirements-dev.txt` and `pytest -q` locally in `.venv`.

Note: `bundle validate` is not yet verified by the scaffold author, and may need CLI authentication. Expected to work, not verified.

## 8. Check-your-understanding questions

1. What is a task in a job, and what does `depends_on` mean? Good answer: a task is one unit of work (a notebook); `depends_on` says which tasks must succeed first.
2. Why does YAML care about spaces? Good answer: indentation defines which keys belong together.
3. What is the difference between `validate` and `deploy`? Good answer: validate checks the files and changes nothing; deploy changes the workspace. This project only validates.
4. Why is `run_id` passed as `"{{job.run_id}}"`? Good answer: Databricks fills in the real run id at run time, so audit rows can be grouped.
5. What does a GitHub Actions job run on, and what is a step? Good answer: a runner machine; a step is one action or one shell command.
6. Why does the CI job need Java? Good answer: PySpark starts a JVM for local tests.
7. Why must CI have no deploy step here? Good answer: the project never deploys, and CI should not need Databricks credentials.

## 9. Interview link

From `docs/INTERVIEW_PREP.md`:

- **Q:** "What are Databricks Asset Bundles, and what do the commands do?" [GENERAL] Mid
- **Q:** "How do Databricks jobs model tasks, dependencies, retries and parameters?" [PROJECT] Mid
- **Q:** "What are the CI/CD stages for a Databricks bundle project?" [GENERAL] Mid
- **Q:** "How do you pass parameters into notebooks in a job, and how does the project do it?" [PROJECT] Junior
- **Q:** "How would you branch and version a data project in Git?" [GENERAL] Junior

One sentence the learner can use: "The job is YAML in the repo, with explicit depends_on. I validate it with the bundle CLI. CI runs the pytest suite on Java 17 and Python 3.12, and it has no deploy step, because this project never deploys."

## 10. Wrap-up

- Tick `- [ ] Done` under `TODO-14` and `TODO-15` in `docs/TODO.md`.
- Tick `TODO-14 Complete the job (bundle)` and `TODO-15 CI tests job` in the README checklist.
- Suggested commits, one per TODO, run by the learner after checking `git status`:
  ```
  git add -A && git commit -m "feat(todo-14): job tasks for silver, gold and quality checks"
  git add -A && git commit -m "feat(todo-15): pytest job in CI with Java 17 and Python 3.12"
  ```
  Do not push without a deliberate decision. Every push runs the CI workflow.
- **Next stage:** `stage-12-stretch.md` (TODO-16, TODO-17, TODO-18). Optional and longer. The learner will build an SCD2 table, compare join plans, and look at cost data.
