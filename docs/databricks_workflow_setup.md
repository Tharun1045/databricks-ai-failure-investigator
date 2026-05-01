# Databricks Workflow Setup

This project runs inside Databricks. Use VS Code for Git and editing, then pull the repo into Databricks and create the workflow.

## Target Workflow

```text
Databricks Job: AI Failure Investigator Demo

Task 1: failing_pipeline_demo
  Notebook: databricks_notebooks/01_failing_pipeline_demo.py
  Run if: All succeeded
  Expected result: Failed

Task 2: ai_failure_investigator
  Notebook: databricks_notebooks/02_ai_failure_investigator.py
  Depends on: failing_pipeline_demo
  Run if: At least one failed
  Expected result: Success
```

## Step 1: Push Repo To GitHub

```powershell
git add .
git commit -m "Add Databricks native AI failure investigator workflow"
git branch -M main
git remote add origin https://github.com/<your-user>/databricks-ai-failure-investigator.git
git push -u origin main
```

If the remote already exists, skip `git remote add origin`.

## Step 2: Pull Repo Into Databricks

In Databricks:

1. Go to Workspace.
2. Open Repos or Git folders.
3. Click Add repo.
4. Paste your GitHub repo URL.
5. Select the `main` branch.

## Step 3: Create The Job

Go to Jobs & Pipelines and create a new job.

### Task 1

```text
Task name: failing_pipeline_demo
Type: Notebook
Source: Git provider / Workspace repo
Notebook path: databricks_notebooks/01_failing_pipeline_demo.py
Compute: Serverless if available, otherwise a small job cluster
```

### Task 2

```text
Task name: ai_failure_investigator
Type: Notebook
Source: Git provider / Workspace repo
Notebook path: databricks_notebooks/02_ai_failure_investigator.py
Depends on: failing_pipeline_demo
Run if: At least one failed
Compute: Same as Task 1
```

Add notebook parameters:

```text
job_id = {{job.id}}
job_run_id = {{job.run_id}}
job_name = {{job.name}}
failed_task_key = failing_pipeline_demo
target_catalog =
target_schema = observability
target_table = databricks_failure_reports
```

For Unity Catalog, use:

```text
target_catalog = main
target_schema = observability
target_table = databricks_failure_reports
```

Use a different catalog if your workspace does not allow table creation in `main`.

## Step 4: Run The Job

Click Run now.

Expected behavior:

1. `failing_pipeline_demo` fails.
2. `ai_failure_investigator` runs because its condition is `At least one failed`.
3. The investigator reads failure context from Databricks task values.
4. If task values are missing, it calls the Databricks Jobs API to fetch failed task output.
5. It calls `ai_gen()` if available.
6. If `ai_gen()` is unavailable, it uses fallback rules.
7. It writes a report to a Unity Catalog Delta table.

## Step 5: View The Incident Report

```sql
SELECT *
FROM main.observability.databricks_failure_reports
ORDER BY created_at DESC;
```

You can also run `databricks_notebooks/03_view_failure_reports.py` interactively.
