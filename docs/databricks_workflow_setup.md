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
target_catalog = demo_catalog
target_schema = observability
target_table = databricks_failure_reports
```

For Unity Catalog, use:

```text
target_catalog = demo_catalog
target_schema = observability
target_table = databricks_failure_reports
openai_secret_scope = openai
openai_secret_key = OPENAI_API_KEY
openai_model = gpt-5.1-codex-max
```

Use a different catalog if your workspace does not allow table creation in `demo_catalog`.

Create a Databricks secret for your OpenAI API key before using Codex analysis. For example, create a secret scope named `openai` and store the key as `OPENAI_API_KEY`. If the secret is not configured, the workflow still runs with fallback rules.

## Step 4: Run The Job

Click Run now.

Expected behavior:

1. `failing_pipeline_demo` fails.
2. `ai_failure_investigator` runs because its condition is `At least one failed`.
3. The investigator reads failure context from Databricks task values.
4. If task values are missing, it calls the Databricks Jobs API to fetch failed task output.
5. It calls OpenAI/Codex through the Responses API if an OpenAI secret is configured.
6. If OpenAI is unavailable, it uses fallback rules.
7. It writes a report to a Unity Catalog Delta table.

## Step 5: View The Incident Report

```sql
SELECT *
FROM demo_catalog.observability.databricks_failure_reports
ORDER BY created_at DESC;
```

You can also run `databricks_notebooks/03_view_failure_reports.py` interactively.
