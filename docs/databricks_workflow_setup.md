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

### Task 2 (Choose One)

**Option A: Groq Failure Investigator (Recommended - Free)**
```text
Task name: groq_ai_failure_investigator
Type: Notebook
Source: Git provider / Workspace repo
Notebook path: databricks_notebooks/04_groq_failure_investigator.py
Depends on: failing_pipeline_demo
Run if: At least one failed
Compute: Same as Task 1
```

**Option B: Ollama + OpenAI Investigator**
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

### For Groq Investigator (Option A - Recommended)

```text
# Groq API - Free, no credit card required
# 1. Get API key: https://console.groq.com/
# 2. Store in Databricks:
#    databricks secrets create-scope groq
#    databricks secrets put --scope groq --key GROQ_API_KEY
groq_secret_scope = groq
groq_secret_key = GROQ_API_KEY
groq_model = llama-3.1-70b-versatile
```

### For Ollama + OpenAI Investigator (Option B)

```text
# Ollama - Must be hosted on a cloud VM reachable by Databricks
# IMPORTANT: localhost won't work - Databricks runs in the cloud
ollama_host = http://<your-ollama-server-ip>:11434
ollama_model = qwen3.5:397b-cloud

# OpenAI (fallback)
openai_secret_scope = openai
openai_secret_key = OPENAI_API_KEY
openai_model = gpt-5.1-codex-max
```

Use a different catalog if your workspace does not allow table creation in `demo_catalog`.

## Step 4: Run The Job

Click Run now.

### Expected Behavior (Groq Investigator)

1. `failing_pipeline_demo` fails with a schema/column error.
2. `groq_ai_failure_investigator` runs because its condition is `At least one failed`.
3. The investigator reads failure context from Databricks task values (or Jobs API fallback).
4. It exports the failed notebook source code via Databricks Workspace API.
5. It sends the error context + code to Groq Cloud API (Llama 3.1 70B).
6. Groq returns JSON analysis with root cause, fix, and prevention steps.
7. The investigator writes a structured report to Unity Catalog Delta table.

### Expected Behavior (Ollama + OpenAI Investigator)

Same flow, but uses Ollama (if configured) or OpenAI Codex for analysis.

## Step 5: View The Incident Report

```sql
SELECT *
FROM demo_catalog.observability.databricks_failure_reports
ORDER BY created_at DESC;
```

You can also run `databricks_notebooks/03_view_failure_reports.py` interactively.
