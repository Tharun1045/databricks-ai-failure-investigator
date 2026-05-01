# Databricks AI Failure Investigator

An AI-assisted debugging workflow for failed Databricks jobs and pipelines.

The goal of this project is to help data engineers quickly understand why a Databricks pipeline failed, what code or data issue caused it, and what fix is likely to prevent it from happening again.

## Problem

Databricks job failures are often hard to debug because the useful information is spread across job run metadata, task logs, Spark errors, notebooks, cluster configuration, and recent code changes.

This project creates a Databricks-native failure investigation workflow:

1. A pipeline task fails intentionally.
2. A downstream investigator task runs only when the first task fails.
3. The investigator first tries to read failure context from Databricks task values.
4. If task values were not written, it falls back to the Databricks Jobs API and reads the failed task output.
5. The investigator calls OpenAI/Codex through the Responses API when an OpenAI secret is configured.
6. If OpenAI is unavailable, it uses local fallback rules.
7. The investigator writes a structured incident report to a Unity Catalog Delta table.

## Project Structure

```text
databricks-ai-failure-investigator/
  databricks_notebooks/
    01_failing_pipeline_demo.py
    02_ai_failure_investigator.py
    03_view_failure_reports.py
  docs/
    databricks_workflow_setup.md
    job_parameters.md
  README.md
  .gitignore
```

## Workflow

```text
Task 1: failing_pipeline_demo
  Runs databricks_notebooks/01_failing_pipeline_demo.py
  Expected result: Failed

Task 2: ai_failure_investigator
  Runs databricks_notebooks/02_ai_failure_investigator.py
  Depends on: failing_pipeline_demo
  Run if: At least one failed
  Expected result: Success

Optional viewer:
  databricks_notebooks/03_view_failure_reports.py
```

## How AI Checks The Failure

The AI logic is written in:

```text
databricks_notebooks/02_ai_failure_investigator.py
```

It builds a prompt from the failed task context and calls OpenAI/Codex through the Responses API when configured.

If OpenAI is not configured or the API call fails, the notebook uses local fallback rules so the workflow still writes a useful Delta report.

Your Codex subscription in the app is separate from Databricks. To use OpenAI/Codex inside Databricks, store an OpenAI API key in a Databricks secret and pass its scope/key to the investigator task.

The investigator supports two context paths:

```text
Best path:
Failed notebook catches the exception -> writes failure_context task value -> re-raises error

Fallback path:
Failed notebook crashes before writing task values -> investigator calls Databricks Jobs API -> reads failed task output
```

For import-related failures, the investigator also exports the failed notebook source,
checks which `pyspark.sql.functions` are used, compares them with the actual imports,
records the source lines where each function is used/imported, and stores the result in
`static_code_analysis_json`.

## Setup

Follow:

```text
docs/databricks_workflow_setup.md
```

## Do We Need GitHub Actions?

No, not for the first version.

The first goal is to prove this flow:

```text
VS Code -> GitHub -> Databricks Git folder -> Databricks Job fails -> AI investigator runs -> Unity Catalog Delta report saved
```

Add GitHub Actions later for linting, tests, Databricks Asset Bundle validation, and automated deployment.

## Resume Bullet

Built a Databricks-native AI failure investigation workflow that triggers after failed Lakeflow Job tasks, analyzes Spark errors and code context using Databricks AI Functions, and writes structured incident reports to Unity Catalog Delta tables for observability and root-cause analysis.
