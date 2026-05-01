# Job Parameters

Use these parameters on the `ai_failure_investigator` notebook task.

| Parameter | Value |
| --- | --- |
| `job_id` | `{{job.id}}` |
| `job_run_id` | `{{job.run_id}}` |
| `job_name` | `{{job.name}}` |
| `failed_task_key` | `failing_pipeline_demo` |
| `target_catalog` | `demo_catalog` |
| `target_schema` | `observability` |
| `target_table` | `databricks_failure_reports` |
| `openai_secret_scope` | your Databricks secret scope, for example `openai` |
| `openai_secret_key` | `OPENAI_API_KEY` |
| `openai_model` | `gpt-5.1-codex-max` |

Use another catalog instead of `demo_catalog` if your workspace has a different development catalog.

The job identity must have permission to create the schema/table, or the schema/table must already exist.

If `openai_secret_scope` is blank or the secret cannot be read, the notebook falls back to local rules.
