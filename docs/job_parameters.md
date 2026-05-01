# Job Parameters

Use these parameters on the `ai_failure_investigator` notebook task.

| Parameter | Value |
| --- | --- |
| `job_id` | `{{job.id}}` |
| `job_run_id` | `{{job.run_id}}` |
| `job_name` | `{{job.name}}` |
| `failed_task_key` | `failing_pipeline_demo` |
| `target_catalog` | `main` |
| `target_schema` | `observability` |
| `target_table` | `databricks_failure_reports` |

Use another catalog instead of `main` if your workspace has a dedicated development catalog.

The job identity must have permission to create the schema/table, or the schema/table must already exist.
