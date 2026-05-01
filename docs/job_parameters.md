# Job Parameters

Use these parameters on the `ai_failure_investigator` notebook task.

| Parameter | Value |
| --- | --- |
| `job_id` | `{{job.id}}` |
| `job_run_id` | `{{job.run_id}}` |
| `job_name` | `{{job.name}}` |
| `failed_task_key` | `failing_pipeline_demo` |
| `target_catalog` | blank, or `main` |
| `target_schema` | `observability` |
| `target_table` | `databricks_failure_reports` |

Use blank `target_catalog` if you are not using Unity Catalog.

Use `main` only if your workspace has Unity Catalog and you have permission to create schemas/tables under the `main` catalog.
