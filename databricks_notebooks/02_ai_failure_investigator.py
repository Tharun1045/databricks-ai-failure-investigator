# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - AI Failure Investigator
# MAGIC
# MAGIC This notebook is designed to run after `01_failing_pipeline_demo` fails.
# MAGIC
# MAGIC Configure this task in Databricks Workflows with:
# MAGIC
# MAGIC - **Depends on:** `failing_pipeline_demo`
# MAGIC - **Run if:** `At least one failed`
# MAGIC
# MAGIC It reads failure context from Databricks task values, analyzes the error with Databricks `ai_gen()` when available, and writes an incident report to Delta.

# COMMAND ----------

import json
import re
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("job_name", "")
dbutils.widgets.text("failed_task_key", "failing_pipeline_demo")
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("target_schema", "observability")
dbutils.widgets.text("target_table", "databricks_failure_reports")

job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
job_name = dbutils.widgets.get("job_name")
failed_task_key = dbutils.widgets.get("failed_task_key")
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
target_table = dbutils.widgets.get("target_table").strip()

# COMMAND ----------

debug_failure_context = json.dumps(
    {
        "pipeline_name": "customer_silver_pipeline",
        "source_name": "demo_customers_stream",
        "target_name": "silver_customers",
        "failure_type_hint": "schema_drift",
        "expected_column": "customer_email",
        "actual_columns": ["customer_id", "email_address", "created_at"],
        "error_class": "AnalysisException",
        "error_message": "Column `customer_email` cannot be resolved. Did you mean `email_address`?",
        "notebook_path": "/Repos/example/databricks-ai-failure-investigator/databricks_notebooks/01_failing_pipeline_demo",
        "code_snippet": "customers.select(col('customer_id'), col('customer_email'), col('created_at'))",
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }
)

failure_context_raw = dbutils.jobs.taskValues.get(
    taskKey=failed_task_key,
    key="failure_context",
    debugValue=debug_failure_context,
)

failure_context = json.loads(failure_context_raw)
failure_context

# COMMAND ----------

def build_prompt(context: dict) -> str:
    return f"""
You are a senior Databricks data platform engineer.

Analyze this failed Databricks pipeline task and return only valid JSON with:
- failure_type
- severity
- root_cause
- evidence
- suggested_fix
- prevention_steps
- confidence

Focus on practical Databricks/Spark fixes.

Failure context:
{json.dumps(context, indent=2)}
""".strip()


def extract_json(text: str) -> dict:
    cleaned = text.strip()
    cleaned = re.sub(r"^```json\s*", "", cleaned)
    cleaned = re.sub(r"^```\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if match:
            return json.loads(match.group(0))
        raise


def fallback_analysis(context: dict) -> dict:
    error_message = context.get("error_message", "").lower()

    if "cannot be resolved" in error_message or "unresolved" in error_message:
        failure_type = "schema_drift"
        root_cause = (
            f"The transformation expects `{context.get('expected_column')}`, "
            f"but the source columns are {context.get('actual_columns')}."
        )
        suggested_fix = (
            "Update the silver transformation to map `email_address` to `customer_email`, "
            "or add schema evolution/contract handling before the select step."
        )
        confidence = 0.86
    else:
        failure_type = "unknown"
        root_cause = "The available context is not enough to identify one clear root cause."
        suggested_fix = "Collect full task output, source schema, and recent code changes."
        confidence = 0.35

    return {
        "failure_type": failure_type,
        "severity": "high" if failure_type == "schema_drift" else "medium",
        "root_cause": root_cause,
        "evidence": [context.get("error_message", "")],
        "suggested_fix": suggested_fix,
        "prevention_steps": [
            "Add schema contract checks before silver transformations.",
            "Write rejected or incompatible records to a quarantine table.",
            "Alert pipeline owners when upstream schemas change.",
        ],
        "confidence": confidence,
    }


prompt = build_prompt(failure_context)

# COMMAND ----------

analysis_source = "databricks_ai_gen"

try:
    prompt_df = spark.createDataFrame([Row(prompt=prompt)])
    prompt_df.createOrReplaceTempView("failure_investigation_prompt")

    ai_text = spark.sql(
        """
        SELECT ai_gen(prompt) AS analysis_json
        FROM failure_investigation_prompt
        """
    ).collect()[0]["analysis_json"]

    analysis = extract_json(ai_text)
except Exception as exc:
    analysis_source = "fallback_rules"
    analysis = fallback_analysis(failure_context)
    analysis["ai_error"] = str(exc)

analysis

# COMMAND ----------

report_row = {
    "job_id": job_id,
    "job_run_id": job_run_id,
    "job_name": job_name,
    "failed_task_key": failed_task_key,
    "pipeline_name": failure_context.get("pipeline_name"),
    "notebook_path": failure_context.get("notebook_path"),
    "failure_type": analysis.get("failure_type"),
    "severity": analysis.get("severity"),
    "root_cause": analysis.get("root_cause"),
    "evidence_json": json.dumps(analysis.get("evidence", []), ensure_ascii=False),
    "suggested_fix": analysis.get("suggested_fix"),
    "prevention_steps_json": json.dumps(analysis.get("prevention_steps", []), ensure_ascii=False),
    "confidence": float(analysis.get("confidence", 0.0)),
    "analysis_source": analysis_source,
    "raw_failure_context_json": json.dumps(failure_context, ensure_ascii=False),
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
}

report_df = spark.createDataFrame([Row(**report_row)]).withColumn("created_at", current_timestamp())

if target_catalog:
    table_identifier = f"`{target_catalog}`.`{target_schema}`.`{target_table}`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`")
else:
    table_identifier = f"`{target_schema}`.`{target_table}`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_schema}`")

report_df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_identifier)

display(report_df)

# COMMAND ----------

print(f"Saved AI failure report to {table_identifier}")
