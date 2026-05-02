# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Groq AI Failure Investigator
# MAGIC
# MAGIC This notebook analyzes Databricks job failures using **Groq Cloud API** (free, no credit card required).
# MAGIC
# MAGIC Groq provides ultra-fast inference for Llama, Mixtral, and Gemma models.
# MAGIC
# MAGIC ## Setup
# MAGIC
# MAGIC 1. Get free API key: https://console.groq.com/
# MAGIC 2. Store in Databricks Secrets:
# MAGIC    ```bash
# MAGIC    databricks secrets create-scope groq
# MAGIC    databricks secrets put --scope groq --key GROQ_API_KEY
# MAGIC    ```
# MAGIC 3. Configure workflow parameters:
# MAGIC    ```
# MAGIC    groq_secret_scope = groq
# MAGIC    groq_secret_key = GROQ_API_KEY
# MAGIC    groq_model = llama-3.1-70b-versatile
# MAGIC    ```

# COMMAND ----------

import json
import re
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# COMMAND ----------
# MAGIC %md
# MAGIC ## Notebook Parameters

# COMMAND ----------

dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("job_name", "")
dbutils.widgets.text("failed_task_key", "failing_pipeline_demo")
dbutils.widgets.text("target_catalog", "demo_catalog")
dbutils.widgets.text("target_schema", "observability")
dbutils.widgets.text("target_table", "databricks_failure_reports")
# Groq configuration
dbutils.widgets.text("groq_secret_scope", "groq")
dbutils.widgets.text("groq_secret_key", "GROQ_API_KEY")
dbutils.widgets.text("groq_model", "llama-3.1-70b-versatile")

job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
job_name = dbutils.widgets.get("job_name")
failed_task_key = dbutils.widgets.get("failed_task_key")
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
target_table = dbutils.widgets.get("target_table").strip()
groq_secret_scope = dbutils.widgets.get("groq_secret_scope").strip()
groq_secret_key = dbutils.widgets.get("groq_secret_key").strip()
groq_model = dbutils.widgets.get("groq_model").strip()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Debug Failure Context (for testing without actual failure)

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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def databricks_api_get(path: str, params: dict) -> dict:
    """Call Databricks API to fetch job/task information."""
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_url = context.apiUrl().get().rstrip("/")
    api_token = context.apiToken().get()
    query = urlencode(params)
    request = Request(
        f"{api_url}{path}?{query}",
        headers={"Authorization": f"Bearer {api_token}"},
        method="GET",
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def databricks_api_get_text(path: str, params: dict) -> str:
    """Export notebook source code from Databricks Workspace."""
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_url = context.apiUrl().get().rstrip("/")
    api_token = context.apiToken().get()
    query = urlencode(params)
    request = Request(
        f"{api_url}{path}?{query}",
        headers={"Authorization": f"Bearer {api_token}"},
        method="GET",
    )
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8")


def get_groq_api_key() -> str | None:
    """Retrieve Groq API key from Databricks Secrets."""
    if not groq_secret_scope or not groq_secret_key:
        return None
    try:
        return dbutils.secrets.get(scope=groq_secret_scope, key=groq_secret_key)
    except Exception:
        return None


def export_notebook_source(notebook_path: str) -> str | None:
    """Export the failed notebook's source code for analysis."""
    if not notebook_path:
        return None
    try:
        return databricks_api_get_text(
            "/api/2.0/workspace/export",
            {"path": notebook_path, "format": "SOURCE", "direct_download": "true"},
        )
    except Exception:
        return None


def collect_failure_context_from_jobs_api(root_run_id: str, task_key: str) -> dict:
    """Collect failure context from Databricks Jobs API when task values are unavailable."""
    if not root_run_id:
        raise ValueError("job_run_id parameter is required for Jobs API fallback.")

    run = databricks_api_get(
        "/api/2.1/jobs/runs/get",
        {"run_id": root_run_id, "include_history": "true"},
    )

    failed_task = None
    for task in run.get("tasks", []):
        state = task.get("state", {})
        if task.get("task_key") == task_key or state.get("result_state") == "FAILED":
            failed_task = task
            if task.get("task_key") == task_key:
                break

    if not failed_task:
        raise ValueError(f"No failed task found in job run {root_run_id}.")

    task_run_id = failed_task.get("run_id")
    output = {}
    if task_run_id:
        output = databricks_api_get(
            "/api/2.1/jobs/runs/get-output",
            {"run_id": task_run_id},
        )

    state = failed_task.get("state", {})
    notebook_path = failed_task.get("notebook_task", {}).get("notebook_path")
    error_message = (
        output.get("error")
        or output.get("error_trace")
        or state.get("state_message")
        or run.get("state", {}).get("state_message")
        or "No error output was returned by the Jobs API."
    )

    return {
        "pipeline_name": job_name or run.get("run_name"),
        "source_name": "databricks_jobs_api",
        "target_name": "unknown",
        "failure_type_hint": "unknown",
        "error_class": "DatabricksJobFailure",
        "error_message": str(error_message)[:12000],
        "notebook_path": notebook_path,
        "failed_task_key": failed_task.get("task_key"),
        "task_run_id": task_run_id,
        "job_run_id": root_run_id,
        "job_id": job_id or run.get("job_id"),
        "run_page_url": run.get("run_page_url"),
        "code_snippet": "Source code was not captured before failure. Review the failed notebook path and stack trace.",
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## Collect Failure Context

# COMMAND ----------

try:
    failure_context_raw = dbutils.jobs.taskValues.get(
        taskKey=failed_task_key,
        key="failure_context",
        debugValue=debug_failure_context,
    )
    failure_context = json.loads(failure_context_raw)
    context_source = "task_values"
except Exception as task_value_exc:
    try:
        failure_context = collect_failure_context_from_jobs_api(job_run_id, failed_task_key)
        context_source = f"jobs_api_fallback: {task_value_exc}"
    except Exception as api_exc:
        failure_context = json.loads(debug_failure_context)
        context_source = f"debug_fallback: task_values={task_value_exc}; jobs_api={api_exc}"

notebook_source = export_notebook_source(failure_context.get("notebook_path"))

failure_context["context_source"] = context_source
if notebook_source:
    failure_context["notebook_source_excerpt"] = notebook_source[:12000]

display(failure_context)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Groq AI Analysis Functions

# COMMAND ----------

def build_prompt(context: dict) -> str:
    """Build a detailed prompt for Groq/Llama to analyze the failure."""
    return f"""
You are a Databricks production incident investigator specializing in PySpark notebook jobs and data pipeline failures.

Analyze this failed Databricks pipeline task and return only valid JSON with:
- failure_type
- severity
- root_cause
- evidence
- location
- suggested_fix
- prevention_steps
- confidence

Requirements:
- Read the notebook source excerpt as real pipeline code, not just as a log message.
- Identify the exact broken statement, function, import, table reference, column reference, or library dependency.
- Explain why the code failed in plain engineering language.
- If the traceback contains a notebook command/cell or line number, include it in `location`.
- If the error is Python syntax or import syntax, explain the exact syntax issue.
- Do not suggest `import *` as the primary fix. Prefer explicit imports.
- If a trailing comma import error appears, suggest either removing the comma or using a parenthesized multi-line import.
- If a missing library appears, suggest where to add the dependency for Databricks job compute.
- If a schema/column error appears, compare expected and available columns when present.
- Mention the failed notebook path when available.
- `suggested_fix` should contain the exact replacement code when possible.
- `prevention_steps` should be senior-level operational advice: tests, schema contracts, CI checks, job cluster/library management, or monitoring.

Failure context:
{json.dumps(context, indent=2)}
""".strip()


def analyze_with_groq(context: dict, api_key: str, model: str) -> dict:
    """
    Analyze failure using Groq Cloud API.
    Groq provides ultra-fast inference for Llama, Mixtral, and Gemma models.

    Available models:
    - llama-3.1-70b-versatile (recommended)
    - llama-3.1-8b-instant
    - mixtral-8x7b-32768
    - gemma2-9b-it
    """
    messages = [
        {
            "role": "system",
            "content": (
                "You are a Databricks production incident investigator specializing in PySpark notebook jobs "
                "and data pipeline failures. Analyze the error context and code, then return ONLY valid JSON "
                "with: failure_type, severity, root_cause, evidence (array), location (object with "
                "cell/line/file/near), suggested_fix, prevention_steps (array), confidence (0-1 float). "
                "No markdown, no explanations outside JSON."
            )
        },
        {
            "role": "user",
            "content": build_prompt(context)
        }
    ]

    payload = {
        "model": model,
        "messages": messages,
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
        "max_tokens": 2048,
    }

    request = Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urlopen(request, timeout=90) as response:
        response_payload = json.loads(response.read().decode("utf-8"))

    response_text = response_payload.get("choices", [{}])[0].get("message", {}).get("content", "")
    if not response_text:
        raise ValueError("Groq response did not contain output.")

    return extract_json(response_text)


def extract_json(text: str) -> dict:
    """Extract JSON from response text, handling markdown code blocks."""
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


def extract_failure_location(error_message: str) -> dict:
    """Parse error message to extract file, line, and cell information."""
    location = {
        "cell": None,
        "line": None,
        "file": None,
        "near": None,
    }

    file_line_match = re.search(r'File "([^"]+)", line (\d+)', error_message)
    if file_line_match:
        location["file"] = file_line_match.group(1)
        location["line"] = int(file_line_match.group(2))

    command_match = re.search(r"command-(\d+)", error_message)
    if command_match:
        location["cell"] = f"command-{command_match.group(1)}"

    line_match = re.search(r"line (\d+)", error_message, flags=re.IGNORECASE)
    if not location["line"] and line_match:
        location["line"] = int(line_match.group(1))

    lines = [line.rstrip() for line in error_message.splitlines()]
    for index, line in enumerate(lines):
        if line.strip().startswith(("from ", "import ", "^")):
            window = lines[max(index - 1, 0) : min(index + 2, len(lines))]
            location["near"] = "\n".join(window)
            break

    return location


def fallback_analysis(context: dict) -> dict:
    """
    Fallback analysis when Groq API is unavailable.
    Uses pattern matching on error messages to identify common failure types.
    """
    raw_error_message = context.get("error_message", "")
    error_message = raw_error_message.lower()
    location = extract_failure_location(raw_error_message)

    if "syntaxerror" in error_message:
        failure_type = "python_syntax_error"
        severity = "high"
        root_cause = "The notebook failed with a Python syntax error. Groq was unavailable, so only basic analysis is provided."
        suggested_fix = "Review the failed cell/line in `location_json` and the notebook source excerpt, then fix the invalid Python syntax."
        confidence = 0.55
    elif "modulenotfounderror" in error_message or "no module named" in error_message:
        failure_type = "dependency_error"
        severity = "high"
        root_cause = "The notebook imports a Python package that is not installed on the job compute."
        suggested_fix = "Install the missing library on the Databricks job compute or add it to the workflow's library/dependency configuration."
        confidence = 0.7
    elif "cannot be resolved" in error_message or "unresolved" in error_message:
        failure_type = "schema_or_column_error"
        severity = "high"
        root_cause = "The notebook references a column, field, or expression that Spark could not resolve."
        suggested_fix = "Compare the failing column/expression with the source DataFrame/table schema. Check for typos or schema drift."
        confidence = 0.65
    elif "analysisexception" in error_message:
        failure_type = "schema_or_column_error"
        severity = "high"
        root_cause = "Spark AnalysisException indicates a query/column resolution issue."
        suggested_fix = "Check column names match the source schema. Look for typos, missing aliases, or schema drift."
        confidence = 0.6
    else:
        failure_type = "unknown"
        severity = "medium"
        root_cause = "Groq was unavailable and fallback rules could not identify a specific root cause."
        suggested_fix = "Check `evidence_json`, `location_json`, and `raw_failure_context_json` for the failed notebook path, error message, and source excerpt."
        confidence = 0.25

    return {
        "failure_type": failure_type,
        "severity": severity,
        "root_cause": root_cause,
        "evidence": [raw_error_message],
        "location": location,
        "suggested_fix": suggested_fix,
        "prevention_steps": [
            "Keep Groq API key configured for full AI-powered code analysis.",
            "Add notebook syntax checks before deploying workflow changes.",
            "Review Databricks job failures with the exported notebook source and traceback together.",
            "Implement schema validation at pipeline boundaries.",
        ],
        "confidence": confidence,
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run AI Analysis

# COMMAND ----------

analysis_errors = []
analysis = None
analysis_source = None

# Try Groq API first
groq_api_key = get_groq_api_key()

if groq_api_key:
    try:
        analysis = analyze_with_groq(failure_context, groq_api_key, groq_model)
        analysis_source = "groq_" + groq_model.replace("-", "_")
    except Exception as exc:
        analysis_errors.append(f"groq_error: {exc}")
        analysis = None
        analysis_source = None
else:
    analysis_errors.append("groq_skipped: groq_secret_scope/groq_secret_key not configured")
    analysis = None
    analysis_source = None

# Fallback to rule-based analysis if Groq fails
if analysis is None:
    analysis = fallback_analysis(failure_context)
    analysis_source = "fallback_rules"

if analysis_errors:
    analysis["analysis_errors"] = analysis_errors

display(analysis)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write Incident Report to Delta Table

# COMMAND ----------

report_row = {
    "job_id": str(job_id or ""),
    "job_run_id": str(job_run_id or ""),
    "job_name": str(job_name or ""),
    "failed_task_key": str(failed_task_key or ""),
    "pipeline_name": str(failure_context.get("pipeline_name") or ""),
    "notebook_path": str(failure_context.get("notebook_path") or ""),
    "failure_type": str(analysis.get("failure_type") or ""),
    "severity": str(analysis.get("severity") or ""),
    "root_cause": str(analysis.get("root_cause") or ""),
    "evidence_json": json.dumps(analysis.get("evidence", []), ensure_ascii=False),
    "location_json": json.dumps(analysis.get("location", {}), ensure_ascii=False),
    "suggested_fix": str(analysis.get("suggested_fix") or ""),
    "prevention_steps_json": json.dumps(analysis.get("prevention_steps", []), ensure_ascii=False),
    "confidence": float(analysis.get("confidence", 0.0)),
    "analysis_source": str(analysis_source or ""),
    "ai_model": str(groq_model if analysis_source and analysis_source.startswith("groq") else ""),
    "raw_failure_context_json": json.dumps(failure_context, ensure_ascii=False),
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
}

report_schema = StructType(
    [
        StructField("job_id", StringType(), True),
        StructField("job_run_id", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("failed_task_key", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("notebook_path", StringType(), True),
        StructField("failure_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("root_cause", StringType(), True),
        StructField("evidence_json", StringType(), True),
        StructField("location_json", StringType(), True),
        StructField("suggested_fix", StringType(), True),
        StructField("prevention_steps_json", StringType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("analysis_source", StringType(), True),
        StructField("ai_model", StringType(), True),
        StructField("raw_failure_context_json", StringType(), True),
        StructField("created_at_utc", StringType(), True),
    ]
)

report_df = spark.createDataFrame([report_row], schema=report_schema).withColumn(
    "created_at",
    current_timestamp(),
)

if not target_catalog:
    raise ValueError(
        "target_catalog is required for Unity Catalog mode. "
        "Use `main` or another catalog where the job identity has permission."
    )

table_identifier = f"`{target_catalog}`.`{target_schema}`.`{target_table}`"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`")

report_df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_identifier)

display(report_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Confirmation

# COMMAND ----------

print(f"Saved AI failure report to {table_identifier}")
print(f"Analysis source: {analysis_source}")
print(f"AI Model: {groq_model if analysis_source and analysis_source.startswith('groq') else 'N/A'}")
