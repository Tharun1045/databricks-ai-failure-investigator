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

import ast
import json
import re
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("job_name", "")
dbutils.widgets.text("failed_task_key", "failing_pipeline_demo")
dbutils.widgets.text("target_catalog", "main")
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

def databricks_api_get(path: str, params: dict) -> dict:
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


PYSPARK_SQL_FUNCTIONS = {
    "abs", "acos", "approx_count_distinct", "array", "array_contains", "avg",
    "broadcast", "ceil", "coalesce", "col", "collect_list", "collect_set",
    "concat", "concat_ws", "count", "countDistinct", "current_date",
    "current_timestamp", "date_add", "date_format", "date_sub", "datediff",
    "dayofmonth", "dayofweek", "dayofyear", "desc", "element_at", "explode",
    "expr", "first", "floor", "from_json", "hour", "input_file_name", "isnan",
    "isnull", "last", "length", "lit", "lower", "max", "md5", "mean", "min",
    "month", "monotonically_increasing_id", "regexp_extract", "regexp_replace",
    "round", "row_number", "sha2", "size", "split", "stddev", "struct", "sum",
    "to_date", "to_json", "to_timestamp", "trim", "udf", "unix_timestamp",
    "upper", "var_pop", "var_samp", "when", "window", "year",
}


def export_notebook_source(notebook_path: str) -> str | None:
    if not notebook_path:
        return None
    try:
        return databricks_api_get_text(
            "/api/2.0/workspace/export",
            {"path": notebook_path, "format": "SOURCE", "direct_download": "true"},
        )
    except Exception:
        return None


def strip_databricks_magic(source: str) -> str:
    lines = []
    for line in source.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("# MAGIC") or stripped.startswith("# COMMAND"):
            continue
        if stripped.startswith("%"):
            continue
        lines.append(line)
    return "\n".join(lines)


def source_line(source: str, line_number: int | None) -> str | None:
    if not source or not line_number:
        return None
    lines = source.splitlines()
    if 1 <= line_number <= len(lines):
        return lines[line_number - 1].strip()
    return None


def analyze_pyspark_imports(source: str | None) -> dict:
    if not source:
        return {
            "available": False,
            "reason": "Notebook source was not available for static import analysis.",
        }

    clean_source = strip_databricks_magic(source)
    imported_functions = set()
    import_lines = {}
    wildcard_import = False
    used_names = set()
    usage_lines = {}
    syntax_error = None

    try:
        tree = ast.parse(clean_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "pyspark.sql.functions":
                for alias in node.names:
                    if alias.name == "*":
                        wildcard_import = True
                    else:
                        imported_name = alias.asname or alias.name
                        imported_functions.add(imported_name)
                        import_lines[imported_name] = {
                            "line": getattr(node, "lineno", None),
                            "code": source_line(clean_source, getattr(node, "lineno", None)),
                        }
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                used_names.add(node.id)
                if node.id in PYSPARK_SQL_FUNCTIONS:
                    usage_lines.setdefault(node.id, []).append(
                        {
                            "line": getattr(node, "lineno", None),
                            "code": source_line(clean_source, getattr(node, "lineno", None)),
                        }
                    )
    except SyntaxError as exc:
        syntax_error = {
            "line": exc.lineno,
            "offset": exc.offset,
            "text": exc.text.strip() if exc.text else None,
            "message": exc.msg,
        }
        for match in re.finditer(r"\b([A-Za-z_]\w*)\s*\(", clean_source):
            function_name = match.group(1)
            used_names.add(function_name)
            if function_name in PYSPARK_SQL_FUNCTIONS:
                line_number = clean_source[: match.start()].count("\n") + 1
                usage_lines.setdefault(function_name, []).append(
                    {
                        "line": line_number,
                        "code": source_line(clean_source, line_number),
                    }
                )
        for match in re.finditer(r"from\s+pyspark\.sql\.functions\s+import\s+([^\n]*)", clean_source):
            imported_part = match.group(1).strip()
            import_line = clean_source[: match.start()].count("\n") + 1
            if imported_part == "*":
                wildcard_import = True
            elif imported_part:
                for name in imported_part.split(","):
                    if name.strip():
                        imported_name = name.strip().split(" as ")[-1]
                        imported_functions.add(imported_name)
                        import_lines[imported_name] = {
                            "line": import_line,
                            "code": source_line(clean_source, import_line),
                        }

    used_pyspark_functions = sorted(used_names.intersection(PYSPARK_SQL_FUNCTIONS))
    missing_imports = [] if wildcard_import else sorted(set(used_pyspark_functions) - imported_functions)
    missing_import_details = {
        function_name: usage_lines.get(function_name, [])
        for function_name in missing_imports
    }

    return {
        "available": True,
        "syntax_error": syntax_error,
        "imported_pyspark_sql_functions": sorted(imported_functions),
        "import_lines": import_lines,
        "uses_wildcard_import": wildcard_import,
        "used_pyspark_sql_functions": used_pyspark_functions,
        "usage_lines": usage_lines,
        "missing_pyspark_sql_function_imports": missing_imports,
        "missing_import_details": missing_import_details,
        "recommended_import_statement": (
            "from pyspark.sql.functions import " + ", ".join(missing_imports)
            if missing_imports
            else None
        ),
    }


def collect_failure_context_from_jobs_api(root_run_id: str, task_key: str) -> dict:
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
static_code_analysis = analyze_pyspark_imports(notebook_source)

failure_context["context_source"] = context_source
failure_context["static_code_analysis"] = static_code_analysis
if notebook_source:
    failure_context["notebook_source_excerpt"] = notebook_source[:12000]
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
- location
- suggested_fix
- prevention_steps
- confidence

Be specific and practical.

Requirements:
- If the traceback contains a notebook command/cell or line number, include it in `location`.
- Use `static_code_analysis` to compare PySpark SQL functions used in the notebook with functions imported from `pyspark.sql.functions`.
- If `static_code_analysis.missing_pyspark_sql_function_imports` is not empty, mention the exact missing imports and use `recommended_import_statement` in `suggested_fix`.
- Use `static_code_analysis.missing_import_details` to mention the exact source line where each missing function is used.
- Use `static_code_analysis.import_lines` to mention the existing import line that should be updated when available.
- If the error is Python syntax or import syntax, explain the exact syntax issue.
- If `static_code_analysis.syntax_error` is present, include its line, offset, text, and message in `location`.
- Do not suggest `import *` as the primary fix. Prefer explicit imports.
- Mention the failed notebook path when available.
- `suggested_fix` should contain the exact replacement code when possible.

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


def extract_failure_location(error_message: str) -> dict:
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
    raw_error_message = context.get("error_message", "")
    error_message = raw_error_message.lower()
    location = extract_failure_location(raw_error_message)

    static_analysis = context.get("static_code_analysis", {})
    missing_imports = static_analysis.get("missing_pyspark_sql_function_imports", []) or []
    missing_import_details = static_analysis.get("missing_import_details", {}) or {}
    import_lines = static_analysis.get("import_lines", {}) or {}
    syntax_error = static_analysis.get("syntax_error")
    recommended_import = static_analysis.get("recommended_import_statement")
    if syntax_error:
        location["syntax_error"] = syntax_error

    missing_usage_summary = []
    for function_name, usages in missing_import_details.items():
        first_usage = usages[0] if usages else {}
        line = first_usage.get("line")
        code = first_usage.get("code")
        missing_usage_summary.append(
            f"`{function_name}` is used on line {line}: {code}"
            if line and code
            else f"`{function_name}` is used in the notebook"
        )

    existing_import_summary = []
    for function_name, import_info in import_lines.items():
        line = import_info.get("line")
        code = import_info.get("code")
        if line and code:
            existing_import_summary.append(f"line {line}: {code}")

    if re.search(r"from\s+pyspark\.sql\.functions\s+import\s*(\n|$)", raw_error_message):
        failure_type = "python_syntax_error"
        root_cause = (
            "The notebook has an incomplete import statement: "
            "`from pyspark.sql.functions import` is missing the function names to import."
        )
        suggested_fix = (
            "Replace the incomplete import with an explicit import based on functions used in the notebook:\n\n"
            f"```python\n{recommended_import or 'from pyspark.sql.functions import col'}\n```\n\n"
            f"Detected missing PySpark SQL function imports: {missing_imports or ['col']}. "
            f"Usage evidence: {missing_usage_summary or ['function usage could not be located because parsing stopped at syntax error']}."
        )
        prevention_steps = [
            "Run the notebook from the first cell after editing imports.",
            "Use explicit imports such as `from pyspark.sql.functions import col, current_timestamp`.",
            "Add a lightweight syntax check in CI before syncing notebooks to Databricks.",
        ]
        confidence = 0.94
    elif "syntaxerror" in error_message and "import" in error_message:
        failure_type = "python_syntax_error"
        root_cause = "The notebook failed because of invalid Python import syntax."
        suggested_fix = (
            "Check the import statement near the reported line/cell and replace it with valid Python syntax. "
            "For Spark functions, prefer explicit imports such as:\n\n"
            "```python\n"
            "from pyspark.sql.functions import col\n"
            "```"
        )
        prevention_steps = [
            "Keep markdown text in `%md` cells only.",
            "Run a syntax check before committing notebook source files.",
            "Prefer small import cells so syntax failures are easy to locate.",
        ]
        confidence = 0.85
    elif "nameerror" in error_message and "is not defined" in error_message and missing_imports:
        failure_type = "missing_import"
        root_cause = (
            "The notebook uses PySpark SQL functions that are not imported from `pyspark.sql.functions`: "
            f"{missing_imports}. Usage evidence: {missing_usage_summary}."
        )
        suggested_fix = (
            "Add or update the Spark function import near the top of the notebook:\n\n"
            f"```python\n{recommended_import}\n```\n\n"
            f"Existing Spark import lines found: {existing_import_summary or 'none'}."
        )
        prevention_steps = [
            "Group all Spark function imports in the first Python cell.",
            "Avoid relying on previous interactive notebook state.",
            "Restart and run all cells before promoting notebook changes.",
        ]
        confidence = 0.92
    elif "modulenotfounderror" in error_message or "no module named" in error_message:
        failure_type = "dependency_error"
        root_cause = "The notebook imports a Python package that is not installed on the job compute."
        suggested_fix = (
            "Install the missing library on the job compute, add it as a Databricks job library, "
            "or package it with your deployment configuration."
        )
        prevention_steps = [
            "Pin Python dependencies for job clusters.",
            "Document required libraries for every production workflow.",
            "Run dependency validation before scheduled jobs execute.",
        ]
        confidence = 0.88
    elif "cannot be resolved" in error_message or "unresolved" in error_message:
        failure_type = "schema_drift"
        root_cause = (
            f"The transformation expects `{context.get('expected_column')}`, "
            f"but the source columns are {context.get('actual_columns')}."
        )
        suggested_fix = (
            "Update the silver transformation to map `email_address` to `customer_email`, "
            "or add schema evolution/contract handling before the select step."
        )
        prevention_steps = [
            "Add schema contract checks before silver transformations.",
            "Write rejected or incompatible records to a quarantine table.",
            "Alert pipeline owners when upstream schemas change.",
        ]
        confidence = 0.86
    else:
        failure_type = "unknown"
        root_cause = "The available context is not enough to identify one clear root cause."
        suggested_fix = "Collect full task output, source schema, and recent code changes."
        prevention_steps = [
            "Persist historical failure reports for pattern analysis.",
            "Add richer logging around each pipeline stage.",
            "Capture notebook source snippets around failed lines when possible.",
        ]
        confidence = 0.35

    return {
        "failure_type": failure_type,
        "severity": "high" if failure_type in {"schema_drift", "python_syntax_error"} else "medium",
        "root_cause": root_cause,
        "evidence": [raw_error_message],
        "location": location,
        "suggested_fix": suggested_fix,
        "prevention_steps": prevention_steps,
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
    "location_json": json.dumps(analysis.get("location", {}), ensure_ascii=False),
    "static_code_analysis_json": json.dumps(static_code_analysis, ensure_ascii=False),
    "suggested_fix": analysis.get("suggested_fix"),
    "prevention_steps_json": json.dumps(analysis.get("prevention_steps", []), ensure_ascii=False),
    "confidence": float(analysis.get("confidence", 0.0)),
    "analysis_source": analysis_source,
    "raw_failure_context_json": json.dumps(failure_context, ensure_ascii=False),
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
}

report_df = spark.createDataFrame([Row(**report_row)]).withColumn("created_at", current_timestamp())

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

print(f"Saved AI failure report to {table_identifier}")





