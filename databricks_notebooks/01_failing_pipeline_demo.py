# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Failing Pipeline Demo
# MAGIC
# MAGIC This notebook intentionally fails with a schema drift error.
# MAGIC The failure gives the AI failure investigator a real Databricks job run to inspect and analyze.

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql.functions import col

# COMMAND ----------

pipeline_name = "customer_silver_pipeline"
source_name = "demo_customers_stream"
target_name = "silver_customers"
expected_column = "customer_email"

customers = spark.createDataFrame(
    [
        (1, "alice@example.com", "2026-05-01T09:00:00Z"),
        (2, "bob@example.com", "2026-05-01T09:05:00Z"),
    ],
    ["customer_id", "email_address", "created_at"],
)

display(customers)

# COMMAND ----------

code_snippet = """
silver_customers = customers.select(
    col("customer_id"),
    col("customer_email"),
    col("created_at"),
)
silver_customers.count()
"""

try:
    # Intentional failure: the source has email_address, but this transformation expects customer_email.
    silver_customers = customers.select(
        col("customer_id"),
        col(expected_column),
        col("created_at"),
    )
    silver_customers.count()
except Exception as exc:
    failure_context = {
        "pipeline_name": pipeline_name,
        "source_name": source_name,
        "target_name": target_name,
        "failure_type_hint": "schema_drift",
        "expected_column": expected_column,
        "actual_columns": customers.columns,
        "error_class": exc.__class__.__name__,
        "error_message": str(exc),
        "notebook_path": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
        "code_snippet": code_snippet.strip(),
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    dbutils.jobs.taskValues.set(
        key="failure_context",
        value=json.dumps(failure_context, ensure_ascii=False),
    )

    raise
