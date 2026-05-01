# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - View Failure Reports
# MAGIC
# MAGIC Use this notebook to inspect generated Databricks failure investigation reports.

# COMMAND ----------

dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("target_schema", "observability")
dbutils.widgets.text("target_table", "databricks_failure_reports")

target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
target_table = dbutils.widgets.get("target_table").strip()

if target_catalog:
    table_identifier = f"`{target_catalog}`.`{target_schema}`.`{target_table}`"
else:
    table_identifier = f"`{target_schema}`.`{target_table}`"

reports = spark.table(table_identifier).orderBy("created_at", ascending=False)
display(reports)

# COMMAND ----------

display(
    reports.select(
        "created_at",
        "job_name",
        "job_run_id",
        "failed_task_key",
        "failure_type",
        "severity",
        "confidence",
        "root_cause",
        "suggested_fix",
        "analysis_source",
    )
)
