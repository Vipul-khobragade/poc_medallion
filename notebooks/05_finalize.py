# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Finalize: run summary (runs even when earlier tasks failed)

# COMMAND ----------

# ===== EDIT HERE when you run the notebook by hand =====
catalog = "dbw_poc_medallion"
dataset_ids = "ALL"        # orders | users | ALL
run_id = ""                       # blank = generated automatically

# ===== When a Workflow runs this notebook, its parameters override the values above =====
try:
    if dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().isDefined():
        catalog = dbutils.widgets.get("catalog")
        dataset_ids = dbutils.widgets.get("dataset_ids")
        run_id = dbutils.widgets.get("run_id")
except Exception:
    pass
print("Using catalog:", catalog, "| datasets:", dataset_ids)
# COMMAND ----------

log = spark.table(f"{catalog}.config.pipeline_run_log")
if run_id:
    log = log.where(log.run_id == run_id)
else:
    log = log.orderBy("end_ts", ascending=False).limit(20)
display(log.orderBy("end_ts"))
failed = log.where("status = 'FAILED'").count()
print(f"Run {run_id or '(latest 20 rows)'}: {failed} failed step(s)")
