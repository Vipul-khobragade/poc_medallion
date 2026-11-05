# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Finalize: run summary (runs even when earlier tasks failed)

# COMMAND ----------

dbutils.widgets.text("catalog", "poc")
dbutils.widgets.text("run_id", "")
catalog = dbutils.widgets.get("catalog").strip()
run_id = dbutils.widgets.get("run_id").strip()

# COMMAND ----------

log = spark.table(f"{catalog}.config.pipeline_run_log")
if run_id:
    log = log.where(log.run_id == run_id)
else:
    latest = spark.table(f"{catalog}.config.pipeline_run_log").orderBy("end_ts", ascending=False).limit(20)
    log = latest
display(log.orderBy("end_ts"))
failed = log.where("status = 'FAILED'").count()
print(f"Run {run_id or '(latest 20 rows)'}: {failed} failed step(s)")
