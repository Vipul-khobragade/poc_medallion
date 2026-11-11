# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver: flatten (column mapping) -> data quality -> dedupe -> CDC MERGE

# COMMAND ----------

import sys, os
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb))
if _root not in sys.path:
    sys.path.insert(0, _root)
print("framework path:", _root)

# COMMAND ----------

# ===== EDIT HERE when you run the notebook by hand =====
catalog = "dbw_poc_medallion"
dataset_ids = "orders"        # orders | users | ALL
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

from framework.config import resolve_datasets
from framework.silver import run_silver
from framework.logger import RunLogger
from framework.utils import new_run_id, set_delta_confs

set_delta_confs(spark)
run_id = run_id or new_run_id()
logger = RunLogger(spark, catalog, run_id)

failures = []
for ds in resolve_datasets(spark, catalog, dataset_ids):
    try:
        run_silver(spark, catalog, ds, run_id, logger)
    except Exception as e:
        failures.append(f"{ds['dataset_id']}: {str(e)[:300]}")
if failures:
    raise Exception("Silver failed for: " + " | ".join(failures))
# COMMAND ----------

# Check: row counts and the run log
from framework.config import get_silver_tables
from framework.utils import quarantine_table
for ds in resolve_datasets(spark, catalog, dataset_ids):
    for st in get_silver_tables(spark, catalog, ds["dataset_id"]):
        for t in (st["silver_table"], quarantine_table(st["silver_table"])):
            try:
                print(t, "->", spark.table(f"{catalog}.{t}").count(), "rows")
            except Exception:
                print(t, "-> (does not exist - nothing was written)")
display(spark.table(f"{catalog}.config.pipeline_run_log").orderBy("end_ts", ascending=False).limit(10))
