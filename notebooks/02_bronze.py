# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: ADLS JSON -> Delta (Auto Loader, incremental, raw structure kept)

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
from framework.bronze import ingest_bronze
from framework.logger import RunLogger
from framework.utils import new_run_id, set_delta_confs

set_delta_confs(spark)
run_id = run_id or new_run_id()
logger = RunLogger(spark, catalog, run_id)

failures = []
for ds in resolve_datasets(spark, catalog, dataset_ids):
    try:
        ingest_bronze(spark, catalog, ds, run_id, logger)
    except Exception as e:           # keep going so one bad dataset does not block the others
        failures.append(f"{ds['dataset_id']}: {str(e)[:300]}")
if failures:
    raise Exception("Bronze failed for: " + " | ".join(failures))
# COMMAND ----------

# Check: rows now in the Bronze tables
for ds in resolve_datasets(spark, catalog, dataset_ids):
    t = f"{catalog}.{ds['bronze_table']}"
    print(t, "->", spark.table(t).count(), "rows")
    display(spark.table(t).limit(10))
