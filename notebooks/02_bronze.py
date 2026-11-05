# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: ADLS JSON -> Delta (Auto Loader, incremental, raw structure kept)

# COMMAND ----------

# make the shared "framework" package importable (it sits next to the notebooks folder)
import sys, os
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb))
if _root not in sys.path:
    sys.path.insert(0, _root)

# COMMAND ----------

dbutils.widgets.text("catalog", "poc")
dbutils.widgets.text("dataset_ids", "ALL")
dbutils.widgets.text("run_id", "")
catalog = dbutils.widgets.get("catalog").strip()
dataset_ids = dbutils.widgets.get("dataset_ids").strip()

# COMMAND ----------

from framework.config import resolve_datasets
from framework.bronze import ingest_bronze
from framework.logger import RunLogger
from framework.utils import new_run_id, set_delta_confs

set_delta_confs(spark)
run_id = dbutils.widgets.get("run_id").strip() or new_run_id()
logger = RunLogger(spark, catalog, run_id)

failures = []
for ds in resolve_datasets(spark, catalog, dataset_ids):
    try:
        ingest_bronze(spark, catalog, ds, run_id, logger)
    except Exception as e:           # keep going so one bad dataset does not block the others
        failures.append(f"{ds['dataset_id']}: {str(e)[:300]}")
if failures:
    raise Exception("Bronze failed for: " + " | ".join(failures))
