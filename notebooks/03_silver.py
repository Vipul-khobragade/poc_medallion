# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver: flatten (column mapping) -> data quality -> dedupe -> CDC MERGE

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
from framework.silver import run_silver
from framework.logger import RunLogger
from framework.utils import new_run_id, set_delta_confs

set_delta_confs(spark)
run_id = dbutils.widgets.get("run_id").strip() or new_run_id()
logger = RunLogger(spark, catalog, run_id)

failures = []
for ds in resolve_datasets(spark, catalog, dataset_ids):
    try:
        run_silver(spark, catalog, ds, run_id, logger)
    except Exception as e:
        failures.append(f"{ds['dataset_id']}: {str(e)[:300]}")
if failures:
    raise Exception("Silver failed for: " + " | ".join(failures))
