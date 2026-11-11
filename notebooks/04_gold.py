# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: metadata-driven aggregations on Silver

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

from framework.config import resolve_datasets, get_gold_configs
from framework.gold import build_gold
from framework.logger import RunLogger
from framework.utils import new_run_id, set_delta_confs

set_delta_confs(spark)
run_id = run_id or new_run_id()
logger = RunLogger(spark, catalog, run_id)

failures = []
for ds in resolve_datasets(spark, catalog, dataset_ids):
    for cfg in get_gold_configs(spark, catalog, ds["dataset_id"]):
        try:
            build_gold(spark, catalog, ds["dataset_id"], cfg, run_id, logger)
        except Exception as e:
            failures.append(f"{cfg['gold_table']}: {str(e)[:300]}")
if failures:
    raise Exception("Gold failed for: " + " | ".join(failures))
# COMMAND ----------

# Check: show the Gold tables
for ds in resolve_datasets(spark, catalog, dataset_ids):
    for cfg in get_gold_configs(spark, catalog, ds["dataset_id"]):
        print(cfg["gold_table"])
        display(spark.table(f"{catalog}.{cfg['gold_table']}"))
