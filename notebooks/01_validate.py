# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Validate configuration (first Workflow task)
# MAGIC Fails fast, with a readable message, if metadata is incomplete or the source path is unreachable.

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

from framework.config import resolve_datasets, get_silver_tables, get_mappings, get_gold_configs
from framework.utils import split_list

datasets = resolve_datasets(spark, catalog, dataset_ids)
errors = []
if not datasets:
    errors.append(f"No active dataset matches '{dataset_ids}' in {catalog}.config.ingestion_config")

for ds in datasets:
    ds_id = ds["dataset_id"]
    try:
        files = dbutils.fs.ls(ds["source_path"])
        if not files:
            errors.append(f"[{ds_id}] source path is empty: {ds['source_path']}")
    except Exception as e:
        errors.append(f"[{ds_id}] cannot read source path {ds['source_path']}: {str(e)[:200]}")

    tables = get_silver_tables(spark, catalog, ds_id)
    if not tables:
        errors.append(f"[{ds_id}] no rows in silver_table_config")
    silver_names = set()
    for st in tables:
        silver_names.add(st["silver_table"])
        maps = get_mappings(spark, catalog, ds_id, st["silver_table"])
        if not maps:
            errors.append(f"[{ds_id}] no column_mapping rows for {st['silver_table']}")
            continue
        targets = {m["target_column"] for m in maps}
        for col in split_list(st["business_keys"]) + [c for c in (st["order_by_column"], st["delete_flag_column"]) if c]:
            if col not in targets:
                errors.append(f"[{ds_id}] {st['silver_table']}: '{col}' is not a mapped target column")
    for g in get_gold_configs(spark, catalog, ds_id):
        if g["source_silver_table"] not in silver_names:
            errors.append(f"[{ds_id}] gold table {g['gold_table']} reads unknown silver table {g['source_silver_table']}")

if errors:
    raise Exception("Validation failed:\n - " + "\n - ".join(errors))
print("Validation OK for datasets:", [d["dataset_id"] for d in datasets])
