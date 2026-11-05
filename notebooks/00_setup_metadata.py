# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: schemas, run log and metadata (one-time, safe to re-run)
# MAGIC Creates the catalog schemas (`config, bronze, silver, gold, quarantine`), the run-log table and seeds the
# MAGIC metadata for the two sample datasets (`orders`, `users`).
# MAGIC
# MAGIC **Widgets:** `catalog` (existing catalog, or one this notebook may create) and `landing_root`
# MAGIC (e.g. `abfss://raw@<storage-account>.dfs.core.windows.net` or a Volume path `/Volumes/<cat>/<schema>/<vol>`).

# COMMAND ----------

# make the shared "framework" package importable (it sits next to the notebooks folder)
import sys, os
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb))
if _root not in sys.path:
    sys.path.insert(0, _root)

# COMMAND ----------

dbutils.widgets.text("catalog", "poc")
dbutils.widgets.text("landing_root", "abfss://raw@<storage-account>.dfs.core.windows.net")
catalog = dbutils.widgets.get("catalog").strip()
landing_root = dbutils.widgets.get("landing_root").strip()
assert "<" not in landing_root, "Set the landing_root widget to your real storage path"

# COMMAND ----------

from framework.seed_metadata import create_schemas_and_log, write_seed

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
except Exception as e:
    print(f"[info] could not create catalog '{catalog}' ({str(e)[:200]}).")
    print("       If it already exists this is fine. Otherwise use an existing catalog (see README troubleshooting).")

create_schemas_and_log(spark, catalog)
write_seed(spark, catalog, landing_root)

# COMMAND ----------

display(spark.table(f"{catalog}.config.ingestion_config"))
display(spark.table(f"{catalog}.config.column_mapping").orderBy("dataset_id", "silver_table", "ordinal"))
