# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: schemas, run log and metadata (one-time, safe to re-run)
# MAGIC Edit the two values in cell 2, then Run all.

# COMMAND ----------

import sys, os
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb))
if _root not in sys.path:
    sys.path.insert(0, _root)
print("framework path:", _root)

# COMMAND ----------

# ===== EDIT HERE =====
catalog = "dbw_poc_medallion"
landing_root = "abfss://medallion@medallionarch123.dfs.core.windows.net/landing"
# COMMAND ----------

from framework.seed_metadata import create_schemas_and_log, write_seed

existing = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
if catalog not in existing:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")   # only needed if the catalog does not exist yet

create_schemas_and_log(spark, catalog)
write_seed(spark, catalog, landing_root)
# COMMAND ----------

display(spark.table(f"{catalog}.config.ingestion_config"))
display(spark.table(f"{catalog}.config.column_mapping").orderBy("dataset_id", "silver_table", "ordinal"))
