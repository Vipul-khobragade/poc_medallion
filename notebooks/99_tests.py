# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Test scenarios with expected results
# MAGIC Run the pipeline (02 -> 03 -> 04, or the Workflow), then run this notebook with the matching `stage`.
# MAGIC Every check prints PASS/FAIL; the notebook fails if any check fails.

# COMMAND ----------

dbutils.widgets.text("catalog", "poc")
dbutils.widgets.dropdown("stage", "after_orders_batch1",
    ["after_orders_batch1", "after_orders_batch2", "after_users_batch1", "after_users_batch2", "rerun_orders", "rerun_users"])
catalog = dbutils.widgets.get("catalog").strip()
stage = dbutils.widgets.get("stage")

# COMMAND ----------

results = []

def cnt(table):
    return spark.table(f"{catalog}.{table}").count()

def check(name, actual, expected):
    ok = actual == expected
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + f"{name}: actual={actual} expected={expected}")

def scalar(sql):
    return spark.sql(sql).collect()[0][0]

def last_bronze_rows_read(dataset):
    return scalar(f"SELECT rows_read FROM {catalog}.config.pipeline_run_log "
                  f"WHERE dataset_id='{dataset}' AND layer='bronze' ORDER BY end_ts DESC LIMIT 1")

# COMMAND ----------

if stage == "after_orders_batch1":
    check("bronze rows (6 raw records incl. 2 bad)", cnt("bronze.orders_raw"), 6)
    check("silver.orders (4 valid)", cnt("silver.orders"), 4)
    check("quarantine orders (null id + negative amount)", cnt("quarantine.orders_quarantine"), 2)
    check("silver.orders_items (flattened array)", cnt("silver.orders_items"), 6)
    check("quarantine items (item of the order without id)", cnt("quarantine.orders_items_quarantine"), 1)
    check("gold.orders_daily_sales rows (2 days)", cnt("gold.orders_daily_sales"), 2)
    check("gold total_sales", round(float(scalar(f"select sum(total_sales) from {catalog}.gold.orders_daily_sales")), 2), 5899.50)

elif stage == "after_orders_batch2":
    check("bronze rows (6 + 6)", cnt("bronze.orders_raw"), 12)
    check("schema evolution: coupon_code column in Bronze", "coupon_code" in spark.table(f"{catalog}.bronze.orders_raw").columns, True)
    check("silver.orders (O1003 deleted, O1006/O1007 added)", cnt("silver.orders"), 5)
    check("no duplicate order_id in Silver", scalar(f"select count(*) - count(distinct order_id) from {catalog}.silver.orders"), 0)
    check("O1001 updated to SHIPPED", scalar(f"select status from {catalog}.silver.orders where order_id='O1001'"), "SHIPPED")
    check("O1002 stale CANCELLED ignored", scalar(f"select status from {catalog}.silver.orders where order_id='O1002'"), "PLACED")
    check("O1003 deleted (soft-delete flag applied as DELETE)", scalar(f"select count(*) from {catalog}.silver.orders where order_id='O1003'"), 0)
    check("O1003 child rows deleted too", scalar(f"select count(*) from {catalog}.silver.orders_items where order_id='O1003'"), 0)
    check("O1007 duplicate resolved to latest (SHIPPED)", scalar(f"select status from {catalog}.silver.orders where order_id='O1007'"), "SHIPPED")
    check("silver.orders_items", cnt("silver.orders_items"), 6)
    check("quarantine unchanged (orders)", cnt("quarantine.orders_quarantine"), 2)
    check("gold.orders_daily_sales rows (3 days)", cnt("gold.orders_daily_sales"), 3)
    check("gold total_sales", round(float(scalar(f"select sum(total_sales) from {catalog}.gold.orders_daily_sales")), 2), 7200.50)

elif stage == "after_users_batch1":
    check("bronze rows", cnt("bronze.users_raw"), 4)
    check("silver.users (U004 invalid email + age)", cnt("silver.users"), 3)
    check("quarantine users", cnt("quarantine.users_quarantine"), 1)
    check("silver.users_logins (array exploded)", cnt("silver.users_logins"), 3)
    check("gold.users_by_city rows", cnt("gold.users_by_city"), 3)

elif stage == "after_users_batch2":
    check("bronze rows (4 + 2)", cnt("bronze.users_raw"), 6)
    check("nested schema evolution: profile.phone in Bronze",
          "phone" in spark.table(f"{catalog}.bronze.users_raw").schema["profile"].dataType.fieldNames(), True)
    check("silver.users", cnt("silver.users"), 4)
    check("U002 email updated", scalar(f"select email from {catalog}.silver.users where user_id='U002'"), "ravi.kumar@example.com")
    check("silver.users_logins (3 + 2 new)", cnt("silver.users_logins"), 5)
    check("quarantine users unchanged", cnt("quarantine.users_quarantine"), 1)
    check("gold.users_by_city rows", cnt("gold.users_by_city"), 4)

elif stage in ("rerun_orders", "rerun_users"):
    ds = stage.split("_")[1]
    check(f"re-run with no new files reads 0 rows in Bronze ({ds})", last_bronze_rows_read(ds), 0)

assert all(results), "One or more checks FAILED - see output above"
print(f"All {len(results)} checks passed for stage '{stage}'")
