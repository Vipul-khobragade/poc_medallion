"""Creates the config tables and seeds the metadata for the two sample datasets.

Re-running overwrites the SEEDED config tables (ingestion_config, silver_table_config,
column_mapping, dq_rules, gold_config). pipeline_run_log is never touched.
"""
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType, IntegerType)

S, B, I = StringType(), BooleanType(), IntegerType()


def _schema(*fields):
    return StructType([StructField(n, t, True) for n, t in fields])


SCHEMAS = {
    "ingestion_config": _schema(
        ("dataset_id", S), ("description", S), ("source_path", S), ("file_format", S), ("multiline", B),
        ("checkpoint_root", S), ("bronze_table", S), ("is_active", B)),
    "silver_table_config": _schema(
        ("dataset_id", S), ("silver_table", S), ("explode_path", S), ("business_keys", S),
        ("order_by_column", S), ("delete_flag_column", S), ("sort_order", I)),
    "column_mapping": _schema(
        ("dataset_id", S), ("silver_table", S), ("ordinal", I), ("source_path", S),
        ("target_column", S), ("data_type", S)),
    "dq_rules": _schema(
        ("dataset_id", S), ("silver_table", S), ("column_name", S), ("rule_type", S),
        ("rule_value", S), ("action", S)),
    "gold_config": _schema(
        ("dataset_id", S), ("gold_table", S), ("source_silver_table", S), ("filter_expression", S),
        ("group_by_columns", S), ("aggregations", S)),
}


def create_schemas_and_log(spark, catalog):
    for s in ("config", "bronze", "silver", "gold", "quarantine"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.config.pipeline_run_log (
            run_id STRING, dataset_id STRING, layer STRING, target_table STRING,
            start_ts TIMESTAMP, end_ts TIMESTAMP,
            rows_read BIGINT, rows_written BIGINT, rows_rejected BIGINT,
            rows_dropped BIGINT, rows_deduped BIGINT,
            status STRING, error_message STRING)""")


# --------------------------------------------------------------------------------------
# Seed content
# --------------------------------------------------------------------------------------
ORDERS_PARENT = [  # (source_path, target_column, data_type)
    ("order_id", "order_id", "string"),
    ("order_ts", "order_ts", "timestamp"),
    ("status", "status", "string"),
    ("updated_at", "updated_at", "timestamp"),
    ("is_deleted", "is_deleted", "boolean"),
    ("customer.id", "customer_id", "string"),
    ("customer.name", "customer_name", "string"),
    ("customer.address.city", "customer_city", "string"),
    ("customer.address.state", "customer_state", "string"),
    ("customer.address.country", "customer_country", "string"),
    ("payment.method", "payment_method", "string"),
    ("payment.amount", "payment_amount", "decimal(12,2)"),
    ("payment.currency", "payment_currency", "string"),
]
ORDERS_ITEMS = [  # '$.x' = field of the exploded array element, plain path = field of the parent record
    ("order_id", "order_id", "string"),
    ("$.sku", "item_sku", "string"),
    ("$.name", "item_name", "string"),
    ("$.qty", "item_qty", "int"),
    ("$.price", "item_price", "decimal(12,2)"),
    ("updated_at", "updated_at", "timestamp"),
    ("is_deleted", "is_deleted", "boolean"),
]
USERS_PARENT = [
    ("user_id", "user_id", "string"),
    ("username", "username", "string"),
    ("email", "email", "string"),
    ("profile.first_name", "first_name", "string"),
    ("profile.last_name", "last_name", "string"),
    ("profile.age", "age", "int"),
    ("profile.address.city", "city", "string"),
    ("profile.address.country", "country", "string"),
    ("preferences.newsletter", "newsletter", "boolean"),
    ("preferences.theme", "theme", "string"),
    ("tags", "tags", "array<string>"),
    ("updated_at", "updated_at", "timestamp"),
]
USERS_LOGINS = [
    ("user_id", "user_id", "string"),
    ("$.ts", "login_ts", "timestamp"),
    ("$.device", "device", "string"),
    ("updated_at", "updated_at", "timestamp"),
]

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def build_seed(landing_root):
    root = landing_root.rstrip("/")
    ckpt = f"{root}/_checkpoints"

    ingestion = [
        ("orders", "Nested orders (JSON Lines): struct + array of items", f"{root}/orders",
         "json", False, ckpt, "bronze.orders_raw", True),
        ("users", "Nested users (multi-line JSON array): struct + map-like prefs + arrays", f"{root}/users",
         "json", True, ckpt, "bronze.users_raw", True),
    ]
    silver_cfg = [
        ("orders", "silver.orders", None, "order_id", "updated_at", "is_deleted", 1),
        ("orders", "silver.orders_items", "items", "order_id,item_sku", "updated_at", "is_deleted", 2),
        ("users", "silver.users", None, "user_id", "updated_at", None, 1),
        ("users", "silver.users_logins", "logins", "user_id,login_ts", "updated_at", None, 2),
    ]
    mapping_sets = {
        ("orders", "silver.orders"): ORDERS_PARENT,
        ("orders", "silver.orders_items"): ORDERS_ITEMS,
        ("users", "silver.users"): USERS_PARENT,
        ("users", "silver.users_logins"): USERS_LOGINS,
    }
    mappings = [(ds, tbl, i + 1, sp, tc, dt)
                for (ds, tbl), rows in mapping_sets.items()
                for i, (sp, tc, dt) in enumerate(rows)]

    dq = [
        ("orders", "silver.orders", "order_id", "not_null", None, "quarantine"),
        ("orders", "silver.orders", "payment_amount", "range", "0,10000000", "quarantine"),
        ("orders", "silver.orders", "status", "allowed_values", "PLACED|SHIPPED|DELIVERED|CANCELLED", "flag"),
        ("orders", "silver.orders_items", "order_id", "not_null", None, "quarantine"),
        ("orders", "silver.orders_items", "item_sku", "not_null", None, "quarantine"),
        ("orders", "silver.orders_items", "item_qty", "range", "1,1000", "quarantine"),
        ("users", "silver.users", "user_id", "not_null", None, "quarantine"),
        ("users", "silver.users", "email", "regex", EMAIL_REGEX, "quarantine"),
        ("users", "silver.users", "age", "range", "0,120", "quarantine"),
        ("users", "silver.users_logins", "user_id", "not_null", None, "quarantine"),
        ("users", "silver.users_logins", "login_ts", "not_null", None, "quarantine"),
    ]
    # expressions are separated by ';' (so that functions with commas are allowed)
    gold = [
        ("orders", "gold.orders_daily_sales", "silver.orders", None,
         "to_date(order_ts) AS order_date",
         "count(*) AS order_count; sum(payment_amount) AS total_sales; round(avg(payment_amount),2) AS avg_order_value"),
        ("orders", "gold.sales_by_sku", "silver.orders_items", None,
         "item_sku; item_name",
         "sum(item_qty) AS units_sold; sum(item_qty * item_price) AS revenue"),
        ("users", "gold.users_by_city", "silver.users", None,
         "city",
         "count(*) AS user_count; round(avg(age),1) AS avg_age; sum(CASE WHEN newsletter THEN 1 ELSE 0 END) AS newsletter_subscribers"),
    ]
    return {"ingestion_config": ingestion, "silver_table_config": silver_cfg, "column_mapping": mappings,
            "dq_rules": dq, "gold_config": gold}


def write_seed(spark, catalog, landing_root):
    for name, rows in build_seed(landing_root).items():
        (spark.createDataFrame(rows, SCHEMAS[name])
              .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
              .saveAsTable(f"{catalog}.config.{name}"))
        print(f"seeded {catalog}.config.{name}: {len(rows)} rows")
