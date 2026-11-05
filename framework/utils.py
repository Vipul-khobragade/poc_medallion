"""Small shared helpers."""
import uuid
from datetime import datetime


def new_run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]


def split_list(value, sep=","):
    """'a, b ,c' -> ['a','b','c'] (empty/None -> [])."""
    if value is None:
        return []
    return [x.strip() for x in str(value).split(sep) if x.strip()]


def quarantine_table(silver_table: str) -> str:
    """silver.orders -> quarantine.orders_quarantine"""
    return "quarantine." + silver_table.split(".")[-1] + "_quarantine"


def set_delta_confs(spark):
    """Session-level settings the framework relies on (ignored if the platform blocks them)."""
    settings = {
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
    }
    for k, v in settings.items():
        try:
            spark.conf.set(k, v)
        except Exception as e:  # e.g. not settable on some compute types
            print(f"[warn] could not set {k}: {e}")
