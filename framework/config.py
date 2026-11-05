"""Reads the metadata tables. Nothing dataset-specific lives in code."""
from pyspark.sql import functions as F
from .utils import split_list


def _tbl(spark, catalog, name):
    return spark.table(f"{catalog}.config.{name}")


def resolve_datasets(spark, catalog, dataset_ids="ALL"):
    df = _tbl(spark, catalog, "ingestion_config").where("is_active = true")
    if dataset_ids and dataset_ids.strip().upper() != "ALL":
        df = df.where(F.col("dataset_id").isin(split_list(dataset_ids)))
    return [r.asDict() for r in df.orderBy("dataset_id").collect()]


def get_silver_tables(spark, catalog, dataset_id):
    df = (_tbl(spark, catalog, "silver_table_config")
          .where(F.col("dataset_id") == dataset_id).orderBy("sort_order"))
    return [r.asDict() for r in df.collect()]


def get_mappings(spark, catalog, dataset_id, silver_table):
    df = (_tbl(spark, catalog, "column_mapping")
          .where((F.col("dataset_id") == dataset_id) & (F.col("silver_table") == silver_table))
          .orderBy("ordinal"))
    return [r.asDict() for r in df.collect()]


def get_dq_rules(spark, catalog, dataset_id, silver_table):
    df = _tbl(spark, catalog, "dq_rules").where(
        (F.col("dataset_id") == dataset_id) & (F.col("silver_table") == silver_table))
    return [r.asDict() for r in df.collect()]


def get_gold_configs(spark, catalog, dataset_id):
    df = _tbl(spark, catalog, "gold_config").where(F.col("dataset_id") == dataset_id)
    return [r.asDict() for r in df.collect()]
