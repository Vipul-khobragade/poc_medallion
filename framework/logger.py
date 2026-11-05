"""Writes one row per dataset / layer / table to <catalog>.config.pipeline_run_log."""
from datetime import datetime
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType, LongType)

LOG_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("dataset_id", StringType()),
    StructField("layer", StringType()),
    StructField("target_table", StringType()),
    StructField("start_ts", TimestampType()),
    StructField("end_ts", TimestampType()),
    StructField("rows_read", LongType()),
    StructField("rows_written", LongType()),
    StructField("rows_rejected", LongType()),
    StructField("rows_dropped", LongType()),
    StructField("rows_deduped", LongType()),
    StructField("status", StringType()),
    StructField("error_message", StringType()),
])


class RunLogger:
    def __init__(self, spark, catalog, run_id):
        self.spark, self.catalog, self.run_id = spark, catalog, run_id
        self.table = f"{catalog}.config.pipeline_run_log"

    def log(self, dataset_id, layer, target_table, start_ts, status,
            rows_read=0, rows_written=0, rows_rejected=0, rows_dropped=0, rows_deduped=0,
            error_message=None):
        row = (self.run_id, dataset_id, layer, target_table, start_ts, datetime.utcnow(),
               int(rows_read or 0), int(rows_written or 0), int(rows_rejected or 0),
               int(rows_dropped or 0), int(rows_deduped or 0), status,
               (error_message or None) and str(error_message)[:4000])
        (self.spark.createDataFrame([row], LOG_SCHEMA)
             .write.format("delta").mode("append").saveAsTable(self.table))
        print(f"[{layer}] {dataset_id} -> {target_table}: {status} "
              f"(read={rows_read}, written={rows_written}, rejected={rows_rejected}, "
              f"dropped={rows_dropped}, deduped={rows_deduped})")
