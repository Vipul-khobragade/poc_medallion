"""Gold: metadata-driven aggregations on Silver."""
from datetime import datetime
from pyspark.sql import functions as F
from .utils import split_list


def build_gold(spark, catalog, ds_id, cfg, run_id, logger):
    target = f"{catalog}.{cfg['gold_table']}"
    start = datetime.utcnow()
    try:
        src = spark.table(f"{catalog}.{cfg['source_silver_table']}")
        rows_in = src.count()
        if cfg.get("filter_expression"):
            src = src.where(cfg["filter_expression"])
        group_exprs = [F.expr(x) for x in split_list(cfg["group_by_columns"], ";")]
        agg_exprs = [F.expr(x) for x in split_list(cfg["aggregations"], ";")]
        out = src.groupBy(*group_exprs).agg(*agg_exprs) if group_exprs else src.agg(*agg_exprs)
        out = out.withColumn("_gold_ts", F.current_timestamp())
        (out.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").saveAsTable(target))
        logger.log(ds_id, "gold", target, start, "SUCCESS",
                   rows_read=rows_in, rows_written=spark.table(target).count())
    except Exception as e:
        logger.log(ds_id, "gold", target, start, "FAILED", error_message=e)
        raise
