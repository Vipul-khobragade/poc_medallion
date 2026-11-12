"""Silver: Bronze (incremental stream) -> flatten -> DQ -> dedupe -> MERGE."""
from datetime import datetime
from pyspark.sql import functions as F
from .config import get_silver_tables, get_mappings, get_dq_rules
from .flatten import flatten_parent, flatten_child
from .dq import apply_dq
from .merge import dedupe, upsert, quarantine_insert
from .utils import split_list, quarantine_table


def run_silver(spark, catalog, ds, run_id, logger):
    ds_id = ds["dataset_id"]
    tables = get_silver_tables(spark, catalog, ds_id)
    bronze = f"{catalog}.{ds['bronze_table']}"
    cp = f"{ds['checkpoint_root']}/{ds_id}/silver"
    state = {"batches": 0}

    def process_batch(batch_df, batch_id):
        state["batches"] += 1
        sp = batch_df.sparkSession
        for st in tables:
            name = st["silver_table"]
            target = f"{catalog}.{name}"
            start = datetime.utcnow()
            try:
                maps = get_mappings(sp, catalog, ds_id, name)
                flat = (flatten_child(batch_df, maps, st["explode_path"])
                        if st["explode_path"] else flatten_parent(batch_df, maps))
                if flat is None:
                    logger.log(ds_id, "silver", target, start, "SKIPPED",
                               error_message=f"array '{st['explode_path']}' not present in data")
                    continue
                rows_in = flat.count()

                clean, quarantine, dropped = apply_dq(flat, get_dq_rules(sp, catalog, ds_id, name))
                rejected = 0
                if quarantine is not None:
                    rejected = quarantine.count()
                    if rejected > 0:
                        (quarantine.withColumn("_quarantined_ts", F.current_timestamp())
                                   .withColumn("_run_id", F.lit(run_id))
                                   .write.format("delta").mode("append")
                                   .option("mergeSchema", "true")
                                   .saveAsTable(f"{catalog}.{quarantine_table(name)}"))

                keys = split_list(st["business_keys"])
                n_clean = clean.count()
                latest = dedupe(clean, keys, st["order_by_column"])
                n_latest = latest.count()

                final = (latest.withColumn("_silver_ts", F.current_timestamp())
                               .withColumn("_run_id", F.lit(run_id)))
                written = upsert(sp, final, target, keys, st["order_by_column"], st["delete_flag_column"])
                logger.log(ds_id, "silver", target, start, "SUCCESS", rows_read=rows_in,
                           rows_written=written, rows_rejected=rejected, rows_dropped=dropped,
                           rows_deduped=n_clean - n_latest)
            except Exception as e:
                logger.log(ds_id, "silver", target, start, "FAILED", error_message=e)
                raise

    (spark.readStream.table(bronze)
          .writeStream.foreachBatch(process_batch)
          .option("checkpointLocation", cp)
          .trigger(availableNow=True)
          .start().awaitTermination())

    if state["batches"] == 0:  # nothing new in Bronze: still leave an audit trail
        for st in tables:
            logger.log(ds_id, "silver", f"{catalog}.{st['silver_table']}", datetime.utcnow(), "SUCCESS")
