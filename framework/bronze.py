"""Bronze: raw JSON from ADLS -> Delta using Auto Loader (cloudFiles)."""
from datetime import datetime
from pyspark.sql import functions as F


def _is_schema_evolution_stop(err: Exception) -> bool:
    s = str(err)
    return "UnknownFieldException" in s or "UNKNOWN_FIELD_EXCEPTION" in s


def ingest_bronze(spark, catalog, ds, run_id, logger):
    ds_id = ds["dataset_id"]
    target = f"{catalog}.{ds['bronze_table']}"
    cp = f"{ds['checkpoint_root']}/{ds_id}/bronze"
    schema_loc = f"{ds['checkpoint_root']}/{ds_id}/schema"
    start = datetime.utcnow()

    def attempt():
        raw = (spark.readStream.format("cloudFiles")
               .option("cloudFiles.format", ds["file_format"])
               .option("cloudFiles.schemaLocation", schema_loc)
               .option("cloudFiles.inferColumnTypes", "true")
               .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
               .option("multiLine", str(bool(ds["multiline"])).lower())
               .load(ds["source_path"]))
        df = (raw
              .withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_file_mod_ts", F.col("_metadata.file_modification_time"))
              .withColumn("_run_id", F.lit(run_id))
              .withColumn("_dataset_id", F.lit(ds_id)))
        try:
            before = spark.table(target).count()
        except Exception:
            before = 0
        q = (df.writeStream.format("delta")
             .option("checkpointLocation", cp)
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .toTable(target))
        q.awaitTermination()
        def _rows(p):  # recentProgress items are dicts on classic compute, objects on serverless
            try:
                return p["numInputRows"] or 0
            except Exception:
                return getattr(p, "numInputRows", 0) or 0
        reported = sum(_rows(p) for p in q.recentProgress)
        if reported:
            return reported
        # serverless often reports no progress: use the change in table row count instead
        return max(spark.table(target).count() - before, 0)

    try:
        rows = 0
        for i in (1, 2, 3):
            try:
                rows = attempt()
                break
            except Exception as e:
                # Auto Loader stops once when it discovers new columns, records them in the
                # schema location and succeeds when restarted. We restart automatically.
                if _is_schema_evolution_stop(e) and i < 3:
                    print(f"[bronze] {ds_id}: new columns detected, schema updated - restarting stream")
                    continue
                raise
        logger.log(ds_id, "bronze", target, start, "SUCCESS", rows_read=rows, rows_written=rows)
    except Exception as e:
        logger.log(ds_id, "bronze", target, start, "FAILED", error_message=e)
        raise
