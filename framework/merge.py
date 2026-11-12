"""CDC-style upsert into Silver: dedupe inside the batch, then MERGE (plain SQL, works on serverless)."""
from pyspark.sql import functions as F, Window


def dedupe(df, keys, order_by):
    """Keep only the latest record per business key (order_by desc, then ingest time desc)."""
    order_cols = []
    if order_by:
        order_cols.append(F.col(order_by).desc_nulls_last())
    if "_ingest_ts" in df.columns:
        order_cols.append(F.col("_ingest_ts").desc())
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def upsert(spark, df, target, keys, order_by, delete_col):
    """
    * first run  -> create the table from the batch (deleted rows are not inserted)
    * later runs -> MERGE on business keys:
        - matched + delete flag true (and not stale) -> DELETE
        - matched + not stale                         -> UPDATE
        - not matched + not deleted                   -> INSERT
      'Not stale' = order_by column is >= the value already in Silver, so
      late-arriving / out-of-order records never overwrite newer data.
    Returns number of rows written (inserted + updated + deleted).
    """
    if not spark.catalog.tableExists(target):
        base = df.filter(f"NOT coalesce(`{delete_col}`, false)") if delete_col else df
        base.write.format("delta").saveAsTable(target)
        return spark.table(target).count()

    view = "_src_" + target.replace(".", "_").replace("-", "_").replace("`", "")
    df.createOrReplaceTempView(view)

    on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    fresh = (f"(t.`{order_by}` IS NULL OR s.`{order_by}` >= t.`{order_by}`)" if order_by else "true")
    if delete_col:
        is_del = f"coalesce(s.`{delete_col}`, false)"
        clauses = [
            f"WHEN MATCHED AND {is_del} = true AND {fresh} THEN DELETE",
            f"WHEN MATCHED AND {is_del} = false AND {fresh} THEN UPDATE SET *",
            f"WHEN NOT MATCHED AND {is_del} = false THEN INSERT *",
        ]
    else:
        clauses = [f"WHEN MATCHED AND {fresh} THEN UPDATE SET *", "WHEN NOT MATCHED THEN INSERT *"]

    result = spark.sql(f"MERGE INTO {target} AS t USING {view} AS s ON {on} " + " ".join(clauses))
    try:
        r = result.collect()[0].asDict()
        return sum(int(r.get(k, 0) or 0) for k in ("num_inserted_rows", "num_updated_rows", "num_deleted_rows"))
    except Exception:
        return 0


def quarantine_insert(spark, df, target):
    """
    Idempotent quarantine write: the same rejected record is never stored twice.
    Each rejected row gets _dq_row_id = hash of all its values (including source file and
    ingest time), then only ids not yet in the quarantine table are inserted.
    Re-processing the same Bronze rows (stream restart, schema change, manual re-run)
    therefore adds nothing; a genuinely new bad record (new file / new ingest time) is added.
    Returns number of rows actually inserted.
    """
    cols = [c for c in df.columns if c not in ("_dq_row_id", "_quarantined_ts", "_run_id")]
    df = df.withColumn("_dq_row_id", F.sha2(F.to_json(F.struct(*[F.col(f"`{c}`") for c in cols])), 256))
    df = df.dropDuplicates(["_dq_row_id"])

    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        return spark.table(target).count()

    # new columns (schema evolution in the source) are added to the quarantine table first
    existing = {f.name.lower() for f in spark.table(target).schema.fields}
    for f in df.schema.fields:
        if f.name.lower() not in existing:
            spark.sql(f"ALTER TABLE {target} ADD COLUMNS (`{f.name}` {f.dataType.simpleString()})")

    view = "_qsrc_" + target.replace(".", "_").replace("-", "_").replace("`", "")
    df.createOrReplaceTempView(view)
    result = spark.sql(f"MERGE INTO {target} AS t USING {view} AS s ON t._dq_row_id = s._dq_row_id "
                       "WHEN NOT MATCHED THEN INSERT *")
    try:
        return int(result.collect()[0].asDict().get("num_inserted_rows", 0) or 0)
    except Exception:
        return 0
