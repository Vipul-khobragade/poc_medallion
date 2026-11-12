def quarantine_insert(spark, df, target):
    """
    Idempotent quarantine write: the same rejected record is never stored twice.
    Each rejected row gets _dq_row_id = hash of all its values (including source file and
    ingest time), then only ids not yet in the quarantine table are inserted.
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
