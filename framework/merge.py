"""CDC-style upsert into Silver: dedupe inside the batch, then MERGE."""
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable


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
    is_deleted = f"coalesce(s.`{delete_col}`, false)" if delete_col else None
    fresh = (f"(t.`{order_by}` IS NULL OR s.`{order_by}` >= t.`{order_by}`)" if order_by else "true")

    if not spark.catalog.tableExists(target):
        base = df.filter(f"NOT coalesce(`{delete_col}`, false)") if delete_col else df
        base.write.format("delta").saveAsTable(target)
        return spark.table(target).count()

    cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    m = DeltaTable.forName(spark, target).alias("t").merge(df.alias("s"), cond)
    if delete_col:
        m = m.whenMatchedDelete(condition=f"{is_deleted} = true AND {fresh}")
        m = m.whenMatchedUpdateAll(condition=f"{is_deleted} = false AND {fresh}")
        m = m.whenNotMatchedInsertAll(condition=f"{is_deleted} = false")
    else:
        m = m.whenMatchedUpdateAll(condition=fresh)
        m = m.whenNotMatchedInsertAll()
    m.execute()

    try:  # operation metrics of the MERGE we just ran
        metrics = DeltaTable.forName(spark, target).history(1).select("operationMetrics").collect()[0][0]
        return sum(int(metrics.get(k, 0)) for k in
                   ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted"))
    except Exception:
        return 0
