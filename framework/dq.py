"""Metadata-driven data quality rules.

rule_type:      not_null | range ('min,max') | regex | allowed_values ('A|B|C')
action:         quarantine -> row goes to quarantine table with the reason
                drop       -> row silently discarded (counted in the log)
                flag       -> row kept, reason written to _dq_flags
"""
from functools import reduce
from pyspark.sql import functions as F


def _fail_condition(rule):
    c = F.col(rule["column_name"])
    t, v = rule["rule_type"], rule["rule_value"]
    if t == "not_null":
        cond = c.isNull()
    elif t == "range":
        lo, hi = [x.strip() for x in v.split(",")]
        cond = (c < F.lit(float(lo))) | (c > F.lit(float(hi)))
    elif t == "regex":
        cond = c.isNotNull() & (~c.cast("string").rlike(v))
    elif t == "allowed_values":
        cond = c.isNotNull() & (~c.cast("string").isin([x.strip() for x in v.split("|")]))
    else:
        raise ValueError(f"Unknown rule_type '{t}'")
    return F.coalesce(cond, F.lit(False))  # NULL result = rule not violated


def apply_dq(df, rules):
    """Returns (clean_df, quarantine_df_or_None, rows_dropped)."""
    buckets = {"quarantine": [], "drop": [], "flag": []}
    for r in rules:
        if r["action"] not in buckets:
            raise ValueError(f"Unknown action '{r['action']}'")
        buckets[r["action"]].append((_fail_condition(r), f"{r['rule_type']}:{r['column_name']}"))

    rows_dropped = 0
    if buckets["drop"]:
        drop_cond = reduce(lambda a, b: a | b, [c for c, _ in buckets["drop"]])
        rows_dropped = df.filter(drop_cond).count()
        df = df.filter(~drop_cond)

    quarantine_df = None
    if buckets["quarantine"]:
        reason = F.concat_ws("; ", *[F.when(c, F.lit(tag)) for c, tag in buckets["quarantine"]])
        df = df.withColumn("_dq_reason", reason)
        quarantine_df = df.filter(F.length("_dq_reason") > 0)
        df = df.filter(F.length("_dq_reason") == 0).drop("_dq_reason")

    if buckets["flag"]:
        flags = F.concat_ws("; ", *[F.when(c, F.lit(tag)) for c, tag in buckets["flag"]])
        df = df.withColumn("_dq_flags", F.when(F.length(flags) > 0, flags))

    return df, quarantine_df, rows_dropped
