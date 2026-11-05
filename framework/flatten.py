"""Column-mapping driven flattening of nested JSON.

Mapping rules (column_mapping.source_path):
  * 'a.b.c'  -> nested struct path from the root record
  * '$.x'    -> field of the exploded array element (child tables only)
  * '$'      -> the array element itself (arrays of scalars)
A path that does not exist in the data (yet) resolves to NULL, so a new
dataset version or a missing optional field never breaks the pipeline.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

ELEM = "_elem"
AUDIT_COLS = ["_ingest_ts", "_source_file"]


def get_type(schema, path):
    """Return the Spark data type at a dotted path, or None if it does not exist."""
    dt = schema
    for part in path.split("."):
        if isinstance(dt, StructType) and part in dt.fieldNames():
            dt = dt[part].dataType
        else:
            return None
    return dt


def _expr(df, mapping):
    path = mapping["source_path"]
    if path.startswith("$"):
        path = ELEM + path[1:]
    col = F.col(path) if get_type(df.schema, path) is not None else F.lit(None)
    return col.cast(mapping["data_type"]).alias(mapping["target_column"])


def _audit(df):
    return [F.col(c) for c in AUDIT_COLS if c in df.columns]


def flatten_parent(df, mappings):
    return df.select(*[_expr(df, m) for m in mappings], *_audit(df))


def flatten_child(df, mappings, explode_path):
    """One output row per array element. Returns None if the array is absent."""
    if not isinstance(get_type(df.schema, explode_path), ArrayType):
        return None
    exploded = df.select("*", F.explode(F.col(explode_path)).alias(ELEM))  # inner explode: empty arrays -> no rows
    return exploded.select(*[_expr(exploded, m) for m in mappings], *_audit(exploded))
