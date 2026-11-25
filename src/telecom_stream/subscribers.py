"""Incremental SCD Type 2 subscriber dimension, built from the CDC topic's bronze table.

Only changes with an LSN above the dimension's high-water mark are read, and only the
subscribers they touch are rebuilt. A subscriber's timeline is rebuilt from all of its
versions, ordered by effective time, so a backdated change (new LSN, old changed_at)
lands in the right place instead of being appended at the end.
"""

from __future__ import annotations

import shutil
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from telecom_stream import config as C
from telecom_stream.config import Config
from telecom_stream.transforms import parse_cdc

OPEN_END = "9999-12-31 00:00:00"
ATTRS = ["account_id", "plan_code", "market", "status"]


def _exists(path) -> bool:
    return path.exists() and any(p.suffix == ".parquet" for p in path.rglob("*.parquet"))


def rebuild_timelines(versions: DataFrame) -> DataFrame:
    """versions: subscriber_id, ATTRS, changed_at, lsn, op -> SCD2 rows."""
    versions = versions.dropDuplicates(["lsn"])
    w = Window.partitionBy("subscriber_id").orderBy("changed_at", "lsn")
    rows = versions.withColumn("next_change", F.lead("changed_at").over(w)).withColumn(
        "next_lsn", F.lead("lsn").over(w)
    )
    # Two changes with the same effective time: the later LSN wins; the earlier is never in effect.
    rows = rows.where(F.col("next_change").isNull() | (F.col("next_change") > F.col("changed_at")))
    return rows.select(
        F.xxhash64("subscriber_id", "lsn").alias("subscriber_sk"),
        "subscriber_id",
        *ATTRS,
        F.col("changed_at").alias("valid_from"),
        F.coalesce("next_change", F.to_timestamp(F.lit(OPEN_END))).alias("valid_to"),
        F.col("next_change").isNull().alias("is_current"),
        (F.col("op") == "d").alias("is_deleted"),
        F.col("lsn").alias("source_lsn"),
        F.current_timestamp().alias("dim_updated_at"),
    )


def merge(dim: DataFrame | None, changes: DataFrame) -> DataFrame:
    """Apply new CDC rows to an existing dimension (or build it when dim is None)."""
    if dim is None:
        return rebuild_timelines(changes)
    affected = changes.select("subscriber_id").distinct()
    prior = dim.join(affected, "subscriber_id").select(
        "subscriber_id",
        *ATTRS,
        F.col("valid_from").alias("changed_at"),
        F.col("source_lsn").alias("lsn"),
        F.when(F.col("is_deleted"), "d").otherwise("u").alias("op"),
    )
    untouched = dim.join(affected, "subscriber_id", "left_anti")
    return untouched.unionByName(rebuild_timelines(prior.unionByName(changes.select(*prior.columns))))


def build_dim_subscriber(spark: SparkSession, cfg: Config) -> dict:
    path = cfg.table(*C.DIM_SUBSCRIBER)
    changes = parse_cdc(spark.read.parquet(str(cfg.table(*C.BRONZE_CDC))))
    dim = spark.read.parquet(str(path)) if _exists(path) else None
    high_water = dim.agg(F.max("source_lsn")).first()[0] if dim is not None else None
    new = changes.where(F.col("lsn") > high_water) if high_water is not None else changes
    new = new.dropDuplicates(["lsn"]).cache()
    n_new = new.count()
    if n_new == 0:
        return {"new_changes": 0, "high_water_lsn": high_water, "rows": dim.count() if dim is not None else 0}
    result = merge(dim, new.select("subscriber_id", *ATTRS, "changed_at", "lsn", "op"))
    # Write next to the table, then swap, so readers never see a half-written dimension.
    tmp = path.with_name(f"{path.name}.__{int(time.time() * 1000)}")
    result.repartition(4).write.mode("overwrite").parquet(str(tmp))
    if path.exists():
        shutil.rmtree(path)
    tmp.rename(path)
    out = spark.read.parquet(str(path))
    stats = {
        "new_changes": n_new,
        "subscribers_rebuilt": new.select("subscriber_id").distinct().count(),
        "rows": out.count(),
        "current_rows": out.where("is_current").count(),
        "high_water_lsn": out.agg(F.max("source_lsn")).first()[0],
    }
    new.unpersist()
    return stats
