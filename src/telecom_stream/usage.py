"""Late-event backfill and the point-in-time daily usage table."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from telecom_stream import config as C
from telecom_stream.config import Config
from telecom_stream.subscribers import _exists
from telecom_stream.transforms import SILVER_COLUMNS, parse_usage, valid_events


def all_events(spark: SparkSession, cfg: Config) -> DataFrame:
    """Silver (streamed) + late backfill: the complete set of clean events."""
    silver = spark.read.parquet(str(cfg.table(*C.SILVER_EVENTS))).select(*SILVER_COLUMNS)
    bf_path = cfg.table(*C.SILVER_BACKFILL)
    if _exists(bf_path):
        silver = silver.unionByName(spark.read.parquet(str(bf_path)).select(*SILVER_COLUMNS))
    return silver


def backfill_late_events(spark: SparkSession, cfg: Config) -> dict:
    """Valid events that are in bronze but not in silver arrived after the watermark and were
    dropped by the stream. Re-derive them from bronze and append them. Re-running adds nothing."""
    bronze = spark.read.parquet(str(cfg.table(*C.BRONZE_USAGE)))
    first = Window.partitionBy("event_id").orderBy("kafka_partition", "kafka_offset")
    valid = (
        valid_events(parse_usage(bronze, cfg)).withColumn("_n", F.row_number().over(first)).where("_n = 1").drop("_n")
    )
    have = all_events(spark, cfg).select("event_id")
    missing = valid.join(have, "event_id", "left_anti").withColumn("backfilled_at", F.current_timestamp()).cache()
    n = missing.count()
    if n:
        missing.repartition("event_date").write.mode("append").partitionBy("event_date").parquet(
            str(cfg.table(*C.SILVER_BACKFILL))
        )
    by_date = {
        str(r["event_date"]): r["n"] for r in missing.groupBy("event_date").agg(F.count("*").alias("n")).collect()
    }
    missing.unpersist()
    return {"backfilled": n, "by_event_date": by_date}


def daily_usage(events: DataFrame, dim: DataFrame) -> DataFrame:
    """Usage per subscriber per day, split by the plan/market in effect at each event's time."""
    events = events.dropDuplicates(["event_id"])
    d = dim.select("subscriber_id", "plan_code", "market", "valid_from", "valid_to").alias("d")
    e = events.alias("e")
    # Range (point-in-time) join. The dimension is small next to events, so broadcast it;
    # at full scale bucket both sides on subscriber_id instead.
    joined = e.join(
        F.broadcast(d),
        (F.col("e.subscriber_id") == F.col("d.subscriber_id"))
        & (F.col("e.event_ts") >= F.col("d.valid_from"))
        & (F.col("e.event_ts") < F.col("d.valid_to")),
        "left",
    )
    t = F.col("e.event_type")
    return (
        joined.groupBy(
            F.col("e.event_date").alias("event_date"),
            F.col("e.subscriber_id").alias("subscriber_id"),
            F.coalesce(F.col("d.plan_code"), F.lit("UNKNOWN")).alias("plan_code"),
            F.coalesce(F.col("d.market"), F.lit("UNKNOWN")).alias("market"),
        )
        .agg(
            F.count("*").alias("events"),
            F.sum(F.col("e.bytes_up") + F.col("e.bytes_down")).alias("total_bytes"),
            F.sum((t == "DATA_SESSION").cast("int")).alias("data_sessions"),
            F.sum((t == "VOICE_CALL").cast("int")).alias("voice_calls"),
            F.sum((t == "SMS").cast("int")).alias("sms_count"),
            F.sum((t == "DROPPED_CALL").cast("int")).alias("dropped_calls"),
            F.sum(F.when(t == "VOICE_CALL", F.col("e.duration_sec")).otherwise(0)).alias("voice_seconds"),
        )
        .withColumn("total_gb", F.round(F.col("total_bytes") / 1e9, 4))
    )


def build_daily_usage(spark: SparkSession, cfg: Config, dates: list[str] | None = None) -> dict:
    """Rebuild only the event_date partitions that have events (or the dates given);
    dynamic partition overwrite leaves every other day untouched."""
    events = all_events(spark, cfg)
    if dates:
        events = events.where(F.col("event_date").isin(dates))
    dim = spark.read.parquet(str(cfg.table(*C.DIM_SUBSCRIBER)))
    usage = daily_usage(events, dim).cache()
    usage.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet(
        str(cfg.table(*C.GOLD_DAILY_USAGE))
    )
    stats = (
        usage.groupBy("event_date")
        .agg(
            F.count("*").alias("rows"),
            F.sum("events").alias("events"),
            F.sum((F.col("plan_code") == "UNKNOWN").cast("int")).alias("unknown_subscriber_rows"),
        )
        .collect()
    )
    usage.unpersist()
    return {str(r["event_date"]): {k: r[k] for k in ("rows", "events", "unknown_subscriber_rows")} for r in stats}
