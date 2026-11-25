"""Reusable transformations shared by the streaming queries and the batch jobs.

Everything here takes and returns DataFrames, so the same code runs on a streaming
DataFrame (Structured Streaming) and on a static one (unit tests, backfill, reconciliation).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from telecom_stream.config import Config
from telecom_stream.schemas import REQUIRED_EVENT_FIELDS, SUBSCRIBER_CHANGE, USAGE_EVENT

DQ_REASONS = [
    "MALFORMED_JSON",
    "UNSUPPORTED_SCHEMA_VERSION",
    "MISSING_REQUIRED_FIELD",
    "INVALID_EVENT_TYPE",
    "INVALID_SERVICE_CODE",
    "BAD_TIMESTAMP",
    "INVALID_USAGE_VALUE",
]


def to_bronze(raw: DataFrame) -> DataFrame:
    """Raw Kafka records exactly as received, plus the date they landed (the partition column)."""
    return raw.select(
        "key",
        "value",
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.to_date("timestamp").alias("ingest_date"),
    )


def _dq_reason(cfg: Config, e: str = "e") -> Column:
    v = cfg.validation
    ts = F.col("event_ts")
    usage_ok = (
        F.coalesce(F.col(f"{e}.bytes_up"), F.lit(0)).between(0, v["max_bytes_per_event"])
        & F.coalesce(F.col(f"{e}.bytes_down"), F.lit(0)).between(0, v["max_bytes_per_event"])
        & F.coalesce(F.col(f"{e}.duration_sec"), F.lit(0)).between(0, v["max_duration_sec"])
    )
    missing = None
    for f in REQUIRED_EVENT_FIELDS:
        cond = F.col(f"{e}.{f}").isNull() | (F.trim(F.col(f"{e}.{f}")) == "")
        missing = cond if missing is None else (missing | cond)
    return (
        F.when(F.col("value").isNull() | F.get_json_object("value", "$").isNull(), "MALFORMED_JSON")
        .when(
            ~F.col(f"{e}.schema_version").isin(v["supported_schema_versions"]) | F.col(f"{e}.schema_version").isNull(),
            "UNSUPPORTED_SCHEMA_VERSION",
        )
        .when(missing, "MISSING_REQUIRED_FIELD")
        .when(~F.col(f"{e}.event_type").isin(v["event_types"]), "INVALID_EVENT_TYPE")
        .when(~F.col(f"{e}.service_code").isin(v["service_codes"]), "INVALID_SERVICE_CODE")
        .when(ts.isNull(), "BAD_TIMESTAMP")
        .when(~usage_ok, "INVALID_USAGE_VALUE")
    )


def parse_usage(bronze: DataFrame, cfg: Config) -> DataFrame:
    """Parse and validate usage events. Every input row comes out once, with dq_reason null
    when it is valid, so valid + invalid always adds back up to bronze."""
    parsed = bronze.withColumn("e", F.from_json("value", USAGE_EVENT, {"mode": "PERMISSIVE"})).withColumn(
        "event_ts", F.to_timestamp("e.event_ts")
    )
    return parsed.select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.subscriber_id").alias("subscriber_id"),
        F.col("e.device_id").alias("device_id"),
        F.col("e.cell_id").alias("cell_id"),
        F.col("e.service_code").alias("service_code"),
        "event_ts",
        F.to_date("event_ts").alias("event_date"),
        F.hour("event_ts").alias("event_hour"),
        F.coalesce("e.bytes_up", F.lit(0)).alias("bytes_up"),
        F.coalesce("e.bytes_down", F.lit(0)).alias("bytes_down"),
        F.coalesce("e.duration_sec", F.lit(0)).alias("duration_sec"),
        F.col("e.schema_version").alias("schema_version"),
        "key",
        "value",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        "kafka_ts",
        _dq_reason(cfg).alias("dq_reason"),
    )


SILVER_COLUMNS = [
    "event_id",
    "event_type",
    "subscriber_id",
    "device_id",
    "cell_id",
    "service_code",
    "event_ts",
    "event_date",
    "event_hour",
    "bytes_up",
    "bytes_down",
    "duration_sec",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
]


def valid_events(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("dq_reason").isNull()).select(*SILVER_COLUMNS)


def dlq_records(parsed: DataFrame) -> DataFrame:
    """Rejected records keep the original bytes and source offsets so they can be replayed."""
    return parsed.where(F.col("dq_reason").isNotNull()).select(
        "dq_reason",
        "key",
        "value",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        F.current_timestamp().alias("rejected_at"),
        F.to_date("kafka_ts").alias("ingest_date"),
    )


def dedup_events(events: DataFrame, watermark_delay: str) -> DataFrame:
    """Drop producer redeliveries (same event_id). State is kept for the watermark delay only;
    anything later than that is handled by the daily backfill, not by unbounded state."""
    return events.withWatermark("event_ts", watermark_delay).dropDuplicatesWithinWatermark(["event_id"])


def cell_kpis(events: DataFrame, window: str) -> DataFrame:
    """Network KPIs per cell per tumbling window. Expects a watermark on event_ts (append mode)."""
    is_voice = F.col("event_type") == "VOICE_CALL"
    is_drop = F.col("event_type") == "DROPPED_CALL"
    agg = events.groupBy(F.window("event_ts", window).alias("w"), "cell_id").agg(
        F.count(F.lit(1)).alias("events"),
        F.round(F.sum(F.col("bytes_up") + F.col("bytes_down")) / 1e6, 3).alias("data_mb"),
        F.sum(is_voice.cast("int")).alias("voice_calls"),
        F.sum(is_drop.cast("int")).alias("dropped_calls"),
        F.approx_count_distinct("subscriber_id").alias("active_subscribers"),
    )
    attempts = F.col("voice_calls") + F.col("dropped_calls")
    return agg.select(
        "cell_id",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "events",
        "data_mb",
        "voice_calls",
        "dropped_calls",
        F.when(attempts > 0, F.round(F.col("dropped_calls") / attempts, 4)).alias("drop_rate"),
        "active_subscribers",
        F.to_date("w.start").alias("window_date"),
    )


def parse_cdc(bronze: DataFrame) -> DataFrame:
    c = F.from_json("value", SUBSCRIBER_CHANGE)
    return (
        bronze.withColumn("c", c)
        .select(
            F.col("c.lsn").alias("lsn"),
            F.col("c.op").alias("op"),
            F.col("c.subscriber_id").alias("subscriber_id"),
            F.col("c.account_id").alias("account_id"),
            F.col("c.plan_code").alias("plan_code"),
            F.col("c.market").alias("market"),
            F.col("c.status").alias("status"),
            F.to_timestamp("c.changed_at").alias("changed_at"),
        )
        .where(F.col("lsn").isNotNull() & F.col("subscriber_id").isNotNull() & F.col("op").isin("c", "u", "d"))
    )
