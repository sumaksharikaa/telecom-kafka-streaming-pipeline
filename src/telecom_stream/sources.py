"""Streaming sources and sinks that differ between Kafka mode and file mode.

Only bronze ingestion touches Kafka. Everything downstream streams from the bronze tables,
so Kafka is read once per topic and replays come from bronze, not from the brokers.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from telecom_stream.config import Config
from telecom_stream.schemas import KAFKA_RECORD


def read_topic(spark: SparkSession, cfg: Config, topic_key: str) -> DataFrame:
    """A streaming DataFrame with the Kafka record columns, whichever mode is configured."""
    if cfg.source_mode == "kafka":
        k = cfg.kafka
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
            .option("subscribe", cfg.topic(topic_key))
            .option("startingOffsets", k["starting_offsets"])
            .option("maxOffsetsPerTrigger", k["max_offsets_per_trigger"])
            .option("failOnDataLoss", str(k["fail_on_data_loss"]).lower())
            .load()
        )
        return raw.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            "topic",
            "partition",
            "offset",
            "timestamp",
        )
    path = cfg.landing(topic_key)
    path.mkdir(parents=True, exist_ok=True)
    return (
        spark.readStream.schema(KAFKA_RECORD)
        .option("maxFilesPerTrigger", cfg.streaming["max_files_per_trigger"])
        .json(str(path))
    )


def to_kafka_dlq(dlq: DataFrame) -> DataFrame:
    """Shape DLQ rows for the Kafka sink: original key, a JSON envelope with the reason + source."""
    envelope = F.to_json(
        F.struct(
            "dq_reason",
            "kafka_partition",
            "kafka_offset",
            "kafka_ts",
            "rejected_at",
            F.col("value").alias("original_value"),
        )
    )
    return dlq.select(F.col("key").cast("string").alias("key"), envelope.alias("value"))
