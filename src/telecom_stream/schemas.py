"""Schemas of the Kafka messages and of the Kafka-shaped records the pipeline reads."""

from __future__ import annotations

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# What the Spark Kafka source returns (key/value cast to string), and what file mode writes.
KAFKA_RECORD = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)

# network.usage.events value (JSON). Timestamps arrive as ISO-8601 strings and are parsed
# explicitly so a bad timestamp is a validation failure rather than a silent null.
USAGE_EVENT = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("subscriber_id", StringType()),
        StructField("device_id", StringType()),
        StructField("cell_id", StringType()),
        StructField("service_code", StringType()),
        StructField("event_ts", StringType()),
        StructField("bytes_up", LongType()),
        StructField("bytes_down", LongType()),
        StructField("duration_sec", IntegerType()),
    ]
)

# subscriber.changes value (JSON): CDC from the billing system, Debezium-style op codes.
SUBSCRIBER_CHANGE = StructType(
    [
        StructField("lsn", LongType()),
        StructField("op", StringType()),  # c = create, u = update, d = delete
        StructField("subscriber_id", StringType()),
        StructField("account_id", StringType()),
        StructField("plan_code", StringType()),
        StructField("market", StringType()),
        StructField("status", StringType()),
        StructField("changed_at", StringType()),
    ]
)

REQUIRED_EVENT_FIELDS = ["event_id", "event_type", "subscriber_id", "cell_id", "service_code", "event_ts"]
