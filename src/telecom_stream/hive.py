"""Register every zone as an external Hive table, plus reporting views."""

from __future__ import annotations

from pyspark.sql import SparkSession

from telecom_stream import config as C
from telecom_stream.config import Config
from telecom_stream.subscribers import _exists

TABLES = {
    "bronze_network_events": C.BRONZE_USAGE,
    "bronze_subscriber_changes": C.BRONZE_CDC,
    "silver_network_events": C.SILVER_EVENTS,
    "network_events_late_backfill": C.SILVER_BACKFILL,
    "network_events_dlq": C.DLQ,
    "cell_kpi_5min": C.GOLD_CELL_KPI,
    "dim_subscriber": C.DIM_SUBSCRIBER,
    "subscriber_daily_usage": C.GOLD_DAILY_USAGE,
}
PARTITIONED = {
    "bronze_network_events",
    "bronze_subscriber_changes",
    "silver_network_events",
    "network_events_late_backfill",
    "network_events_dlq",
    "cell_kpi_5min",
    "subscriber_daily_usage",
}


def register_tables(spark: SparkSession, cfg: Config) -> list[str]:
    db = cfg.hive_database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    done = []
    for name, loc in TABLES.items():
        path = cfg.table(*loc)
        if not _exists(path):
            continue
        # Schema and partition columns are inferred from the files at the location.
        spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{name} USING parquet LOCATION '{path}'")
        if name in PARTITIONED:
            spark.sql(f"ALTER TABLE {db}.{name} RECOVER PARTITIONS")
        spark.sql(f"REFRESH TABLE {db}.{name}")
        done.append(f"{db}.{name}")
    if f"{db}.silver_network_events" in done:
        backfill = (
            f"UNION ALL SELECT {_cols()} FROM {db}.network_events_late_backfill"
            if f"{db}.network_events_late_backfill" in done
            else ""
        )
        spark.sql(
            f"CREATE OR REPLACE VIEW {db}.network_events AS SELECT {_cols()} FROM {db}.silver_network_events {backfill}"
        )
        done.append(f"{db}.network_events (view)")
    if f"{db}.dim_subscriber" in done:
        spark.sql(
            f"CREATE OR REPLACE VIEW {db}.subscriber_current AS "
            f"SELECT * FROM {db}.dim_subscriber WHERE is_current AND NOT is_deleted"
        )
        done.append(f"{db}.subscriber_current (view)")
    return done


def _cols() -> str:
    from telecom_stream.transforms import SILVER_COLUMNS

    return ", ".join(SILVER_COLUMNS)
