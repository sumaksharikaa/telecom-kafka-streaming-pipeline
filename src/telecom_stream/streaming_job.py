"""Structured Streaming job: Kafka -> bronze -> silver / DLQ / cell KPIs.

    python -m telecom_stream.streaming_job --trigger available-now   # drain what's there, then stop
    python -m telecom_stream.streaming_job --trigger continuous      # run until killed

Each query has its own checkpoint, so a restart resumes from the last committed Kafka
offsets / bronze files, and the file sinks are exactly-once.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DateType, StructField, StructType

from telecom_stream import config as C
from telecom_stream.config import Config, load_config
from telecom_stream.schemas import KAFKA_RECORD
from telecom_stream.sources import read_topic, to_kafka_dlq
from telecom_stream.spark import get_spark
from telecom_stream.transforms import (
    cell_kpis,
    dedup_events,
    dlq_records,
    parse_usage,
    to_bronze,
    valid_events,
)

log = logging.getLogger("telecom_stream.streaming")

BRONZE_SCHEMA = StructType(
    [f if f.name != "timestamp" else StructField("kafka_ts", f.dataType) for f in KAFKA_RECORD.fields]
    + [StructField("ingest_date", DateType())]
)


@dataclass
class QuerySpec:
    name: str
    stage: int  # with available-now, stage 1 (Kafka -> bronze) drains before stage 2 starts
    start: Callable[[SparkSession, Config, dict], StreamingQuery]


def _bronze_stream(spark: SparkSession, cfg: Config, table: tuple[str, str]) -> DataFrame:
    return (
        spark.readStream.schema(BRONZE_SCHEMA)
        .option("maxFilesPerTrigger", cfg.streaming["max_files_per_trigger"])
        .parquet(str(cfg.table(*table)))
    )


def _file_sink(
    df: DataFrame, cfg: Config, name: str, table: tuple[str, str], partition: str, trigger: dict
) -> StreamingQuery:
    return (
        df.writeStream.queryName(name)
        .format("parquet")
        .option("path", str(cfg.table(*table)))
        .option("checkpointLocation", str(cfg.checkpoint(name)))
        .partitionBy(partition)
        .outputMode("append")
        .trigger(**trigger)
        .start()
    )


def _bronze(topic_key: str, table: tuple[str, str]) -> Callable:
    def start(spark: SparkSession, cfg: Config, trigger: dict) -> StreamingQuery:
        cfg.table(*table).mkdir(parents=True, exist_ok=True)
        return _file_sink(
            to_bronze(read_topic(spark, cfg, topic_key)), cfg, f"bronze_{topic_key}", table, "ingest_date", trigger
        )

    return start


def _parsed(spark: SparkSession, cfg: Config) -> DataFrame:
    return parse_usage(_bronze_stream(spark, cfg, C.BRONZE_USAGE), cfg)


def _silver(spark: SparkSession, cfg: Config, trigger: dict) -> StreamingQuery:
    events = dedup_events(valid_events(_parsed(spark, cfg)), cfg.streaming["watermark_delay"])
    return _file_sink(events, cfg, "silver_events", C.SILVER_EVENTS, "event_date", trigger)


def _dlq(spark: SparkSession, cfg: Config, trigger: dict) -> StreamingQuery:
    return _file_sink(dlq_records(_parsed(spark, cfg)), cfg, "dlq_parquet", C.DLQ, "ingest_date", trigger)


def _dlq_kafka(spark: SparkSession, cfg: Config, trigger: dict) -> StreamingQuery:
    return (
        to_kafka_dlq(dlq_records(_parsed(spark, cfg)))
        .writeStream.queryName("dlq_kafka")
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
        .option("topic", cfg.topic("dlq"))
        .option("checkpointLocation", str(cfg.checkpoint("dlq_kafka")))
        .trigger(**trigger)
        .start()
    )


def _cell_kpi(spark: SparkSession, cfg: Config, trigger: dict) -> StreamingQuery:
    events = dedup_events(valid_events(_parsed(spark, cfg)), cfg.streaming["watermark_delay"])
    kpis = cell_kpis(events, cfg.streaming["kpi_window"])
    return _file_sink(kpis, cfg, "cell_kpi_5min", C.GOLD_CELL_KPI, "window_date", trigger)


def query_specs(cfg: Config) -> list[QuerySpec]:
    specs = [
        QuerySpec("bronze_usage", 1, _bronze("usage", C.BRONZE_USAGE)),
        QuerySpec("bronze_cdc", 1, _bronze("cdc", C.BRONZE_CDC)),
        QuerySpec("silver_events", 2, _silver),
        QuerySpec("dlq_parquet", 2, _dlq),
        QuerySpec("cell_kpi_5min", 2, _cell_kpi),
    ]
    if cfg.source_mode == "kafka" and cfg.streaming["publish_dlq_to_kafka"]:
        specs.append(QuerySpec("dlq_kafka", 2, _dlq_kafka))
    return specs


def _summary(q: StreamingQuery) -> dict:
    rows = sum(p.get("numInputRows", 0) for p in q.recentProgress)
    return {"query": q.name, "batches": len(q.recentProgress), "input_rows": rows}


def run(cfg: Config, trigger: str = "available-now", spark: SparkSession | None = None) -> list[dict]:
    spark = spark or get_spark("telecom-streaming", cfg, kafka=cfg.source_mode == "kafka")
    specs = query_specs(cfg)
    if trigger == "continuous":
        t = {"processingTime": cfg.streaming["processing_trigger"]}
        queries = [s.start(spark, cfg, t) for s in specs]
        log.info("started %s", [q.name for q in queries])
        spark.streams.awaitAnyTermination()  # surfaces the first failure; the scheduler restarts the job
        return [_summary(q) for q in queries]
    summaries = []
    for stage in sorted({s.stage for s in specs}):
        queries = [s.start(spark, cfg, {"availableNow": True}) for s in specs if s.stage == stage]
        for q in queries:
            q.awaitTermination()
            if q.exception():
                raise RuntimeError(f"query {q.name} failed: {q.exception()}")
            summaries.append(_summary(q))
    return summaries


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--trigger", choices=["available-now", "continuous"], default="available-now")
    args = p.parse_args()
    for s in run(load_config(), args.trigger):
        log.info("%s", s)


if __name__ == "__main__":
    main()
