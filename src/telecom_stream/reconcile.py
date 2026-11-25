"""Source-to-target reconciliation: Kafka offsets -> bronze -> silver / DLQ -> daily usage.

Every check is a number on both sides, so a failure says exactly where records went missing.
The job writes reports/reconciliation_<ts>.json and exits non-zero if any check fails.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from telecom_stream import config as C
from telecom_stream.config import Config
from telecom_stream.subscribers import _exists
from telecom_stream.transforms import parse_usage
from telecom_stream.usage import all_events


def source_offsets(cfg: Config, topic_key: str) -> dict[int, tuple[int, int]]:
    """{partition: (low, high)} — the range of offsets the source holds for the topic."""
    topic = cfg.topic(topic_key)
    if cfg.source_mode == "file":
        state = json.loads(cfg.broker_state.read_text()) if cfg.broker_state.exists() else {}
        return {int(p): (0, hi) for p, hi in state.get(topic, {}).items()}
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {"bootstrap.servers": cfg.bootstrap_servers, "group.id": "telecom-reconcile", "enable.auto.commit": False}
    )
    try:
        meta = consumer.list_topics(topic, timeout=30).topics[topic]
        return {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30) for p in meta.partitions}
    finally:
        consumer.close()


def _check(name: str, expected: Any, actual: Any, ok: bool | None = None) -> dict:
    return {"check": name, "expected": expected, "actual": actual, "ok": bool(expected == actual if ok is None else ok)}


def offset_checks(spark: SparkSession, cfg: Config, topic_key: str, table: tuple[str, str]) -> list[dict]:
    src = source_offsets(cfg, topic_key)
    bronze = spark.read.parquet(str(cfg.table(*table)))
    landed = {
        r["partition"]: r
        for r in bronze.groupBy("partition")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("offset").alias("distinct"),
            F.min("offset").alias("lo"),
            F.max("offset").alias("hi"),
        )
        .collect()
    }
    checks = []
    for p, (lo, hi) in sorted(src.items()):
        r = landed.get(p)
        rows, distinct = (r["rows"], r["distinct"]) if r else (0, 0)
        checks.append(_check(f"{topic_key} p{p}: bronze offsets cover source [{lo},{hi})", hi - lo, distinct))
        checks.append(_check(f"{topic_key} p{p}: no offset landed twice", distinct, rows))
    gaps = sum((hi - lo) - (landed[p]["distinct"] if p in landed else 0) for p, (lo, hi) in src.items())
    checks.append(
        _check(
            f"{topic_key}: offset gaps (records lost)",
            f"<= {cfg.reconciliation['max_offset_gaps']}",
            gaps,
            gaps <= cfg.reconciliation["max_offset_gaps"],
        )
    )
    return checks


def reconcile(spark: SparkSession, cfg: Config) -> dict:
    checks = offset_checks(spark, cfg, "usage", C.BRONZE_USAGE) + offset_checks(spark, cfg, "cdc", C.BRONZE_CDC)

    parsed = parse_usage(spark.read.parquet(str(cfg.table(*C.BRONZE_USAGE))), cfg).cache()
    bronze_rows = parsed.count()
    invalid = parsed.where("dq_reason IS NOT NULL").count()
    valid = bronze_rows - invalid
    distinct_valid = parsed.where("dq_reason IS NULL").select("event_id").distinct().count()
    dlq_path = cfg.table(*C.DLQ)
    dlq_rows = spark.read.parquet(str(dlq_path)).count() if _exists(dlq_path) else 0
    by_reason = {
        r["dq_reason"]: r["n"]
        for r in parsed.where("dq_reason IS NOT NULL").groupBy("dq_reason").agg(F.count("*").alias("n")).collect()
    }

    events = all_events(spark, cfg).cache()
    loaded = events.count()
    loaded_distinct = events.select("event_id").distinct().count()
    missing = (
        parsed.where("dq_reason IS NULL")
        .select("event_id")
        .distinct()
        .join(events.select("event_id"), "event_id", "left_anti")
        .count()
    )

    usage_path = cfg.table(*C.GOLD_DAILY_USAGE)
    usage_events = spark.read.parquet(str(usage_path)).agg(F.sum("events")).first()[0] if _exists(usage_path) else 0

    checks += [
        _check("bronze = valid + DLQ", bronze_rows, valid + dlq_rows),
        _check("DLQ table = invalid records in bronze", invalid, dlq_rows),
        _check("silver+backfill has no duplicate event_id", loaded, loaded_distinct),
        _check(
            "valid events missing from silver+backfill",
            f"<= {cfg.reconciliation['max_missing_events']}",
            missing,
            missing <= cfg.reconciliation["max_missing_events"],
        ),
        _check("daily usage events = distinct clean events", loaded_distinct, int(usage_events or 0)),
    ]
    if cfg.source_mode == "kafka" and cfg.streaming["publish_dlq_to_kafka"]:
        dlq_topic = sum(hi - lo for lo, hi in source_offsets(cfg, "dlq").values())
        checks.append(_check("DLQ topic messages = DLQ table", dlq_rows, dlq_topic))
    parsed.unpersist()
    events.unpersist()

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_mode": cfg.source_mode,
        "passed": all(c["ok"] for c in checks),
        "counts": {
            "bronze_usage_records": bronze_rows,
            "valid_records": valid,
            "distinct_valid_events": distinct_valid,
            "redeliveries_removed": valid - distinct_valid,
            "dlq_records": dlq_rows,
            "dlq_by_reason": dict(sorted(by_reason.items())),
            "clean_events_loaded": loaded,
        },
        "checks": checks,
    }
    cfg.reports_dir.mkdir(parents=True, exist_ok=True)
    out = cfg.reports_dir / f"reconciliation_{datetime.now(UTC):%Y%m%dT%H%M%S%f}.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    report["path"] = str(out)
    return report
