"""Streaming behaviour on a real file-source stream: dedup state, watermark, checkpoint restart."""

from __future__ import annotations

import json

from conftest import event
from telecom_stream.streaming_job import BRONZE_SCHEMA
from telecom_stream.transforms import dedup_events, parse_usage, valid_events


def _drop(spark, folder, name, values):
    """Write one bronze 'micro-batch' of events into the streaming source folder."""
    from conftest import kafka_rows

    spark.createDataFrame(kafka_rows([json.dumps(v) for v in values]), BRONZE_SCHEMA).coalesce(1).write.mode(
        "append"
    ).parquet(str(folder / name))


def _run(spark, cfg, src, sink, chk):
    stream = spark.readStream.schema(BRONZE_SCHEMA).option("recursiveFileLookup", "true").parquet(str(src))
    events = dedup_events(valid_events(parse_usage(stream, cfg)), "2 hours")
    q = (
        events.writeStream.format("parquet")
        .option("path", str(sink))
        .option("checkpointLocation", str(chk))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    return sorted(r["event_id"] for r in spark.read.parquet(str(sink)).collect())


def test_dedup_and_watermark_across_restarts(spark, cfg, tmp_path):
    src, sink, chk = tmp_path / "src", tmp_path / "sink", tmp_path / "chk"
    _drop(
        spark,
        src,
        "b1",
        [
            event(event_id="A", event_ts="2025-06-01T18:00:00Z"),
            event(event_id="A", event_ts="2025-06-01T18:00:00Z"),  # redelivery in the same batch
            event(event_id="B", event_ts="2025-06-01T20:00:00Z"),
        ],
    )
    assert _run(spark, cfg, src, sink, chk) == ["A", "B"]

    # Restart from the checkpoint. Watermark is now 20:00 - 2h = 18:00.
    _drop(
        spark,
        src,
        "b2",
        [
            event(event_id="B", event_ts="2025-06-01T20:00:00Z"),  # redelivery across restarts -> state drops it
            event(event_id="C", event_ts="2025-06-01T19:30:00Z"),  # out of order but inside the watermark
            event(event_id="L", event_ts="2025-06-01T12:00:00Z"),  # late: behind the watermark
        ],
    )
    assert _run(spark, cfg, src, sink, chk) == ["A", "B", "C"]  # L is left for the backfill
