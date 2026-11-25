from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from telecom_stream.config import load_config  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    monkeypatch.delenv("TELECOM_SOURCE_MODE", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    return load_config(root=tmp_path)


def event(**over) -> dict:
    e = {
        "schema_version": 1,
        "event_id": "EV1",
        "event_type": "DATA_SESSION",
        "subscriber_id": "SUB1",
        "device_id": "IMEI1",
        "cell_id": "CELL-PHI-001",
        "service_code": "DATA_LTE",
        "event_ts": "2025-06-01T10:00:00Z",
        "bytes_up": 100,
        "bytes_down": 900,
        "duration_sec": 60,
    }
    e.update(over)
    return e


def kafka_rows(values: list, start_offset: int = 0) -> list[tuple]:
    """Bronze-shaped rows: key, value, topic, partition, offset, kafka_ts, ingest_date."""
    ts = datetime(2025, 6, 1, 12, 0, 0)
    out = []
    for i, v in enumerate(values):
        value = v if isinstance(v, str) or v is None else json.dumps(v)
        out.append(("SUB1", value, "network.usage.events", 0, start_offset + i, ts, ts.date()))
    return out


@pytest.fixture
def bronze_df(spark):
    from telecom_stream.streaming_job import BRONZE_SCHEMA

    def make(values: list, start_offset: int = 0):
        return spark.createDataFrame(kafka_rows(values, start_offset), BRONZE_SCHEMA)

    return make
