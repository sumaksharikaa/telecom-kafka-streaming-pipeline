from __future__ import annotations

import json

import pytest

from telecom_stream.config import load_config
from telecom_stream.reconcile import source_offsets
from telecom_stream.simulator import FileSink, build


def test_scenario_is_deterministic():
    a, b = build(n_subscribers=200), build(n_subscribers=200)
    assert a.manifest == b.manifest
    assert [m.value for m in a.phases[2]] == [m.value for m in b.phases[2]]


def test_manifest_adds_up():
    sc = build(n_subscribers=200)
    m = sc.manifest
    usage_msgs = m["usage_messages"]["1"] + m["usage_messages"]["2"]
    assert usage_msgs == m["unique_valid_events"] + m["duplicate_messages"] + sum(m["invalid_by_reason"].values())
    assert sum(r["events"] for r in sc.expected_usage) == m["unique_valid_events"]


def test_file_sink_assigns_contiguous_offsets_across_phases(cfg):
    sc = build(n_subscribers=100)
    sink = FileSink(cfg, records_per_file=500)
    sink.send(sc.phases[1], "p1")
    FileSink(cfg, records_per_file=500).send(sc.phases[2], "p2")  # new process, resumes offsets
    rows = [
        json.loads(line)
        for f in sorted(cfg.landing("usage").glob("part-*.json"))
        for line in f.read_text().splitlines()
    ]
    by_part: dict[int, list[int]] = {}
    for r in rows:
        by_part.setdefault(r["partition"], []).append(r["offset"])
    for offs in by_part.values():
        assert sorted(offs) == list(range(len(offs)))
    ends = source_offsets(cfg, "usage")
    assert sum(hi - lo for lo, hi in ends.values()) == len(rows)


def test_config_env_overrides(tmp_path, monkeypatch):
    monkeypatch.setenv("TELECOM_SOURCE_MODE", "kafka")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    cfg = load_config(root=tmp_path)
    assert cfg.source_mode == "kafka"
    assert cfg.bootstrap_servers == "broker:29092"
    assert cfg.landing("usage") == tmp_path / "landing" / "network.usage.events"


def test_config_rejects_unknown_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("TELECOM_SOURCE_MODE", "kinesis")
    with pytest.raises(ValueError):
        load_config(root=tmp_path)


def test_reconciliation_detects_lost_and_doubled_offsets(spark, cfg):
    from conftest import kafka_rows
    from telecom_stream import config as C
    from telecom_stream.reconcile import offset_checks
    from telecom_stream.streaming_job import BRONZE_SCHEMA

    rows = [r for r in kafka_rows(["{}"] * 5) if r[4] != 2]  # offset 2 never landed
    rows.append(kafka_rows(["{}"] * 5)[4])  # offset 4 landed twice
    spark.createDataFrame(rows, BRONZE_SCHEMA).write.parquet(str(cfg.table(*C.BRONZE_USAGE)))
    cfg.broker_state.parent.mkdir(parents=True)
    cfg.broker_state.write_text(json.dumps({cfg.topic("usage"): {"0": 5}}))
    checks = {c["check"].split(": ", 1)[1]: c for c in offset_checks(spark, cfg, "usage", C.BRONZE_USAGE)}
    assert checks["bronze offsets cover source [0,5)"]["actual"] == 4
    assert not checks["no offset landed twice"]["ok"]
    assert checks["offset gaps (records lost)"]["actual"] == 1
