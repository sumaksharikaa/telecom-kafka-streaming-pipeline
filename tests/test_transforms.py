from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from conftest import event
from telecom_stream.transforms import cell_kpis, dlq_records, parse_cdc, parse_usage, valid_events


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (json.dumps(event()), None),
        ('{"event_id": "EV1", "event_type": ', "MALFORMED_JSON"),
        (None, "MALFORMED_JSON"),
        (json.dumps(event(schema_version=9)), "UNSUPPORTED_SCHEMA_VERSION"),
        (json.dumps(event(subscriber_id=None)), "MISSING_REQUIRED_FIELD"),
        (json.dumps(event(cell_id="  ")), "MISSING_REQUIRED_FIELD"),
        (json.dumps(event(event_type="PING")), "INVALID_EVENT_TYPE"),
        (json.dumps(event(service_code="DATA_2G")), "INVALID_SERVICE_CODE"),
        (json.dumps(event(event_ts="2025-13-45T99:00:00Z")), "BAD_TIMESTAMP"),
        (json.dumps(event(bytes_down=-1)), "INVALID_USAGE_VALUE"),
        (json.dumps(event(duration_sec=90000)), "INVALID_USAGE_VALUE"),
    ],
)
def test_validation_reason(bronze_df, cfg, value, reason):
    row = parse_usage(bronze_df([value]), cfg).first()
    assert row["dq_reason"] == reason


def test_every_record_is_either_valid_or_dlq(bronze_df, cfg):
    values = [json.dumps(event(event_id=f"EV{i}")) for i in range(5)] + ["not json", json.dumps(event(event_type="X"))]
    parsed = parse_usage(bronze_df(values), cfg)
    assert valid_events(parsed).count() + dlq_records(parsed).count() == len(values)


def test_dlq_keeps_original_bytes_and_offsets(bronze_df, cfg):
    dlq = dlq_records(parse_usage(bronze_df([json.dumps(event()), "{broken"], start_offset=41), cfg)).first()
    assert dlq["value"] == "{broken"
    assert dlq["kafka_offset"] == 42
    assert dlq["dq_reason"] == "MALFORMED_JSON"


def test_valid_event_columns(bronze_df, cfg):
    row = valid_events(parse_usage(bronze_df([json.dumps(event())]), cfg)).first()
    assert str(row["event_date"]) == "2025-06-01"
    assert row["event_hour"] == 10
    assert row["bytes_up"] + row["bytes_down"] == 1000


def test_cell_kpis_drop_rate(bronze_df, cfg):
    values = [
        json.dumps(event(event_id="a", event_type="VOICE_CALL", service_code="VOICE_VOLTE")),
        json.dumps(
            event(event_id="b", event_type="VOICE_CALL", service_code="VOICE_VOLTE", event_ts="2025-06-01T10:01:00Z")
        ),
        json.dumps(
            event(event_id="c", event_type="VOICE_CALL", service_code="VOICE_VOLTE", event_ts="2025-06-01T10:02:00Z")
        ),
        json.dumps(
            event(event_id="d", event_type="DROPPED_CALL", service_code="VOICE_VOLTE", event_ts="2025-06-01T10:03:00Z")
        ),
        json.dumps(event(event_id="e", event_ts="2025-06-01T10:07:00Z")),  # next window
    ]
    kpis = cell_kpis(valid_events(parse_usage(bronze_df(values), cfg)), "5 minutes").orderBy("window_start").collect()
    assert [k["events"] for k in kpis] == [4, 1]
    assert kpis[0]["drop_rate"] == 0.25
    assert kpis[1]["drop_rate"] is None  # no call attempts -> no rate, not 0
    assert kpis[1]["data_mb"] == 0.001


def test_parse_cdc_drops_unusable_rows(bronze_df):
    msgs = [
        {"lsn": 1, "op": "c", "subscriber_id": "S1", "plan_code": "P", "changed_at": "2025-06-01T00:00:00Z"},
        {"lsn": 2, "op": "x", "subscriber_id": "S1"},  # unknown op
        {"op": "u", "subscriber_id": "S1"},  # no lsn
    ]
    out = parse_cdc(bronze_df(msgs)).collect()
    assert [r["lsn"] for r in out] == [1]
    assert out[0]["changed_at"] is not None


def test_bad_record_does_not_break_neighbours(bronze_df, cfg):
    parsed = parse_usage(bronze_df(["\x00\x01binary", json.dumps(event())]), cfg)
    assert parsed.where(F.col("dq_reason").isNull()).count() == 1
