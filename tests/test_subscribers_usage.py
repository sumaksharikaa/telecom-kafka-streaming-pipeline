from __future__ import annotations

from datetime import datetime

from pyspark.sql import functions as F

from telecom_stream.subscribers import merge, rebuild_timelines
from telecom_stream.usage import daily_usage

COLS = ["subscriber_id", "account_id", "plan_code", "market", "status", "changed_at", "lsn", "op"]


def ts(h, m=0, day=1):
    return datetime(2025, 6, day, h, m)


def changes(spark, rows):
    return spark.createDataFrame(rows, COLS)


def timeline(dim, sid="S1"):
    return [
        (r["plan_code"], r["valid_from"], r["is_current"])
        for r in dim.where(F.col("subscriber_id") == sid).orderBy("valid_from").collect()
    ]


def test_initial_build_and_open_end(spark):
    dim = rebuild_timelines(
        changes(
            spark,
            [
                ("S1", "A1", "GIG", "PHL", "ACTIVE", ts(0), 1, "c"),
                ("S1", "A1", "UNL", "PHL", "ACTIVE", ts(12), 2, "u"),
            ],
        )
    )
    rows = dim.orderBy("valid_from").collect()
    assert [r["plan_code"] for r in rows] == ["GIG", "UNL"]
    assert rows[0]["valid_to"] == ts(12)
    assert rows[1]["is_current"] and rows[1]["valid_to"].year == 9999


def test_backdated_change_is_inserted_in_effective_order(spark):
    dim = rebuild_timelines(
        changes(
            spark,
            [
                ("S1", "A1", "GIG", "PHL", "ACTIVE", ts(0), 1, "c"),
                ("S1", "A1", "UNL", "PHL", "ACTIVE", ts(12), 2, "u"),
            ],
        )
    )
    # Entered later (LSN 3) but effective at 09:00.
    late = changes(spark, [("S1", "A1", "PLUS", "PHL", "ACTIVE", ts(9), 3, "u")])
    out = merge(dim, late)
    assert timeline(out) == [("GIG", ts(0), False), ("PLUS", ts(9), False), ("UNL", ts(12), True)]


def test_merge_leaves_other_subscribers_untouched(spark):
    dim = rebuild_timelines(
        changes(
            spark,
            [
                ("S1", "A1", "GIG", "PHL", "ACTIVE", ts(0), 1, "c"),
                ("S2", "A2", "UNL", "CHI", "ACTIVE", ts(0), 2, "c"),
            ],
        )
    )
    out = merge(dim, changes(spark, [("S1", "A1", "UNL", "PHL", "ACTIVE", ts(8), 3, "u")]))
    s2_before = dim.where("subscriber_id = 'S2'").select("subscriber_sk", "valid_to").collect()
    s2_after = out.where("subscriber_id = 'S2'").select("subscriber_sk", "valid_to").collect()
    assert s2_before == s2_after
    assert out.count() == 3


def test_redelivered_lsn_is_applied_once_and_same_time_tie_goes_to_higher_lsn(spark):
    dim = rebuild_timelines(
        changes(
            spark,
            [
                ("S1", "A1", "GIG", "PHL", "ACTIVE", ts(0), 1, "c"),
                ("S1", "A1", "UNL", "PHL", "ACTIVE", ts(10), 2, "u"),
                ("S1", "A1", "UNL", "PHL", "ACTIVE", ts(10), 2, "u"),  # same LSN twice
                ("S1", "A1", "PLUS", "PHL", "ACTIVE", ts(10), 3, "u"),  # same effective time, later LSN
            ],
        )
    )
    assert timeline(dim) == [("GIG", ts(0), False), ("PLUS", ts(10), True)]


def test_delete_closes_subscriber(spark):
    dim = rebuild_timelines(
        changes(
            spark,
            [
                ("S1", "A1", "GIG", "PHL", "ACTIVE", ts(0), 1, "c"),
                ("S1", "A1", "GIG", "PHL", "DISCONNECTED", ts(15), 2, "d"),
            ],
        )
    )
    current = dim.where("is_current").first()
    assert current["is_deleted"] and current["status"] == "DISCONNECTED"


def _events(spark, rows):
    cols = [
        "event_id",
        "event_type",
        "subscriber_id",
        "event_ts",
        "event_date",
        "bytes_up",
        "bytes_down",
        "duration_sec",
    ]
    return spark.createDataFrame(rows, cols)


def test_daily_usage_splits_at_plan_change_and_keeps_unknowns(spark):
    dim = rebuild_timelines(
        changes(
            spark,
            [
                ("S1", "A1", "GIG", "PHL", "ACTIVE", ts(0), 1, "c"),
                ("S1", "A1", "UNL", "PHL", "ACTIVE", ts(12), 2, "u"),
            ],
        )
    )
    d = ts(0).date()
    events = _events(
        spark,
        [
            ("e1", "DATA_SESSION", "S1", ts(11, 59), d, 10, 90, 5),
            ("e2", "DATA_SESSION", "S1", ts(12), d, 20, 80, 5),  # exactly at the change -> new plan
            ("e2", "DATA_SESSION", "S1", ts(12), d, 20, 80, 5),  # duplicate never double-counts
            ("e3", "VOICE_CALL", "S1", ts(13), d, 0, 0, 120),
            ("e4", "SMS", "ROAM1", ts(9), d, 0, 0, 0),
        ],
    )
    out = {(r["subscriber_id"], r["plan_code"]): r for r in daily_usage(events, dim).collect()}
    assert out[("S1", "GIG")]["total_bytes"] == 100
    assert out[("S1", "UNL")]["events"] == 2
    assert out[("S1", "UNL")]["voice_seconds"] == 120
    assert out[("ROAM1", "UNKNOWN")]["market"] == "UNKNOWN"
    assert sum(r["events"] for r in out.values()) == 4
