"""Network-event and subscriber-CDC simulator.

Builds one deterministic day of traffic and sends it in two phases, the way a real day arrives:

  phase 1  subscriber snapshot + daytime CDC, on-time events up to 20:00
  phase 2  events 20:00-24:00, plus late events (hours behind), backdated plan changes,
           redelivered CDC messages

Both phases mix in producer redeliveries (duplicate event_id) and bad records (malformed JSON,
unknown event types, invalid service codes, bad timestamps, negative usage, wrong schema version).

It also writes the ground truth to data/_expected/ so tests can check the pipeline's output
exactly: per-subscriber usage priced against the plan in effect at each event's time.

    python -m telecom_stream.simulator --phase 1            # sink = source_mode from the config
    python -m telecom_stream.simulator --phase 2 --sink kafka
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import zlib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from telecom_stream.config import Config, load_config

DAY = datetime(2025, 6, 1, tzinfo=UTC)
MARKETS = ["PHILADELPHIA", "CHICAGO", "DENVER", "ATLANTA"]
PLANS = ["MOBILE_BY_THE_GIG", "MOBILE_UNLIMITED", "MOBILE_UNLIMITED_PLUS"]
CELLS_PER_MARKET = 20
PHASE_2_STARTS = DAY + timedelta(hours=20)
EVENT_TYPES = [("DATA_SESSION", 0.60), ("VOICE_CALL", 0.22), ("SMS", 0.15), ("DROPPED_CALL", 0.03)]
SERVICE = {
    "DATA_SESSION": ["DATA_LTE", "DATA_5G"],
    "VOICE_CALL": ["VOICE_VOLTE", "VOICE_3G"],
    "DROPPED_CALL": ["VOICE_VOLTE", "VOICE_3G"],
    "SMS": ["SMS_STD"],
}


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Message:
    topic_key: str  # usage | cdc
    key: str | None
    value: str


@dataclass
class Scenario:
    phases: dict[int, list[Message]] = field(default_factory=lambda: {1: [], 2: []})
    manifest: dict[str, Any] = field(default_factory=dict)
    expected_usage: list[dict[str, Any]] = field(default_factory=list)


def build(n_subscribers: int = 1500, seed: int = 7) -> Scenario:
    rng = random.Random(seed)
    sc = Scenario()
    lsn = 0

    # ------------------------------------------------------------- subscribers + CDC
    subs: dict[str, dict[str, Any]] = {}
    history: dict[str, list[tuple[datetime, int, dict[str, Any]]]] = defaultdict(list)
    cdc_by_phase: dict[int, list[dict[str, Any]]] = {1: [], 2: []}

    def change(phase: int, op: str, sid: str, at: datetime, **attrs: Any) -> dict[str, Any]:
        nonlocal lsn
        lsn += 1
        row = {**subs[sid], **attrs}
        msg = {
            "lsn": lsn,
            "op": op,
            "subscriber_id": sid,
            "account_id": row["account_id"],
            "plan_code": row["plan_code"],
            "market": row["market"],
            "status": "DISCONNECTED" if op == "d" else row["status"],
            "changed_at": iso(at),
        }
        cdc_by_phase[phase].append(msg)
        history[sid].append((at, lsn, msg))
        subs[sid] = {k: msg[k] for k in ("account_id", "plan_code", "market", "status")}
        return msg

    snapshot_at = DAY - timedelta(days=31)
    for i in range(1, n_subscribers + 1):
        sid = f"SUB{i:07d}"
        subs[sid] = {
            "account_id": f"ACC{(i + 1) // 2:07d}",
            "plan_code": rng.choice(PLANS),
            "market": rng.choice(MARKETS),
            "status": "ACTIVE",
        }
        change(1, "c", sid, snapshot_at)

    ids = list(subs)
    plan_changers = rng.sample(ids, 60)
    for sid in plan_changers:  # daytime plan changes, sent in order
        at = DAY + timedelta(hours=rng.randint(6, 17), minutes=rng.randint(0, 59))
        change(1, "u", sid, at, plan_code=rng.choice([p for p in PLANS if p != subs[sid]["plan_code"]]))
    disconnected = rng.choice([s for s in ids if s not in plan_changers])
    disconnect_at = DAY + timedelta(hours=15)
    change(1, "d", disconnected, disconnect_at)

    # Phase 2: plan changes entered late but effective earlier in the day (backdated). They have
    # higher LSNs than everything above, yet must re-price usage from their effective time.
    backdated = rng.sample([s for s in ids if s not in plan_changers and s != disconnected], 10)
    for sid in backdated:
        at = DAY + timedelta(hours=rng.randint(9, 11), minutes=rng.randint(0, 59))
        change(2, "u", sid, at, plan_code=rng.choice([p for p in PLANS if p != subs[sid]["plan_code"]]))
    redelivered_cdc = rng.sample(cdc_by_phase[1][-60:], 5)  # same LSN sent again

    for phase in (1, 2):
        extra = redelivered_cdc if phase == 2 else []
        for msg in cdc_by_phase[phase] + extra:
            sc.phases[phase].append(Message("cdc", msg["subscriber_id"], json.dumps(msg)))

    def plan_at(sid: str, ts: datetime) -> tuple[str, str]:
        versions = sorted(history.get(sid, []), key=lambda v: (v[0], v[1]))
        current = None
        for at, _, msg in versions:
            if at <= ts:
                current = msg
        return (current["plan_code"], current["market"]) if current else ("UNKNOWN", "UNKNOWN")

    # ------------------------------------------------------------------ usage events
    roaming = [f"ROAM{i:05d}" for i in range(1, 21)]  # not in the billing system -> UNKNOWN
    cells = {m: [f"CELL-{m[:3]}-{c:03d}" for c in range(1, CELLS_PER_MARKET + 1)] for m in MARKETS}
    events: list[dict[str, Any]] = []
    seq = 0
    for sid in ids + roaming:
        market = subs[sid]["market"] if sid in subs else rng.choice(MARKETS)
        end = disconnect_at if sid == disconnected else DAY + timedelta(days=1)
        for _ in range(rng.randint(10, 50)):
            ts = DAY + timedelta(seconds=rng.randint(0, 86399))
            if ts >= end:
                continue
            etype = rng.choices([t for t, _ in EVENT_TYPES], weights=[w for _, w in EVENT_TYPES])[0]
            seq += 1
            ev = {
                "schema_version": 1,
                "event_id": f"EV{seq:09d}",
                "event_type": etype,
                "subscriber_id": sid,
                "device_id": f"IMEI{zlib.crc32(sid.encode()):010d}",
                "cell_id": rng.choice(cells[market]),
                "service_code": rng.choice(SERVICE[etype]),
                "event_ts": iso(ts),
                "bytes_up": 0,
                "bytes_down": 0,
                "duration_sec": 0,
            }
            if etype == "DATA_SESSION":
                ev["bytes_down"] = int(rng.lognormvariate(16, 1.2))
                ev["bytes_up"] = ev["bytes_down"] // rng.randint(5, 20)
                ev["duration_sec"] = rng.randint(30, 3600)
            elif etype == "VOICE_CALL":
                ev["duration_sec"] = rng.randint(10, 1800)
            elif etype == "DROPPED_CALL":
                ev["duration_sec"] = rng.randint(1, 300)
            events.append(ev)

    late_pool = [
        e
        for e in events
        if DAY + timedelta(hours=8) <= datetime.fromisoformat(e["event_ts"]) < DAY + timedelta(hours=15)
    ]
    late_ids = {e["event_id"] for e in rng.sample(late_pool, 300)}
    dup_ids = {e["event_id"] for e in rng.sample(events, 400)}

    def phase_of(e: dict[str, Any]) -> int:
        if e["event_id"] in late_ids:
            return 2
        return 1 if datetime.fromisoformat(e["event_ts"]) < PHASE_2_STARTS else 2

    # Arrival order: by event time with a few minutes of jitter (out-of-order within a partition);
    # late events arrive after everything else in phase 2.
    def arrival(e: dict[str, Any]) -> float:
        base = datetime.fromisoformat(e["event_ts"]).timestamp() + rng.uniform(-180, 180)
        return base + (10**9 if e["event_id"] in late_ids else 0)

    for e in sorted(events, key=arrival):
        p = phase_of(e)
        payload = json.dumps(e)
        sc.phases[p].append(Message("usage", e["subscriber_id"], payload))
        if e["event_id"] in dup_ids:  # producer retry: identical message right behind it
            sc.phases[p].append(Message("usage", e["subscriber_id"], payload))

    # ------------------------------------------------------------------ bad records
    bad_kinds = Counter()

    def bad(kind: str, value: str, key: str | None) -> Message:
        bad_kinds[kind] += 1
        return Message("usage", key, value)

    for i in range(250):
        base = dict(rng.choice(events))
        base["event_id"] = f"BAD{i:06d}"
        kind = [
            "MALFORMED_JSON",
            "UNSUPPORTED_SCHEMA_VERSION",
            "MISSING_REQUIRED_FIELD",
            "INVALID_EVENT_TYPE",
            "INVALID_SERVICE_CODE",
            "BAD_TIMESTAMP",
            "INVALID_USAGE_VALUE",
        ][i % 7]
        if kind == "MALFORMED_JSON":
            msg = bad(kind, json.dumps(base)[:-7], base["subscriber_id"])
        else:
            if kind == "UNSUPPORTED_SCHEMA_VERSION":
                base["schema_version"] = 9
            elif kind == "MISSING_REQUIRED_FIELD":
                base["subscriber_id"] = None
            elif kind == "INVALID_EVENT_TYPE":
                base["event_type"] = "PING"
            elif kind == "INVALID_SERVICE_CODE":
                base["service_code"] = "DATA_2G"
            elif kind == "BAD_TIMESTAMP":
                base["event_ts"] = "2025-13-45T99:00:00Z"
            else:
                base["bytes_down"] = -1024
            msg = bad(kind, json.dumps(base), base["subscriber_id"])
        phase = 1 if i % 2 == 0 else 2
        pos = rng.randint(0, len(sc.phases[phase]))
        sc.phases[phase].insert(pos, msg)

    # ------------------------------------------------------------------ ground truth
    totals: dict[tuple, Counter] = defaultdict(Counter)
    for e in events:
        ts = datetime.fromisoformat(e["event_ts"])
        plan, market = plan_at(e["subscriber_id"], ts)
        k = (ts.date().isoformat(), e["subscriber_id"], plan, market)
        c = totals[k]
        c["events"] += 1
        c["total_bytes"] += e["bytes_up"] + e["bytes_down"]
        c[
            {
                "DATA_SESSION": "data_sessions",
                "VOICE_CALL": "voice_calls",
                "SMS": "sms_count",
                "DROPPED_CALL": "dropped_calls",
            }[e["event_type"]]
        ] += 1
        if e["event_type"] == "VOICE_CALL":
            c["voice_seconds"] += e["duration_sec"]
    metrics = ["events", "total_bytes", "data_sessions", "voice_calls", "sms_count", "dropped_calls", "voice_seconds"]
    sc.expected_usage = [
        {"event_date": k[0], "subscriber_id": k[1], "plan_code": k[2], "market": k[3], **{m: c[m] for m in metrics}}
        for k, c in sorted(totals.items())
    ]
    sc.manifest = {
        "day": DAY.date().isoformat(),
        "unique_valid_events": len(events),
        "duplicate_messages": len(dup_ids),
        "late_events": len(late_ids),
        "invalid_by_reason": dict(sorted(bad_kinds.items())),
        "usage_messages": {str(p): sum(m.topic_key == "usage" for m in sc.phases[p]) for p in (1, 2)},
        "cdc_messages": {str(p): sum(m.topic_key == "cdc" for m in sc.phases[p]) for p in (1, 2)},
        "subscribers": n_subscribers,
        "unknown_subscribers": len(roaming),
        "plan_changes": len(plan_changers),
        "backdated_plan_changes": len(backdated),
        "disconnected": [disconnected],
        "redelivered_cdc": len(redelivered_cdc),
        "late_event_ids_sample": sorted(late_ids)[:5],
    }
    return sc


# ---------------------------------------------------------------------- sinks
class FileSink:
    """Kafka-shaped JSON files: what a broker would hand to a consumer, with real offsets."""

    def __init__(self, cfg: Config, records_per_file: int = 4000):
        self.cfg = cfg
        self.n = cfg.kafka["partitions"]
        self.per_file = records_per_file
        self.offsets: dict[str, dict[str, int]] = (
            json.loads(cfg.broker_state.read_text()) if cfg.broker_state.exists() else {}
        )

    def send(self, messages: list[Message], tag: str) -> None:
        now = datetime.now(UTC).isoformat()
        by_topic: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for m in messages:
            topic = self.cfg.topic(m.topic_key)
            part = zlib.crc32((m.key or "").encode()) % self.n
            offs = self.offsets.setdefault(topic, {})
            off = offs.get(str(part), 0)
            offs[str(part)] = off + 1
            by_topic[m.topic_key].append(
                {"key": m.key, "value": m.value, "topic": topic, "partition": part, "offset": off, "timestamp": now}
            )
        for topic_key, rows in by_topic.items():
            folder = self.cfg.landing(topic_key)
            folder.mkdir(parents=True, exist_ok=True)
            for i in range(0, len(rows), self.per_file):
                tmp = folder / f".part-{tag}-{i // self.per_file:04d}.json.tmp"
                tmp.write_text("".join(json.dumps(r) + "\n" for r in rows[i : i + self.per_file]))
                tmp.rename(folder / f"part-{tag}-{i // self.per_file:04d}.json")  # atomic publish
        self.cfg.broker_state.parent.mkdir(parents=True, exist_ok=True)
        self.cfg.broker_state.write_text(json.dumps(self.offsets, indent=2, sort_keys=True))


class KafkaSink:
    def __init__(self, cfg: Config):
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        self.cfg = cfg
        admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})
        existing = set(admin.list_topics(timeout=30).topics)
        new = [
            NewTopic(cfg.topic(t), num_partitions=cfg.kafka["partitions"], replication_factor=1)
            for t in ("usage", "cdc", "dlq")
            if cfg.topic(t) not in existing
        ]
        for _topic, fut in admin.create_topics(new).items() if new else []:
            try:
                fut.result()
            except Exception as exc:  # created concurrently is fine
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise
        # Idempotent producer: retries can't create broker-side duplicates. The duplicates in the
        # scenario are application-level redeliveries, which is what the stream's dedup handles.
        self.producer = Producer(
            {
                "bootstrap.servers": cfg.bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 20,
                "compression.type": "lz4",
            }
        )
        self.errors: list[str] = []

    def _report(self, err, _msg) -> None:
        if err is not None:
            self.errors.append(str(err))

    def send(self, messages: list[Message], tag: str) -> None:
        for i, m in enumerate(messages):
            self.producer.produce(self.cfg.topic(m.topic_key), key=m.key, value=m.value, on_delivery=self._report)
            if i % 5000 == 0:
                self.producer.poll(0)
        remaining = self.producer.flush(60)
        if remaining or self.errors:
            raise RuntimeError(f"{remaining} undelivered, errors: {self.errors[:3]}")


def write_expected(cfg: Config, sc: Scenario) -> None:
    cfg.expected_dir.mkdir(parents=True, exist_ok=True)
    (cfg.expected_dir / "manifest.json").write_text(json.dumps(sc.manifest, indent=2))
    with open(cfg.expected_dir / "expected_usage.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(sc.expected_usage[0]))
        w.writeheader()
        w.writerows(sc.expected_usage)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--phase", choices=["1", "2", "all"], required=True)
    p.add_argument("--sink", choices=["kafka", "file"], default=None)
    p.add_argument("--subscribers", type=int, default=1500)
    args = p.parse_args()
    cfg = load_config()
    sc = build(args.subscribers)
    sink = KafkaSink(cfg) if (args.sink or cfg.source_mode) == "kafka" else FileSink(cfg)
    for phase in [1, 2] if args.phase == "all" else [int(args.phase)]:
        sink.send(sc.phases[phase], f"p{phase}")
        print(f"phase {phase}: sent {len(sc.phases[phase])} messages")
    write_expected(cfg, sc)
    print(json.dumps(sc.manifest, indent=2))


if __name__ == "__main__":
    main()
