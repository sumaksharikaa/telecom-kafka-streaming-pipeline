"""Pipeline configuration and the folder layout of the data root."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


@dataclass(frozen=True)
class Config:
    root: Path
    source_mode: str
    kafka: dict[str, Any]
    streaming: dict[str, Any]
    validation: dict[str, Any]
    reconciliation: dict[str, Any]
    hive_database: str

    # ----------------------------------------------------------------- topics
    def topic(self, name: str) -> str:
        return self.kafka["topics"][name]

    @property
    def bootstrap_servers(self) -> str:
        return self.kafka["bootstrap_servers"]

    # ------------------------------------------------------------------ zones
    def landing(self, topic_key: str) -> Path:
        """File-mode 'broker': Kafka-shaped JSON files, one folder per topic."""
        return self.root / "landing" / self.topic(topic_key)

    @property
    def broker_state(self) -> Path:
        """File-mode log-end offsets per topic partition (what Kafka would report)."""
        return self.root / "landing" / "_offsets.json"

    def table(self, layer: str, name: str) -> Path:
        return self.root / layer / name

    def checkpoint(self, query: str) -> Path:
        return self.root / "_checkpoints" / query

    @property
    def state_dir(self) -> Path:
        return self.root / "_state"

    @property
    def reports_dir(self) -> Path:
        return self.root / "reports"

    @property
    def expected_dir(self) -> Path:
        """Ground truth written by the simulator, used only by tests and the e2e script."""
        return self.root / "_expected"

    @property
    def hive_dir(self) -> Path:
        return self.root / "hive"


# Table locations, in one place so jobs and Hive DDL agree.
BRONZE_USAGE = ("bronze", "network_events_raw")
BRONZE_CDC = ("bronze", "subscriber_changes_raw")
SILVER_EVENTS = ("silver", "network_events")
SILVER_BACKFILL = ("silver", "network_events_late_backfill")
DLQ = ("quarantine", "network_events_dlq")
GOLD_CELL_KPI = ("gold", "cell_kpi_5min")
DIM_SUBSCRIBER = ("gold", "dim_subscriber")
GOLD_DAILY_USAGE = ("gold", "subscriber_daily_usage")


def load_config(path: str | Path | None = None, root: str | Path | None = None) -> Config:
    raw = yaml.safe_load(Path(path or os.environ.get("TELECOM_CONFIG") or DEFAULT_CONFIG).read_text())
    data_root = Path(root or os.environ.get("TELECOM_DATA_ROOT") or raw["data_root"]).resolve()
    kafka = dict(raw["kafka"])
    kafka["bootstrap_servers"] = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"])
    mode = os.environ.get("TELECOM_SOURCE_MODE", raw["source_mode"])
    if mode not in ("kafka", "file"):
        raise ValueError(f"source_mode must be 'kafka' or 'file', got {mode!r}")
    return Config(
        root=data_root,
        source_mode=mode,
        kafka=kafka,
        streaming=raw["streaming"],
        validation=raw["validation"],
        reconciliation=raw["reconciliation"],
        hive_database=raw["hive"]["database"],
    )
