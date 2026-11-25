"""Batch jobs that run after the stream (e.g. hourly / nightly from a scheduler).

python -m telecom_stream.batch dim-subscriber   # incremental SCD2 from CDC
python -m telecom_stream.batch backfill         # recover late events from bronze
python -m telecom_stream.batch daily-usage [--date 2025-06-01]
python -m telecom_stream.batch register-hive
python -m telecom_stream.batch reconcile        # exits 1 if any check fails
python -m telecom_stream.batch all              # the above, in order
"""

from __future__ import annotations

import argparse
import json
import sys

from telecom_stream.config import load_config
from telecom_stream.hive import register_tables
from telecom_stream.reconcile import reconcile
from telecom_stream.spark import get_spark
from telecom_stream.subscribers import build_dim_subscriber
from telecom_stream.usage import backfill_late_events, build_daily_usage

STEPS = ["dim-subscriber", "backfill", "daily-usage", "register-hive", "reconcile"]


def run(step: str, dates: list[str] | None = None) -> dict:
    cfg = load_config()
    spark = get_spark(f"telecom-batch-{step}", cfg, hive=True)
    results: dict = {}
    for s in STEPS if step == "all" else [step]:
        if s == "dim-subscriber":
            results[s] = build_dim_subscriber(spark, cfg)
        elif s == "backfill":
            results[s] = backfill_late_events(spark, cfg)
        elif s == "daily-usage":
            results[s] = build_daily_usage(spark, cfg, dates)
        elif s == "register-hive":
            results[s] = register_tables(spark, cfg)
        elif s == "reconcile":
            results[s] = reconcile(spark, cfg)
    return results


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("step", choices=STEPS + ["all"])
    p.add_argument("--date", action="append", help="event_date partition(s) to rebuild (daily-usage)")
    args = p.parse_args()
    results = run(args.step, args.date)
    print(json.dumps(results, indent=2, default=str))
    rec = results.get("reconcile")
    if rec is not None and not rec["passed"]:
        failed = [c["check"] for c in rec["checks"] if not c["ok"]]
        print(f"RECONCILIATION FAILED: {failed}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
