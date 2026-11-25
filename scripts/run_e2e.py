"""End-to-end scenario: two phases of traffic through the stream and the batch jobs,
checked against the simulator's ground truth.

    python scripts/run_e2e.py                 # file mode, no broker needed
    TELECOM_SOURCE_MODE=kafka python scripts/run_e2e.py   # against a real Kafka

Each step runs as its own process (its own Spark driver), the way a scheduler would run it,
so phase 2 also proves the stream resumes from its checkpoints.
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from telecom_stream import config as C  # noqa: E402
from telecom_stream.config import load_config  # noqa: E402

FAILURES: list[str] = []


def step(*args: str) -> str:
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    print(f"\n$ {' '.join(args)}", flush=True)
    proc = subprocess.run([sys.executable, *args], env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout[-4000:], proc.stderr[-4000:], sep="\n")
        raise SystemExit(f"step failed: {' '.join(args)}")
    return proc.stdout


def batch(name: str) -> dict:
    out = step("-m", "telecom_stream.batch", name)
    return json.loads(out[out.index("{") :])


def expect(name: str, actual, expected) -> None:
    ok = actual == expected
    print(f"  [{'ok' if ok else 'FAIL'}] {name}: {actual!r}" + ("" if ok else f" (expected {expected!r})"))
    if not ok:
        FAILURES.append(name)


def main() -> None:
    root = Path(os.environ.get("TELECOM_DATA_ROOT") or tempfile.mkdtemp(prefix="telecom-e2e-"))
    if root.exists() and os.environ.get("E2E_KEEP_DATA") != "1":
        shutil.rmtree(root)
    os.environ["TELECOM_DATA_ROOT"] = str(root)
    cfg = load_config()
    print(f"data root: {root}  source mode: {cfg.source_mode}")

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    # ---------------------------------------------------------------- phase 1
    step("-m", "telecom_stream.simulator", "--phase", "1")
    step("-m", "telecom_stream.streaming_job", "--trigger", "available-now")
    r1 = batch("all")
    manifest = json.loads((cfg.expected_dir / "manifest.json").read_text())
    print("\nphase 1")
    expect("reconciliation passed", r1["reconcile"]["passed"], True)
    expect("nothing to backfill yet", r1["backfill"]["backfilled"], 0)
    expect("CDC rows applied", r1["dim-subscriber"]["new_changes"], manifest["cdc_messages"]["1"])

    # ---------------------------------------------------------------- phase 2
    step("-m", "telecom_stream.simulator", "--phase", "2")
    step("-m", "telecom_stream.streaming_job", "--trigger", "available-now")  # resumes from checkpoints
    r2 = batch("all")
    rec = r2["reconcile"]
    print("\nphase 2")
    expect("reconciliation passed", rec["passed"], True)
    expect("late events recovered from bronze", r2["backfill"]["backfilled"], manifest["late_events"])
    expect("redeliveries removed", rec["counts"]["redeliveries_removed"], manifest["duplicate_messages"])
    expect("DLQ by reason", rec["counts"]["dlq_by_reason"], manifest["invalid_by_reason"])
    expect("clean events", rec["counts"]["clean_events_loaded"], manifest["unique_valid_events"])
    expect(
        "only new CDC applied (redeliveries skipped)",
        r2["dim-subscriber"]["new_changes"],
        manifest["backdated_plan_changes"],
    )

    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Daily usage must equal the ground truth exactly, plan by plan (point-in-time, incl. backdated).
    usage = {
        (str(r["event_date"]), r["subscriber_id"], r["plan_code"], r["market"]): r.asDict()
        for r in spark.read.parquet(str(cfg.table(*C.GOLD_DAILY_USAGE))).collect()
    }
    with open(cfg.expected_dir / "expected_usage.csv") as fh:
        truth = {(r["event_date"], r["subscriber_id"], r["plan_code"], r["market"]): r for r in csv.DictReader(fh)}
    expect("daily usage rows", len(usage), len(truth))
    metrics = ["events", "total_bytes", "data_sessions", "voice_calls", "sms_count", "dropped_calls", "voice_seconds"]
    mismatched = [k for k, t in truth.items() if k not in usage or any(int(t[m]) != usage[k][m] for m in metrics)]
    expect("usage rows matching ground truth on every metric", len(truth) - len(mismatched), len(truth))
    expect(
        "unknown (roaming) subscribers kept, not dropped",
        len({k[1] for k in usage if k[2] == "UNKNOWN"}),
        manifest["unknown_subscribers"],
    )

    dim = spark.read.parquet(str(cfg.table(*C.DIM_SUBSCRIBER)))
    expect("one current row per subscriber", dim.where("is_current").count(), manifest["subscribers"])
    expect(
        "SCD2 versions",
        dim.count(),
        manifest["subscribers"] + manifest["plan_changes"] + 1 + manifest["backdated_plan_changes"],
    )
    expect(
        "disconnected subscriber flagged",
        [r["subscriber_id"] for r in dim.where("is_current AND is_deleted").collect()],
        manifest["disconnected"],
    )

    # Streaming KPIs: every emitted window equals a batch recount of the streamed events in it.
    kpi = spark.read.parquet(str(cfg.table(*C.GOLD_CELL_KPI)))
    expect("no window emitted twice", kpi.count(), kpi.select("cell_id", "window_start").distinct().count())
    silver = spark.read.parquet(str(cfg.table(*C.SILVER_EVENTS)))
    recount = silver.groupBy(
        F.window("event_ts", cfg.streaming["kpi_window"]).start.alias("window_start"), "cell_id"
    ).agg(F.count("*").alias("n"), F.sum((F.col("event_type") == "DROPPED_CALL").cast("int")).alias("d"))
    diff = (
        kpi.join(recount, ["cell_id", "window_start"], "left")
        .where((F.col("n").isNull()) | (F.col("n") != F.col("events")) | (F.col("d") != F.col("dropped_calls")))
        .count()
    )
    expect("KPI windows matching a recount of silver", diff, 0)
    print(f"  KPI windows emitted: {kpi.count()} (windows newer than the watermark stay open)")
    spark.stop()

    # ---------------------------------------------------------------- idempotence
    step("-m", "telecom_stream.streaming_job", "--trigger", "available-now")
    r3 = batch("all")
    print("\nre-run with no new data")
    expect("reconciliation passed", r3["reconcile"]["passed"], True)
    expect("backfill adds nothing", r3["backfill"]["backfilled"], 0)
    expect("no CDC re-applied", r3["dim-subscriber"]["new_changes"], 0)
    expect("clean events unchanged", r3["reconcile"]["counts"]["clean_events_loaded"], manifest["unique_valid_events"])

    print("\nreconciliation report:", rec["path"])
    if FAILURES:
        raise SystemExit(f"\nE2E FAILED: {FAILURES}")
    print("\nALL EXPECTATIONS MET")


if __name__ == "__main__":
    main()
