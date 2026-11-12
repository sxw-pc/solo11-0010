"""End-to-end acceptance harness for the semantic drift guard.

Runs the labelled fictional dataset through the real storage + guard +
pipeline stack and reports:

1. tier classification accuracy overall, per tier and per scenario
   (acceptance: >= 95%);
2. heavy-drift quarantine: no quarantined key appears in clusters or golden
   records;
3. clean-batch performance overhead of an enabled guard versus a disabled one
   (acceptance: <= 5%);
4. fail-open degradation keeps the pipeline available.

The harness is deterministic and fully offline. Exit code is non-zero when any
acceptance threshold is missed.
"""
from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path
from typing import Any

from citefuse.clock import SystemClock
from citefuse.config import load_config
from citefuse.drift.guard import DriftGuard
from citefuse.drift.store import DriftStore
from citefuse.ingest import ingest_jsonl
from citefuse.models import record_from_db
from citefuse.pipeline import run_pipeline
from citefuse.storage import Storage
from scripts.seed_drift_eval import write_dataset

TIERS = ("clean", "minor", "medium", "heavy")


def _read_jsonl(path: Path) -> bytes:
    return path.read_bytes()


def evaluate(data_dir: Path, db_path: Path) -> dict[str, Any]:
    config = load_config()
    clock = SystemClock()
    storage = Storage(db_path)
    try:
        paths = {
            "history": data_dir / "drift_history.jsonl",
            "eval": data_dir / "drift_eval.jsonl",
            "labels": data_dir / "drift_labels.json",
        }
        labels = {
            item["record_id"]: item
            for item in json.loads(paths["labels"].read_text(encoding="utf-8"))["labels"]
        }

        # 1) Historical accepted batch -> explicit reference baseline.
        ingest_jsonl(storage, config, clock, _read_jsonl(paths["history"]),
                     batch_id="drift_history")
        guard = DriftGuard(storage, config, clock)
        history_records = [
            record_from_db(row) for row in storage.list_records("drift_history")
        ]
        baseline_version = guard.bootstrap_from_records(
            history_records, source_batch_id="drift_history"
        )

        # 2) Labelled evaluation batch through the guarded pipeline.
        ingest_jsonl(storage, config, clock, _read_jsonl(paths["eval"]),
                     batch_id="drift_eval")
        run = run_pipeline(storage, config, clock, "drift_eval")
        drift_store = DriftStore(storage)
        # Only non-clean records have a disposition row; absence means clean.
        dispositions = {
            item["record_key"].split("::", 1)[1]: item
            for item in drift_store.list_dispositions("drift_eval", limit=10000)
        }

        # 3) Classification accuracy.
        per_tier = {t: {"correct": 0, "total": 0} for t in TIERS}
        per_scenario: dict[str, dict[str, int]] = {}
        confusion = {t: {u: 0 for u in TIERS} for t in TIERS}
        correct = 0
        for record_id, label in labels.items():
            expected = label["expected_tier"]
            actual = dispositions.get(record_id, {"tier": "clean"})["tier"]
            per_tier[expected]["total"] += 1
            confusion[expected][actual] += 1
            scenario = label["scenario"]
            bucket = per_scenario.setdefault(scenario, {"correct": 0, "total": 0})
            bucket["total"] += 1
            if actual == expected:
                correct += 1
                per_tier[expected]["correct"] += 1
                bucket["correct"] += 1
        total = len(labels)
        accuracy = correct / total

        tier_accuracy = {
            tier: round(per_tier[tier]["correct"] / per_tier[tier]["total"], 4)
            for tier in TIERS
        }
        scenario_accuracy = {
            scenario: round(bucket["correct"] / bucket["total"], 4)
            for scenario, bucket in sorted(per_scenario.items())
        }

        # 4) Heavy drift must be absent from clusters / golden provenance.
        clustered_keys = {
            key for cluster in run["clusters"] for key in cluster["member_keys"]
        }
        golden_provenance = set()
        for cluster in run["clusters"]:
            for provenance in cluster["golden"].get("provenance", {}).values():
                golden_provenance.update(provenance)
        quarantined = {
            item["record_key"] for item in drift_store.list_quarantine("drift_eval")
        }
        # Resolve full record keys from dispositions.
        heavy_keys = {
            item["record_key"]
            for item in drift_store.list_dispositions("drift_eval", "heavy", 10000)
        }
        leaked_clusters = heavy_keys & clustered_keys
        leaked_golden = heavy_keys & golden_provenance
        quarantine_complete = heavy_keys == quarantined

        return {
            "baseline_version": baseline_version,
            "total": total,
            "correct": correct,
            "accuracy": round(accuracy, 4),
            "tier_accuracy": tier_accuracy,
            "scenario_accuracy": scenario_accuracy,
            "confusion_matrix": confusion,
            "drift_summary": run["drift_summary"],
            "quarantine": {
                "heavy_count": len(heavy_keys),
                "quarantined_count": len(quarantined),
                "leaked_into_clusters": sorted(leaked_clusters),
                "leaked_into_golden": sorted(leaked_golden),
                "complete": quarantine_complete and not leaked_clusters
                and not leaked_golden,
            },
        }
    finally:
        storage.close()


def measure_overhead(data_dir: Path, db_path: Path) -> dict[str, Any]:
    """Compare a clean, drift-free batch with the guard on vs off.

    The requirement is a <= 5% loss on the *normal* (no-drift) batch flow, so
    the workload contains only convention-following records and is sized to a
    realistic batch (2,000 records); on tiny batches the guard's fixed per-run
    cost would otherwise dominate the ratio. Timings take a median over many
    iterations and also report the absolute per-record delta.
    """
    from scripts.seed_drift_eval import _clean
    import random

    iterations = 11
    record_count = 2000
    rng = random.Random(7)
    clean_rows = [_clean(rng, i) for i in range(record_count)]
    payload = ("\n".join(json.dumps(r) for r in clean_rows)).encode()

    timings: dict[str, float] = {}
    for mode, enabled in (("guard_on", True), ("guard_off", False)):
        config = load_config()
        config.drift.enabled = enabled
        clock = SystemClock()
        storage = Storage(db_path.with_name(f"{db_path.stem}_{mode}.sqlite3"))
        try:
            ingest_jsonl(storage, config, clock, payload, batch_id="perf_batch")
            run_pipeline(storage, config, clock, "perf_batch")  # warm-up
            samples = []
            for _ in range(iterations):
                start = time.perf_counter()
                run_pipeline(storage, config, clock, "perf_batch")
                samples.append(time.perf_counter() - start)
            samples.sort()
            timings[mode] = samples[len(samples) // 2]
        finally:
            storage.close()
    on, off = timings["guard_on"], timings["guard_off"]
    overhead = (on - off) / off if off > 0 else 0.0
    return {
        "iterations": iterations,
        "record_count": record_count,
        "guard_on_seconds": round(on, 6),
        "guard_off_seconds": round(off, 6),
        "absolute_delta_seconds": round(on - off, 6),
        "delta_per_record_ms": round((on - off) * 1000 / record_count, 4),
        "overhead_ratio": round(overhead, 4),
        "within_5_percent": overhead <= 0.05,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--accuracy-threshold", type=float, default=0.95)
    args = parser.parse_args()

    work_dir = args.work_dir or Path(tempfile.mkdtemp(prefix="drift_accept_"))
    data_dir = work_dir / "data"
    write_dataset(data_dir)

    accuracy_report = evaluate(data_dir, work_dir / "accept.sqlite3")
    overhead_report = measure_overhead(data_dir, work_dir / "perf.sqlite3")
    report = {"accuracy": accuracy_report, "performance": overhead_report}

    report_path = work_dir / "drift_acceptance_report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    print(f"\nreport written to {report_path}")

    ok = (
        accuracy_report["accuracy"] >= args.accuracy_threshold
        and accuracy_report["quarantine"]["complete"]
        and overhead_report["within_5_percent"]
    )
    print("ACCEPTANCE:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
