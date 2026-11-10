from __future__ import annotations

import json

import pytest

from citefuse.ingest import ingest_jsonl
from citefuse.metrics import MetricsError, build_report, scan_thresholds
from citefuse.pipeline import run_pipeline
from scripts.seed_fixtures import generate


def _seed(storage, config, clock):
    records, _ = generate(20240517)
    payload = (
        "\n".join(json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records)
        + "\n"
    ).encode("utf-8")
    ingest_jsonl(storage, config, clock, payload, batch_id="metrics")
    return run_pipeline(storage, config, clock, "metrics")


def test_metrics_report_and_threshold_scan(storage, config, clock, tmp_path):
    run = _seed(storage, config, clock)
    report = build_report(storage, config, tmp_path / "reports", run["run_id"])
    assert report["pairwise"]["f1"] == 1.0
    assert report["cluster_exact"]["exact_rate"] == 1.0
    path = tmp_path / "reports" / f"{run['run_id']}.json"
    assert path.exists()
    first_bytes = path.read_bytes()
    payload = json.loads(first_bytes)
    assert payload["run_id"] == run["run_id"]
    build_report(storage, config, tmp_path / "reports", run["run_id"])
    assert path.read_bytes() == first_bytes

    scan = scan_thresholds(
        storage, config, tmp_path / "reports", run["run_id"], [0.5, 0.8, 0.8, 0.9]
    )
    assert [row["threshold"] for row in scan["thresholds"]] == [0.5, 0.8, 0.9]
    at_match = next(row for row in scan["thresholds"] if row["threshold"] == 0.8)
    assert at_match["f1"] == 1.0
    assert scan["thresholds"][0]["precision"] < at_match["precision"]
    assert (tmp_path / "reports" / f"{run['run_id']}_thresholds.json").exists()


def test_metrics_require_gold_labels(storage, config, clock, tmp_path):
    payload = (
        '{"record_id":"X1","source":"ORBIT","title":"Only one record"}\n'
    ).encode("utf-8")
    ingest_jsonl(storage, config, clock, payload, batch_id="nogold")
    run = run_pipeline(storage, config, clock, "nogold")
    with pytest.raises(MetricsError):
        build_report(storage, config, tmp_path / "reports", run["run_id"])
    with pytest.raises(MetricsError):
        scan_thresholds(storage, config, tmp_path / "reports", run["run_id"], [])
