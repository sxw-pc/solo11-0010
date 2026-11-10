from __future__ import annotations

import json

import pytest

from citefuse.audit import run_events
from citefuse.ingest import ingest_jsonl
from citefuse.metrics import _threshold_groups, evaluate_groups
from citefuse.pipeline import get_run_summary, run_pipeline
from scripts.seed_fixtures import generate, write_fixtures


def _payload(records):
    return (
        "\n".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for record in records
        )
        + "\n"
    ).encode("utf-8")


def _cluster_rows(storage, run_id):
    return [tuple(row) for row in storage.list_clusters(run_id)]


def test_seeded_pipeline_quality_and_idempotency(storage, config, clock):
    records, gold = generate(20240517)
    payload = _payload(records)
    batch = ingest_jsonl(storage, config, clock, payload, batch_id="seed")
    assert batch["valid_lines"] == 200

    first = run_pipeline(storage, config, clock, "seed")
    summary = get_run_summary(storage, first["run_id"])
    assert summary["attempt_count"] == 1
    assert summary["last_attempt"]["status"] == "completed"
    clusters_first = _cluster_rows(storage, first["run_id"])
    golden_first = [
        json.loads(row["golden_json"]) for row in storage.list_clusters(first["run_id"])
    ]
    second = run_pipeline(storage, config, clock, "seed")
    assert second["run_id"] == first["run_id"]
    assert _cluster_rows(storage, second["run_id"]) == clusters_first
    assert [
        json.loads(row["golden_json"]) for row in storage.list_clusters(second["run_id"])
    ] == golden_first

    all_records, groups = _threshold_groups(
        storage, config, "seed", first["run_id"], config.scoring.match_threshold
    )
    metrics = evaluate_groups(all_records, groups)
    assert metrics["pairwise"]["f1"] >= 0.90
    assert metrics["pairwise"]["precision"] == 1.0
    assert metrics["predicted_clusters"] == gold["gold_cluster_count"] == 120

    reimport = ingest_jsonl(storage, config, clock, payload, batch_id="seed")
    assert reimport["inserted"] == 0
    assert storage.count_records("seed") == 200
    attempts = storage.query(
        "SELECT * FROM run_attempts WHERE run_id=?", (first["run_id"],)
    )
    assert len(attempts) == 2


def test_seed_generation_is_byte_deterministic(tmp_path):
    first_dir = tmp_path / "one"
    second_dir = tmp_path / "two"
    write_fixtures(42, first_dir)
    write_fixtures(42, second_dir)
    assert (first_dir / "records.jsonl").read_bytes() == (second_dir / "records.jsonl").read_bytes()
    assert (first_dir / "gold.json").read_bytes() == (second_dir / "gold.json").read_bytes()
    other_dir = tmp_path / "three"
    write_fixtures(43, other_dir)
    assert (first_dir / "records.jsonl").read_bytes() != (other_dir / "records.jsonl").read_bytes()


def test_pipeline_missing_batch_and_failure_audit(
    storage, config, clock, tiny_payload, monkeypatch
):
    with pytest.raises(LookupError):
        run_pipeline(storage, config, clock, "absent")
    with pytest.raises(LookupError):
        get_run_summary(storage, "missing")

    ingest_jsonl(storage, config, clock, tiny_payload, batch_id="failing")
    monkeypatch.setattr(
        "citefuse.pipeline.generate_candidates",
        lambda *_: (_ for _ in ()).throw(RuntimeError("synthetic blocking failure")),
    )
    with pytest.raises(RuntimeError, match="synthetic blocking failure"):
        run_pipeline(storage, config, clock, "failing")

    runs = storage.list_runs("failing")
    assert len(runs) == 1 and runs[0]["status"] == "failed"
    failed_id = runs[0]["run_id"]
    attempts = storage.query(
        "SELECT status FROM run_attempts WHERE run_id=? ORDER BY attempt_id", (failed_id,)
    )
    assert [row["status"] for row in attempts] == ["failed"]
    events = run_events(storage, failed_id)
    assert events[-1]["event_type"] == "run_failed"
    assert "synthetic blocking failure" in events[-1]["detail"]["error"]
