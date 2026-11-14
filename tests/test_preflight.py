from __future__ import annotations

import json

from fastapi.testclient import TestClient

from citefuse.api import create_app
from citefuse.audit import run_events
from citefuse.ingest import ingest_jsonl
from citefuse.metrics import build_report
from citefuse.pipeline import get_run_summary, run_pipeline
from citefuse.storage import Storage, json_dumps


def _payload(records):
    return (
        "\n".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for record in records
        )
        + "\n"
    ).encode("utf-8")


def _low_quality_records(count=20):
    """Placeholder titles and no venue/year spread: degenerate bucket keys."""
    return [
        {
            "record_id": f"L{index:03d}",
            "source": "ORBIT",
            "title": "N/A" if index % 2 else "Untitled",
            "authors": ["Zorvath, Vaelis"],
            "year": 2020,
        }
        for index in range(count)
    ]


def test_preflight_alerts_surface_in_runs_and_report(client):
    payload = _payload(_low_quality_records())
    assert client.post(
        "/batches?batch_id=LOWQ", content=payload,
        headers={"content-type": "application/x-ndjson"},
    ).status_code == 201

    run = client.post("/runs", json={"batch_id": "LOWQ"}).json()
    preflight = run["blocking_stats"]["preflight"]
    assert preflight["record_count"] == 20
    assert preflight["degraded"] is False
    assert preflight["status"] == "critical"
    assert preflight["fields"]["title"]["anomaly_rate"] == 1.0
    assert preflight["fields"]["venue"]["missing_rate"] == 1.0
    alerts = {(a["field"], a["metric"], a["level"]) for a in preflight["alerts"]}
    assert ("title", "anomaly_rate", "critical") in alerts
    assert ("venue", "missing_rate", "critical") in alerts
    # Candidate volume is explainable from the same payload: one degenerate
    # author_year bucket of 20 records -> C(20, 2) pairs.
    assert run["candidate_pairs"] == 190
    assert run["blocking_stats"]["union_count"] == run["candidate_pairs"]

    listed = client.get("/runs", params={"batch_id": "LOWQ"}).json()
    assert listed[0]["blocking_stats"]["preflight"] == preflight
    detail = client.get(f"/runs/{run['run_id']}").json()
    assert detail["blocking_stats"]["preflight"] == preflight
    report = client.get("/metrics/report", params={"run_id": run["run_id"]})
    assert report.status_code == 409  # no gold labels, but preflight stays in /runs


def test_preflight_summaries_are_bound_to_their_own_run(storage, config, clock):
    # Venue missing rate 0.2: below the default warn threshold (0.25) but
    # above the tightened one (0.10), so the two runs must grade differently.
    records = []
    for index in range(20):
        record = {
            "record_id": f"V{index:03d}",
            "source": "ORBIT",
            "title": f"Distinct synthetic topic {index:03d} cohort",
            "authors": [f"Family{index:03d}, Given"],
            "venue": "Journal of Fictional Evidence Synthesis",
            "year": 2020,
            "pages": "1-9",
            "doi": f"10.5555/tworun.{index:03d}",
        }
        if index >= 16:
            record["venue"] = None
        records.append(record)
    ingest_jsonl(storage, config, clock, _payload(records), batch_id="TWORUN")

    relaxed = run_pipeline(storage, config, clock, "TWORUN")
    tightened = config.model_copy(deep=True)
    tightened.preflight.warn_missing_rate = 0.10
    strict_run = run_pipeline(storage, tightened, clock, "TWORUN")
    assert relaxed["run_id"] != strict_run["run_id"]

    relaxed_pf = relaxed["blocking_stats"]["preflight"]
    strict_pf = strict_run["blocking_stats"]["preflight"]
    assert relaxed_pf["fields"]["venue"]["missing_rate"] == 0.2
    assert strict_pf["fields"]["venue"]["missing_rate"] == 0.2
    assert relaxed_pf["alerts"] == []
    assert any(
        alert["field"] == "venue" and alert["metric"] == "missing_rate"
        for alert in strict_pf["alerts"]
    )
    # Each stored run keeps its own summary; reloading does not cross them.
    assert get_run_summary(storage, relaxed["run_id"])["blocking_stats"]["preflight"] == relaxed_pf
    assert get_run_summary(storage, strict_run["run_id"])["blocking_stats"]["preflight"] == strict_pf


def test_all_empty_batch_degrades_without_division_by_zero(storage, config, clock):
    batch = ingest_jsonl(storage, config, clock, b'{"broken"\n{"also broken"\n', batch_id="EMPTY")
    assert batch["valid_lines"] == 0
    assert batch["bad_lines"] == 2

    result = run_pipeline(storage, config, clock, "EMPTY")
    preflight = result["blocking_stats"]["preflight"]
    assert result["candidate_pairs"] == 0
    assert preflight["degraded"] is True
    assert preflight["reason"] == "no_valid_records"
    assert preflight["record_count"] == 0
    assert preflight["alerts"] == []
    for field in ("title", "authors", "venue", "year", "pages", "doi"):
        assert preflight["fields"][field]["missing_rate"] is None
        assert preflight["fields"][field]["anomaly_rate"] is None
    events = run_events(storage, result["run_id"])
    assert events[0]["event_type"] == "preflight_degraded"
    assert events[0]["detail"]["reason"] == "no_valid_records"
    assert events[-1]["event_type"] == "run_completed"


def test_preflight_failure_degrades_and_never_blocks_pipeline(
    storage, config, clock, tiny_payload, monkeypatch
):
    ingest_jsonl(storage, config, clock, tiny_payload, batch_id="DEGRADE")
    monkeypatch.setattr(
        "citefuse.pipeline.preflight_summary",
        lambda *_: (_ for _ in ()).throw(RuntimeError("synthetic preflight failure")),
    )
    result = run_pipeline(storage, config, clock, "DEGRADE")
    preflight = result["blocking_stats"]["preflight"]
    assert result["status"] == "completed"
    assert result["cluster_count"] == 4
    assert preflight["degraded"] is True
    assert preflight["fields"] == {} and preflight["alerts"] == []
    assert "synthetic preflight failure" in preflight["reason"]
    events = run_events(storage, result["run_id"])
    assert events[0]["event_type"] == "preflight_degraded"
    assert "synthetic preflight failure" in events[0]["detail"]["reason"]
    assert events[-1]["event_type"] == "run_completed"


def test_legacy_runs_without_preflight_stay_readable(storage, config, clock, tiny_payload):
    ingest_jsonl(storage, config, clock, tiny_payload, batch_id="LEGACY")
    run = run_pipeline(storage, config, clock, "LEGACY")
    run_id = run["run_id"]
    # Simulate a historical database row written before preflight existed.
    storage.execute(
        "UPDATE runs SET blocking_stats_json=? WHERE run_id=?",
        (json_dumps({"enabled": {"year_venue": True}, "strategy_counts": {}, "union_count": 3}), run_id),
    )

    summary = get_run_summary(storage, run_id)
    assert "preflight" not in summary["blocking_stats"]
    report = build_report(storage, config, storage.path.parent / "reports", run_id)
    assert report["preflight"] is None
    assert report["pairwise"]["f1"] == 1.0


def test_legacy_database_boots_and_serves_runs(tmp_path, clock):
    """A pre-upgrade database file must serve /runs without a KeyError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)
    legacy = Storage(data_dir / "citefuse.sqlite3")
    legacy.save_batch({
        "batch_id": "OLD", "name": None, "input_hash": "0" * 64,
        "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z",
        "total_lines": 1, "valid_lines": 1, "bad_lines": 0, "status": "ingested",
    })
    legacy.save_run({
        "run_id": "run_legacy", "batch_id": "OLD", "config_hash": "legacy",
        "input_rows": 1, "bad_rows": 0, "candidate_pairs": 0, "matched_edges": 0,
        "cluster_count": 0, "golden_count": 0, "timings": {},
        "blocking_stats": {"enabled": {}, "strategy_counts": {}, "union_count": 0},
        "status": "completed", "created_at": "2024-01-01T00:00:00Z",
    })
    legacy.close()

    app = create_app(data_dir=data_dir, clock=clock)
    with TestClient(app) as client:
        listed = client.get("/runs")
        assert listed.status_code == 200
        assert listed.json()[0]["run_id"] == "run_legacy"
        assert "preflight" not in listed.json()[0]["blocking_stats"]
        detail = client.get("/runs/run_legacy")
        assert detail.status_code == 200
        assert detail.json()["blocking_stats"]["union_count"] == 0


def test_default_fixture_stays_clean_and_unchanged(storage, config, clock, seeded_paths):
    payload = seeded_paths[0].read_bytes()
    ingest_jsonl(storage, config, clock, payload, batch_id="SEED")
    result = run_pipeline(storage, config, clock, "SEED")
    assert result["cluster_count"] == 120
    preflight = result["blocking_stats"]["preflight"]
    assert preflight["record_count"] == 200
    assert preflight["status"] == "ok"
    assert preflight["alerts"] == []
    report = build_report(storage, config, storage.path.parent / "reports", result["run_id"])
    assert report["pairwise"]["f1"] == 1.0
    assert report["preflight"] == preflight


def test_preflight_can_be_disabled(storage, config, clock, tiny_payload):
    disabled = config.model_copy(deep=True)
    disabled.preflight.enabled = False
    ingest_jsonl(storage, disabled, clock, tiny_payload, batch_id="OFF")
    result = run_pipeline(storage, disabled, clock, "OFF")
    preflight = result["blocking_stats"]["preflight"]
    assert preflight["enabled"] is False
    assert preflight["status"] == "disabled"
    assert result["cluster_count"] == 4
