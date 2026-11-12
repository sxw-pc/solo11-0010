"""HTTP contract tests for the semantic drift guard endpoints."""
from __future__ import annotations

import json


def _ingest(client, batch_id: str, rows: list[dict]) -> None:
    payload = ("\n".join(json.dumps(r) for r in rows)).encode()
    response = client.post(
        f"/batches?batch_id={batch_id}", content=payload,
        headers={"content-type": "application/x-ndjson"},
    )
    assert response.status_code == 201


CLEAN = {
    "record_id": "C1", "source": "ORBIT", "title": "Conventional record",
    "authors": ["Zorvath, Vaelis"], "venue": "Journal of Fictional Evidence Synthesis",
    "year": 2010, "doi": "10.5555/api.0001",
}
HEAVY = {
    "record_id": "H1", "source": "ORBIT", "title": "Era corrupted record",
    "authors": ["Quellmar, Tirune"], "venue": "JFES",
    "year": "回历1420年", "doi": "10.5555/api.0002",
}
MINOR = {
    "record_id": "M1", "source": "Orbital Bibliographic Exchange",
    "title": "Alias source record", "authors": ["Branteth, Soreth"],
    "venue": "JFES", "year": 2011, "doi": "10.5555/api.0003",
}


def test_run_response_includes_drift_summary(client):
    _ingest(client, "D1", [CLEAN, HEAVY, MINOR])
    run = client.post("/runs", json={"batch_id": "D1"}).json()
    summary = run["drift_summary"]
    assert summary["total_records"] == 3
    assert summary["tier_counts"]["heavy"] == 1
    assert summary["quarantined_records"] == 1
    assert "drift_guard" in run["timings"]


def test_quarantine_endpoint_lists_heavy_records(client):
    _ingest(client, "D2", [CLEAN, HEAVY])
    client.post("/runs", json={"batch_id": "D2"})
    payload = client.get("/drift/quarantine", params={"batch_id": "D2"}).json()
    assert payload["count"] == 1
    item = payload["items"][0]
    assert item["record_key"] == "ORBIT::H1"
    assert item["raw"]["year"] == "回历1420年"


def test_dispositions_endpoint_filters_by_tier(client):
    _ingest(client, "D3", [CLEAN, HEAVY, MINOR])
    client.post("/runs", json={"batch_id": "D3"})
    # Clean records have no disposition row; only drift is stored per record.
    all_rows = client.get("/drift/dispositions", params={"batch_id": "D3"}).json()
    keys = {item["record_key"] for item in all_rows["items"]}
    # The minor record keeps its original (pre-correction) source in the key;
    # the canonical correction is recorded in corrected_fields.
    assert "ORBIT::H1" in keys and "ORBITAL BIBLIOGRAPHIC EXCHANGE::M1" in keys
    assert "ORBIT::C1" not in keys
    minor = next(
        item for item in all_rows["items"]
        if item["record_key"] == "ORBITAL BIBLIOGRAPHIC EXCHANGE::M1"
    )
    assert minor["corrected_fields"]["source"] == "ORBIT"
    heavy = client.get(
        "/drift/dispositions", params={"batch_id": "D3", "tier": "heavy"}
    ).json()
    assert [item["record_key"] for item in heavy["items"]] == ["ORBIT::H1"]
    bad = client.get(
        "/drift/dispositions", params={"batch_id": "D3", "tier": "bogus"}
    )
    assert bad.status_code == 400
    missing = client.get("/drift/dispositions", params={"batch_id": "nope"})
    assert missing.status_code == 404


def test_drift_summary_endpoint_and_baseline(client):
    _ingest(client, "D4", [dict(CLEAN, record_id=f"C{i}") for i in range(25)])
    run = client.post("/runs", json={"batch_id": "D4"}).json()
    run_id = run["run_id"]
    summary = client.get("/drift/summary", params={"run_id": run_id}).json()
    assert summary["summary"]["total_records"] == 25

    baseline = client.get("/drift/baseline").json()
    assert baseline["latest_version"] >= 1
    assert "source" in baseline["fields"]

    rebuilt = client.post("/drift/baseline/rebuild", json={"batch_id": "D4"})
    assert rebuilt.status_code == 201
    assert rebuilt.json()["absorbed_records"] == 25


def test_audit_events_record_guard_activity(client):
    _ingest(client, "D5", [CLEAN])
    run = client.post("/runs", json={"batch_id": "D5"}).json()
    detail = client.get(f"/runs/{run['run_id']}").json()
    event_types = [event["event_type"] for event in detail["audit_events"]]
    assert "drift_guard_completed" in event_types


def test_seeded_fixture_runs_are_guarded_and_complete(client, seeded_paths):
    payload = seeded_paths[0].read_bytes()
    client.post(
        "/batches?batch_id=SEED", content=payload,
        headers={"content-type": "application/x-ndjson"},
    )
    run = client.post("/runs", json={"batch_id": "SEED"}).json()
    assert run["status"] == "completed"
    # The clean fictional fixture produces no heavy drift / quarantine.
    assert run["drift_summary"]["quarantined_records"] == 0
    # Core clustering contract is preserved with the guard enabled.
    assert run["cluster_count"] == 120
