"""HTTP contract tests for the incremental add-on routes."""
from __future__ import annotations

import json


def _payload(records):
    return (
        "\n".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
            for record in records
        )
        + "\n"
    ).encode("utf-8")


def test_incremental_api_contract_and_snapshot_isolation(client, seeded_paths):
    from scripts.seed_fixtures import generate

    records, _ = generate(20240517)
    split = 95
    client.post(
        "/batches?batch_id=IINC",
        content=_payload(records[:split]),
        headers={"content-type": "application/x-ndjson"},
    )
    base = client.post("/runs", json={"batch_id": "IINC"}).json()
    client.post(
        "/batches?batch_id=IINC",
        content=_payload(records[split:]),
        headers={"content-type": "application/x-ndjson"},
    )

    response = client.post(
        "/incremental/runs", json={"batch_id": "IINC"}
    )
    assert response.status_code == 201
    incr = response.json()
    assert incr["mode"] == "incremental"
    assert incr["base_run_id"] == base["run_id"]
    assert incr["run_id"] != base["run_id"]
    assert incr["counters"]["new_records"] == 200 - split
    assert incr["cluster_count"] == 120

    # The incremental run is fully visible through the existing read routes.
    clusters = client.get(
        "/clusters", params={"run_id": incr["run_id"]}
    ).json()["clusters"]
    assert len(clusters) == 120
    golden = client.get(
        "/golden/export",
        params={"run_id": incr["run_id"], "format": "jsonl"},
    )
    assert len(golden.text.splitlines()) == 120
    metrics = client.get(
        "/metrics/report", params={"run_id": incr["run_id"]}
    ).json()
    assert metrics["pairwise"]["f1"] == 1.0

    # Task introspection endpoints.
    tasks = client.get(
        "/incremental/tasks", params={"batch_id": "IINC"}
    ).json()
    assert len(tasks) == 1 and tasks[0]["status"] == "completed"
    task_id = tasks[0]["task_id"]
    detail = client.get(f"/incremental/tasks/{task_id}").json()
    assert detail["run_id"] == incr["run_id"]

    # Idempotent replay over HTTP.
    replay = client.post(
        "/incremental/runs", json={"batch_id": "IINC"}
    ).json()
    assert replay["idempotent_replay"] is True
    assert replay["run_id"] == incr["run_id"]

    # Baseline snapshot remains independently readable and unchanged.
    baseline_clusters = client.get(
        "/clusters", params={"run_id": base["run_id"]}
    ).json()["clusters"]
    assert len(baseline_clusters) == base["cluster_count"]


def test_incremental_api_error_mapping(client, tiny_payload):
    # Unknown batch -> 404.
    assert client.post(
        "/incremental/runs", json={"batch_id": "MISSING"}
    ).status_code == 404

    # Batch without any baseline run -> 400.
    client.post(
        "/batches?batch_id=NOBASE",
        content=tiny_payload,
        headers={"content-type": "application/x-ndjson"},
    )
    response = client.post(
        "/incremental/runs", json={"batch_id": "NOBASE"}
    )
    assert response.status_code == 400
    assert "baseline" in response.json()["detail"]

    # Unknown baseline run id -> 404.
    response = client.post(
        "/incremental/runs",
        json={"batch_id": "NOBASE", "base_run_id": "run_0000000000000000"},
    )
    assert response.status_code == 404

    # Unknown task id -> 404.
    assert client.get(
        "/incremental/tasks/incr_0000000000000000"
    ).status_code == 404

    # Validation error on the request envelope -> 422.
    assert client.post(
        "/incremental/runs", json={"batch_id": ""}
    ).status_code == 422
