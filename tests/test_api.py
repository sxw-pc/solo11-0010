from __future__ import annotations


def _post_seed(client, seeded_paths):
    payload = seeded_paths[0].read_bytes()
    response = client.post(
        "/batches?batch_id=API&name=Seeded", content=payload,
        headers={"content-type": "application/x-ndjson"},
    )
    assert response.status_code == 201
    assert response.json()["valid_lines"] == 200
    return payload


def test_end_to_end_api_contract(client, seeded_paths):
    payload = _post_seed(client, seeded_paths)
    duplicate = client.post(
        "/batches?batch_id=API", content=payload,
        headers={"content-type": "application/x-ndjson"},
    )
    assert duplicate.json()["inserted"] == 0
    assert client.get("/health").json()["status"] == "ok"
    assert client.get("/batches").json()[0]["batch_id"] == "API"
    detail = client.get("/batches/API").json()
    assert detail["total_lines"] == detail["valid_lines"] == 200
    assert detail["dead_letters"] == []

    run_response = client.post("/runs", json={"batch_id": "API"})
    assert run_response.status_code == 201
    run = run_response.json()
    run_id = run["run_id"]
    assert run["cluster_count"] == 120
    assert client.get("/runs", params={"batch_id": "API"}).json()[0]["run_id"] == run_id
    run_detail = client.get(f"/runs/{run_id}").json()
    assert run_detail["audit_events"][-1]["event_type"] == "run_completed"

    clusters = client.get("/clusters", params={"run_id": run_id}).json()["clusters"]
    assert len(clusters) == 120
    multi = next(item for item in clusters if item["member_count"] == 3)
    cluster = client.get(
        f"/clusters/{multi['cluster_id']}", params={"run_id": run_id}
    ).json()
    assert len(cluster["members"]) == cluster["golden"]["member_count"]
    assert cluster["edges"]

    queue = client.get("/review/next", params={"batch_id": "API", "n": 2}).json()
    assert len(queue["pairs"]) == 2
    pair = queue["pairs"][0]
    assert "details" in pair and len(pair["records"]) == 2

    assert client.get("/metrics/report", params={"run_id": run_id}).json()["pairwise"]["f1"] == 1.0
    scan = client.get(
        "/metrics/thresholds", params={"run_id": run_id, "grid": "0.5,0.8"}
    ).json()
    assert [row["threshold"] for row in scan["thresholds"]] == [0.5, 0.8]

    golden_jsonl = client.get("/golden/export", params={"run_id": run_id, "format": "jsonl"})
    assert golden_jsonl.status_code == 200
    assert len(golden_jsonl.text.splitlines()) == 120
    assert "attachment" in golden_jsonl.headers["content-disposition"]
    golden_csv = client.get("/golden/export", params={"run_id": run_id, "format": "csv"})
    assert golden_csv.status_code == 200
    assert golden_csv.text.splitlines()[0].startswith("cluster_id,title,authors")

    invalid_decision = client.post("/review/decisions", json={
        "batch_id": "API", "pair_key": pair["pair_key"], "decision": "maybe",
    })
    assert invalid_decision.status_code == 422
    missing_pair = client.post("/review/decisions", json={
        "batch_id": "API", "pair_key": "missing||pair", "decision": "same",
    })
    assert missing_pair.status_code == 404
    decision = client.post("/review/decisions", json={
        "batch_id": "API", "pair_key": pair["pair_key"], "decision": "different",
        "reviewer": "qa", "note": "API contract",
    })
    assert decision.status_code == 201
    payload = decision.json()
    assert "run" not in payload
    assert payload["decision"]["decision"] == "different"
    assert payload["decision"]["view_version"] == 1
    # Instant reflux: the pair leaves the review queue without a rerun.
    queue_after = client.get("/review/next", params={"batch_id": "API", "n": 100}).json()
    assert all(item["pair_key"] != pair["pair_key"] for item in queue_after["pairs"])
    view = client.get("/review/view", params={"batch_id": "API"}).json()
    assert view["view_version"] == 1
    assert view["counts"] == {"same": 0, "different": 1}
    assert client.get("/review/decisions", params={"batch_id": "API"}).json()[0]["decision"] == "different"

    assert client.get("/batches/missing").status_code == 404
    assert client.get("/runs/missing").status_code == 404
    assert client.get("/clusters/missing").status_code == 404
    assert client.get("/metrics/report", params={"run_id": "missing"}).status_code == 404
    assert client.get("/golden/export", params={"run_id": run_id, "format": "xml"}).status_code == 400
    assert client.post("/batches?batch_id=bad id", content=b"{}", headers={
        "content-type": "application/x-ndjson"
    }).status_code == 400
    assert client.post("/batches?batch_id=ENV", content=b'{"no_jsonl": true}', headers={
        "content-type": "application/json"
    }).status_code == 400

    deleted = client.delete("/batches/API")
    assert deleted.status_code == 200
    assert deleted.json()["runs_removed"] == 1
    assert client.get("/batches/API").status_code == 404
    assert client.get("/runs", params={"batch_id": "API"}).json() == []


def test_api_empty_state_and_validation_errors(client, tiny_payload):
    assert client.get("/clusters").status_code == 404
    assert client.get("/clusters", params={"batch_id": "EMPTY"}).status_code == 404
    assert client.get("/review/next").json() == {"batch_id": None, "pairs": []}
    assert client.get("/review/next", params={"n": 0}).status_code == 400
    assert client.get("/review/decisions").status_code == 400
    assert client.get("/review/decisions", params={"batch_id": "missing"}).status_code == 404
    assert client.post("/runs", json={"batch_id": "missing"}).status_code == 404
    assert client.delete("/batches/missing").status_code == 404
    assert client.post("/batches", content=b"{broken", headers={
        "content-type": "application/json"
    }).status_code == 400

    created = client.post("/batches?batch_id=NORUN", content=tiny_payload, headers={
        "content-type": "application/x-ndjson"
    })
    assert created.status_code == 201
    assert client.get("/clusters", params={"batch_id": "NORUN"}).status_code == 404
    assert client.get("/review/next", params={"batch_id": "NORUN"}).json()["pairs"] == []
    assert client.post("/review/decisions", json={
        "batch_id": "NORUN", "pair_key": "x||y", "decision": "same"
    }).status_code == 404
    assert client.get("/review/next", params={"batch_id": "missing"}).status_code == 404

    run = client.post("/runs", json={"batch_id": "NORUN"}).json()
    assert client.get("/review/next").json()["batch_id"] == "NORUN"
    assert client.get("/clusters", params={"run_id": "missing"}).status_code == 404
    assert client.get("/metrics/thresholds", params={
        "run_id": run["run_id"], "grid": "not-a-number"
    }).status_code == 400
    assert client.get("/metrics/thresholds", params={
        "run_id": "missing"
    }).status_code == 404

    payload = b'{"record_id":"X","source":"ORBIT","title":"Another lone synthetic record"}\n'
    client.post("/batches?batch_id=THRESH_NOGOLD", content=payload, headers={
        "content-type": "application/x-ndjson"
    })
    no_gold_run = client.post("/runs", json={"batch_id": "THRESH_NOGOLD"}).json()
    assert client.get("/metrics/thresholds", params={
        "run_id": no_gold_run["run_id"]
    }).status_code == 409


def test_metrics_without_gold_labels_returns_conflict(client):
    payload = b'{"record_id":"X","source":"ORBIT","title":"A lone synthetic record"}\n'
    client.post("/batches?batch_id=NOGOLD", content=payload, headers={
        "content-type": "application/x-ndjson"
    })
    run = client.post("/runs", json={"batch_id": "NOGOLD"}).json()
    response = client.get("/metrics/report", params={"run_id": run["run_id"]})
    assert response.status_code == 409


def test_reflux_view_endpoint_and_immediate_queue_filter(client, seeded_paths):
    payload = seeded_paths[0].read_bytes()
    client.post(
        "/batches?batch_id=REFLUX", content=payload,
        headers={"content-type": "application/x-ndjson"},
    )
    run = client.post("/runs", json={"batch_id": "REFLUX"}).json()
    baseline_candidates = run["candidate_pairs"]

    assert client.get("/review/view", params={"batch_id": "REFLUX"}).json() == {
        "batch_id": "REFLUX",
        "view_version": 0,
        "effective_count": 0,
        "counts": {"same": 0, "different": 0},
        "decisions": [],
    }

    queue = client.get("/review/next", params={"batch_id": "REFLUX", "n": 5}).json()
    pair = queue["pairs"][0]
    runs_before = len(client.get("/runs", params={"batch_id": "REFLUX"}).json())

    submitted = client.post("/review/decisions", json={
        "batch_id": "REFLUX", "pair_key": pair["pair_key"], "decision": "different",
        "reviewer": "qa", "note": "no rerun expected",
    })
    assert submitted.status_code == 201
    assert "run" not in submitted.json()
    # The decision endpoint no longer triggers a pipeline run.
    runs_after = client.get("/runs", params={"batch_id": "REFLUX"}).json()
    assert len(runs_after) == runs_before

    queue_after = client.get(
        "/review/next", params={"batch_id": "REFLUX", "n": 100}
    ).json()
    assert all(item["pair_key"] != pair["pair_key"] for item in queue_after["pairs"])

    view = client.get("/review/view", params={"batch_id": "REFLUX"}).json()
    assert view["view_version"] == 1
    assert view["effective_count"] == 1
    assert view["counts"] == {"same": 0, "different": 1}

    # The next explicit run reflects reflux in its counters.
    next_run = client.post("/runs", json={"batch_id": "REFLUX"}).json()
    assert next_run["candidate_pairs"] == baseline_candidates - 1
    reflux = next_run["blocking_stats"]["decision_reflux"]
    assert reflux["excluded_different_pairs"] == 1
    assert reflux["decision_view_version"] == 1
    assert client.get("/review/view", params={"batch_id": "missing"}).status_code == 404
