"""Batch merge (POST /batches?merge=true): idempotency, conflicts and snapshots."""
from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from citefuse.ingest import MergeConflictError, ingest_jsonl, merge_jsonl
from citefuse.pipeline import run_pipeline
from citefuse.review import next_review_pairs, submit_decision
from citefuse.storage import MergeInProgress, Storage
from scripts.seed_fixtures import generate


def _jsonl(records) -> bytes:
    return (
        "\n".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for record in records
        )
        + "\n"
    ).encode("utf-8")


def _new_record(record_id: str, source: str = "ORBIT", **overrides):
    record = {
        "record_id": record_id,
        "source": source,
        "title": f"A fresh synthetic trial number {record_id}",
        "authors": ["Vaelis Zorvath"],
        "venue": "Journal of Fictional Evidence Synthesis",
        "year": 2024,
        "volume": 12,
        "issue": 2,
        "pages": "10-20",
        "doi": f"10.5555/merge.{record_id.lower()}",
        "gold_cluster": f"NEW-{record_id}",
    }
    record.update(overrides)
    return record


def _seed_batch(storage, config, clock, batch_id="merge"):
    records, gold = generate(20240517)
    ingest_jsonl(storage, config, clock, _jsonl(records), batch_id=batch_id)
    return records, gold


def _cluster_rows(storage, run_id):
    return [tuple(row) for row in storage.list_clusters(run_id)]


def _membership(storage, run_id):
    result = {}
    for cluster in storage.list_clusters(run_id):
        for key in json.loads(cluster["member_keys_json"]):
            result[key] = cluster["cluster_id"]
    return result


def test_merge_skips_same_identifier_same_content_and_appends_new(
    storage, config, clock
):
    records, _ = _seed_batch(storage, config, clock)
    baseline = run_pipeline(storage, config, clock, "merge")
    before = storage.get_batch("merge")
    existing = records[0]
    new = _new_record("M001", source="LANTERN")

    result = merge_jsonl(storage, config, clock, _jsonl([existing, new]), "merge")
    assert result["merged"] is True
    assert result["created"] is False
    assert result["duplicates"] == 1
    assert result["inserted"] == 1
    assert result["valid_lines"] == before["valid_lines"] + 1
    assert result["total_lines"] == before["total_lines"] + 2
    assert result["bad_lines"] == before["bad_lines"]
    assert result["input_hash"] != before["input_hash"]
    assert storage.count_records("merge") == 201

    # Re-merging the identical payload is a no-op on records and counters.
    again = merge_jsonl(storage, config, clock, _jsonl([existing, new]), "merge")
    assert again["inserted"] == 0
    assert again["duplicates"] == 2
    assert storage.count_records("merge") == 201
    assert again["input_hash"] == result["input_hash"]

    # A later run over the merged batch sees the new record.
    after = run_pipeline(storage, config, clock, "merge")
    assert after["run_id"] != baseline["run_id"]
    assert "LANTERN::M001" in _membership(storage, after["run_id"])


def test_merge_conflicting_identifier_rejects_whole_merge(storage, config, clock):
    records, _ = _seed_batch(storage, config, clock)
    before = dict(storage.get_batch("merge"))
    before_records = [tuple(row) for row in storage.list_records("merge")]
    changed = dict(records[0])
    changed["title"] = "A completely different title for the same identifier"
    good = _new_record("M002")

    with pytest.raises(MergeConflictError) as caught:
        merge_jsonl(storage, config, clock, _jsonl([good, changed]), "merge")
    conflicts = caught.value.conflicts
    assert len(conflicts) == 1
    assert conflicts[0]["record_id"] == records[0]["record_id"]
    assert conflicts[0]["source"] == records[0]["source"].upper()
    assert conflicts[0]["incoming"]["title"] == changed["title"]

    # Nothing was written: not the good row, not the bad row, not counters.
    after = storage.get_batch("merge")
    assert dict(after) == before
    assert [tuple(row) for row in storage.list_records("merge")] == before_records


def test_merge_conflict_inside_single_payload_detected(storage, config, clock):
    _seed_batch(storage, config, clock)
    first = _new_record("M003")
    second = _new_record("M003", source="COMPASS")  # different business key
    third = _new_record("M003")  # same key, same content as first
    result = merge_jsonl(storage, config, clock, _jsonl([first, second, third]), "merge")
    assert result["inserted"] == 2
    assert result["duplicates"] == 1

    conflicting = _new_record("M004")
    conflicting_again = _new_record("M004", title="Different body, same identity")
    with pytest.raises(MergeConflictError):
        merge_jsonl(
            storage, config, clock, _jsonl([conflicting, conflicting_again]), "merge"
        )
    assert storage.query_one(
        "SELECT 1 FROM records WHERE record_id='M004'"
    ) is None


def test_merge_unknown_batch_and_bad_payload(storage, config, clock):
    with pytest.raises(LookupError):
        merge_jsonl(storage, config, clock, b'{"source":"S"}', "missing")
    _seed_batch(storage, config, clock)
    with pytest.raises(Exception):
        merge_jsonl(storage, config, clock, b"", "merge")


def test_historical_run_is_frozen_after_merge(storage, config, clock):
    records, _ = _seed_batch(storage, config, clock)
    baseline = run_pipeline(storage, config, clock, "merge")
    cluster_rows = _cluster_rows(storage, baseline["run_id"])
    old_membership = _membership(storage, baseline["run_id"])

    merge_jsonl(
        storage, config, clock,
        _jsonl([records[0], _new_record("M005"), _new_record("M006")]),
        "merge",
    )

    # The old run answers exactly as before, despite the batch input_hash move.
    assert _cluster_rows(storage, baseline["run_id"]) == cluster_rows
    assert _membership(storage, baseline["run_id"]) == old_membership
    assert storage.get_run(baseline["run_id"]) is not None
    for key in ("ORBIT::M005", "ORBIT::M006"):
        assert key not in old_membership

    # Rerunning the old run id (same input snapshot is not reproducible after a
    # merge, so it must be a different run id) still cannot leak new members.
    new_run = run_pipeline(storage, config, clock, "merge")
    assert "ORBIT::M005" in _membership(storage, new_run["run_id"])
    assert _cluster_rows(storage, baseline["run_id"]) == cluster_rows


def test_merge_reuses_human_decisions_and_pair_keys_are_business_derived(
    storage, config, clock
):
    records, _ = _seed_batch(storage, config, clock)
    baseline = run_pipeline(storage, config, clock, "merge")
    queue = next_review_pairs(storage, "merge", 100)
    assert queue
    target = queue[0]
    keys = [record["record_key"] for record in target["records"]]
    submit_decision(
        storage, clock, "merge", target["pair_key"], "different", "reviewer", "keep apart"
    )

    merge_jsonl(storage, config, clock, _jsonl([_new_record("M007")]), "merge")
    merged = run_pipeline(storage, config, clock, "merge")
    assert merged["run_id"] != baseline["run_id"]
    membership = _membership(storage, merged["run_id"])
    assert membership[keys[0]] != membership[keys[1]]
    assert "ORBIT::M007" in membership

    # pair_key is derived from (source, record_id), not from auto-increment pks,
    # so it is identical across both runs.
    old_pairs = {row["pair_key"] for row in storage.list_pairs(baseline["run_id"])}
    new_pairs = {row["pair_key"] for row in storage.list_pairs(merged["run_id"])}
    assert target["pair_key"] in old_pairs
    assert target["pair_key"] in new_pairs
    decisions = [dict(row) for row in storage.decision_history("merge")]
    assert decisions[-1]["pair_key"] == target["pair_key"]


def test_concurrent_merges_only_one_wins(storage, config, clock, monkeypatch):
    _seed_batch(storage, config, clock)
    results: list[str] = []
    errors: list[Exception] = []
    inside = threading.Event()
    second_done = threading.Event()

    import citefuse.ingest as ingest_module

    real_hash = ingest_module.batch_content_hash

    def hash_hook(*args, **kwargs):
        # Called while the first merge holds the per-batch guard.
        inside.set()
        assert second_done.wait(timeout=5)
        return real_hash(*args, **kwargs)

    monkeypatch.setattr(ingest_module, "batch_content_hash", hash_hook)

    def worker(record_id: str) -> None:
        try:
            merge_jsonl(
                storage, config, clock, _jsonl([_new_record(record_id)]), "merge"
            )
            results.append(record_id)
        except (MergeInProgress, MergeConflictError) as exc:
            errors.append(exc)
        except Exception as exc:  # pragma: no cover - surfaced for debugging
            errors.append(exc)

    first = threading.Thread(target=worker, args=("C001",))
    first.start()
    assert inside.wait(timeout=5)  # first merge is now mid-critical-section
    second = threading.Thread(target=worker, args=("C002",))
    second.start()
    second.join(timeout=5)
    second_done.set()
    first.join(timeout=10)

    assert len(results) == 1
    assert len(errors) == 1
    assert storage.count_records("merge") == 201  # exactly one appended row
    winner = results[0]
    loser = "C002" if winner == "C001" else "C001"
    assert storage.query_one(
        "SELECT 1 FROM records WHERE record_id=?", (winner,)
    ) is not None
    assert storage.query_one(
        "SELECT 1 FROM records WHERE record_id=?", (loser,)
    ) is None


def test_merge_rolls_back_on_midway_failure(storage, config, clock, monkeypatch):
    records, _ = _seed_batch(storage, config, clock)
    before = dict(storage.get_batch("merge"))
    before_records = [tuple(row) for row in storage.list_records("merge")]

    original = storage.insert_record
    calls = {"n": 0}

    def failing_insert(batch_id, row, conn=None):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("synthetic failure halfway through merge")
        return original(batch_id, row, conn=conn)

    monkeypatch.setattr(storage, "insert_record", failing_insert)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        merge_jsonl(
            storage, config, clock,
            _jsonl([_new_record("R001"), _new_record("R002"), _new_record("R003")]),
            "merge",
        )

    # The first row reached SQLite inside the transaction, yet it is gone.
    after = storage.get_batch("merge")
    assert dict(after) == before
    assert [tuple(row) for row in storage.list_records("merge")] == before_records


def test_merge_survives_a_killed_process(tmp_path, config):
    """A merge interrupted before COMMIT leaves no trace once the DB is reopened."""
    from datetime import datetime, timezone

    from citefuse.clock import FixedClock

    db_path = tmp_path / "kill.sqlite3"
    clock = FixedClock(datetime(2025, 1, 2, tzinfo=timezone.utc))
    first = Storage(db_path)
    records, _ = generate(20240517)
    ingest_jsonl(first, config, clock, _jsonl(records), batch_id="kill")
    before = dict(first.get_batch("kill"))

    # Simulate a process dying after rows were written but before commit: open a
    # manual IMMEDIATE transaction, write through the raw connection (so no
    # helper implicitly commits), then close without committing.
    first.conn.execute("BEGIN IMMEDIATE")
    first.conn.execute(
        """INSERT INTO records(batch_id,source,record_id,line_no,raw_json,norm_json,
             title_norm,authors_norm,gold_cluster)
           VALUES('kill','ORBIT','KILLED',999,'{}','{}','killed','[]',NULL)"""
    )
    first.conn.execute(
        "UPDATE batches SET total_lines=total_lines+1, valid_lines=valid_lines+1"
        " WHERE batch_id='kill'"
    )
    # No commit/rollback; dropping the connection mimics an abrupt exit.
    first.conn.close()

    reopened = Storage(db_path)
    try:
        after = dict(reopened.get_batch("kill"))
        assert after == before
        assert reopened.count_records("kill") == 200
        assert reopened.query_one(
            "SELECT 1 FROM records WHERE record_id='KILLED'"
        ) is None
    finally:
        reopened.close()


def test_merge_via_http_endpoint(client, seeded_paths):
    payload = seeded_paths[0].read_bytes()
    created = client.post(
        "/batches?batch_id=HTTP&name=HTTP", content=payload,
        headers={"content-type": "application/x-ndjson"},
    )
    assert created.status_code == 201
    baseline = client.post("/runs", json={"batch_id": "HTTP"}).json()

    existing = json.loads(payload.decode("utf-8").splitlines()[0])
    new = _new_record("H001")
    merged = client.post(
        "/batches?batch_id=HTTP&merge=true",
        content=_jsonl([existing, new]),
        headers={"content-type": "application/x-ndjson"},
    )
    assert merged.status_code == 201
    body = merged.json()
    assert (body["inserted"], body["duplicates"]) == (1, 1)
    assert body["valid_lines"] == 201
    assert body["input_hash"] != created.json()["input_hash"]

    old_clusters = client.get(
        "/clusters", params={"run_id": baseline["run_id"]}
    ).json()
    assert len(old_clusters["clusters"]) == 120
    assert all(
        "ORBIT::H001" not in cluster["member_keys"]
        for cluster in old_clusters["clusters"]
    )
    old_export = client.get(
        "/golden/export", params={"run_id": baseline["run_id"], "format": "jsonl"}
    )
    assert len(old_export.text.splitlines()) == 120

    rerun = client.post("/runs", json={"batch_id": "HTTP"}).json()
    new_clusters = client.get("/clusters", params={"run_id": rerun["run_id"]}).json()
    assert any(
        "ORBIT::H001" in cluster["member_keys"]
        for cluster in new_clusters["clusters"]
    )

    conflict = _new_record("H001", title="changed body")
    rejected = client.post(
        "/batches?batch_id=HTTP&merge=true",
        content=_jsonl([conflict]),
        headers={"content-type": "application/x-ndjson"},
    )
    assert rejected.status_code == 409
    detail = rejected.json()["detail"]
    assert detail["batch_id"] == "HTTP"
    assert detail["conflicts"][0]["record_id"] == "H001"
    assert detail["conflicts"][0]["reason"] == "identifier_conflict"
    assert client.get("/batches/HTTP").json()["valid_lines"] == 201

    assert client.post(
        "/batches?merge=true", content=_jsonl([new]),
        headers={"content-type": "application/x-ndjson"},
    ).status_code == 400
    assert client.post(
        "/batches?batch_id=ABSENT&merge=true", content=_jsonl([new]),
        headers={"content-type": "application/x-ndjson"},
    ).status_code == 404


def test_plain_reingest_after_merge_is_still_idempotent(storage, config, clock):
    payload_records, _ = generate(20240517)
    payload = _jsonl(payload_records)
    ingest_jsonl(storage, config, clock, payload, batch_id="same")
    merge_jsonl(storage, config, clock, _jsonl([_new_record("S001")]), "same")
    repeat = ingest_jsonl(storage, config, clock, payload, batch_id="same")
    assert repeat["created"] is False
    assert repeat["inserted"] == 0
    assert storage.count_records("same") == 201


def test_old_run_clusters_edges_golden_and_review_are_byte_stable(
    client, seeded_paths, storage
):
    payload = seeded_paths[0].read_bytes()
    client.post(
        "/batches?batch_id=STABLE", content=payload,
        headers={"content-type": "application/x-ndjson"},
    )
    run_id = client.post("/runs", json={"batch_id": "STABLE"}).json()["run_id"]

    before_clusters = client.get("/clusters", params={"run_id": run_id}).content
    before_export = client.get(
        "/golden/export", params={"run_id": run_id, "format": "jsonl"}
    ).content
    before_csv = client.get(
        "/golden/export", params={"run_id": run_id, "format": "csv"}
    ).content
    before_queue = client.get(
        "/review/next", params={"batch_id": "STABLE", "n": 100}
    ).json()
    cluster_ids = [c["cluster_id"] for c in json.loads(before_clusters)["clusters"]]
    before_details = {
        cid: client.get(f"/clusters/{cid}", params={"run_id": run_id}).content
        for cid in cluster_ids
    }

    existing = json.loads(payload.decode("utf-8").splitlines()[0])
    client.post(
        "/batches?batch_id=STABLE&merge=true",
        content=_jsonl([existing, _new_record("STABLE1")]),
        headers={"content-type": "application/x-ndjson"},
    )

    # Old run: clusters, both export formats and every cluster detail unchanged.
    assert client.get("/clusters", params={"run_id": run_id}).content == before_clusters
    assert client.get(
        "/golden/export", params={"run_id": run_id, "format": "jsonl"}
    ).content == before_export
    assert client.get(
        "/golden/export", params={"run_id": run_id, "format": "csv"}
    ).content == before_csv
    for cid, body in before_details.items():
        current = client.get(f"/clusters/{cid}", params={"run_id": run_id})
        assert current.status_code == 200
        assert current.content == body


def test_old_run_review_queue_frozen_and_metrics_stable(client, seeded_paths):
    payload = seeded_paths[0].read_bytes()
    client.post(
        "/batches?batch_id=FROZEN", content=payload,
        headers={"content-type": "application/x-ndjson"},
    )
    run_id = client.post("/runs", json={"batch_id": "FROZEN"}).json()["run_id"]
    before = client.get("/review/next", params={"batch_id": "FROZEN", "n": 100}).json()
    report_before = client.get("/metrics/report", params={"run_id": run_id}).json()

    existing = json.loads(payload.decode("utf-8").splitlines()[0])
    client.post(
        "/batches?batch_id=FROZEN&merge=true",
        content=_jsonl([existing, _new_record("FZ1")]),
        headers={"content-type": "application/x-ndjson"},
    )

    after = client.get("/review/next", params={"batch_id": "FROZEN", "n": 100}).json()
    # The queue's pairs all belong to the old run's snapshot; no new-record pair
    # can appear until a new run exists.
    assert [p["pair_key"] for p in after["pairs"]] == [p["pair_key"] for p in before["pairs"]]
    assert all(
        "ORBIT::FZ1" not in [r["record_key"] for r in pair["records"]]
        for pair in after["pairs"]
    )
    report_after = client.get("/metrics/report", params={"run_id": run_id}).json()
    assert report_after["pairwise"] == report_before["pairwise"]
    assert report_after["predicted_clusters"] == report_before["predicted_clusters"] == 120
    assert report_after["pairwise"]["f1"] == 1.0


def test_merged_batch_new_run_f1_and_cluster_count_hold(storage, config, clock, tmp_path):
    records, gold = _seed_batch(storage, config, clock, batch_id="quality")
    baseline = run_pipeline(storage, config, clock, "quality")
    merge_jsonl(storage, config, clock, _jsonl([_new_record("Q1")]), "quality")
    merged_run = run_pipeline(storage, config, clock, "quality")
    assert merged_run["run_id"] != baseline["run_id"]
    from citefuse.metrics import build_report

    report = build_report(storage, config, tmp_path / "reports", merged_run["run_id"])
    # The 200 labelled fixture records still form 120 exact gold clusters; the
    # unlabeled appended row becomes its own cluster.
    assert report["predicted_clusters"] == gold["gold_cluster_count"] + 1
    assert report["pairwise"]["f1"] == 1.0
    assert baseline["cluster_count"] == 120
