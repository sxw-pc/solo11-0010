"""Cross-batch decision scope, inheritance and concurrency tests."""
from __future__ import annotations

import json
import sqlite3
import threading

import pytest

from citefuse.ingest import ingest_jsonl
from citefuse.models import SCOPE_BATCH, SCOPE_CROSS_BATCH, make_record_key
from citefuse.pipeline import get_run_summary, run_pipeline
from citefuse.repository import DecisionConflict
from citefuse.review import (
    cross_decision_dicts,
    effective_decision_history,
    next_review_pairs,
    submit_decision,
)
from scripts.seed_fixtures import generate


def _payload(records):
    return (
        "\n".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for record in records
        )
        + "\n"
    ).encode("utf-8")


def _seed_records():
    records, _ = generate(20240517)
    return records


def _gray_pair(storage, config, clock, batch_id, records):
    """Ingest ``records`` under ``batch_id`` and return one gray-zone pair."""
    ingest_jsonl(storage, config, clock, _payload(records), batch_id=batch_id)
    run_pipeline(storage, config, clock, batch_id)
    queue = next_review_pairs(storage, batch_id, 100)
    assert queue, "fixture must contain at least one gray-zone pair"
    return queue[0]


def _records_for_keys(records, keys):
    wanted = set(keys)
    return [r for r in records if make_record_key(r["source"], r["record_id"]) in wanted]


def test_cross_batch_decision_is_inherited_by_later_batch(storage, config, clock):
    records = _seed_records()
    first_pair = _gray_pair(storage, config, clock, "B1", records)
    pair_key = first_pair["pair_key"]
    keys = [r["record_key"] for r in first_pair["records"]]
    pair_records = _records_for_keys(records, keys)

    # The later batch is ingested but has not run yet when the decision lands.
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="B2")

    # A cross-batch 'same' on B1; B2's first pipeline run then inherits it.
    submit_decision(
        storage, clock, "B1", pair_key, "same", "cross-reviewer",
        "reusable across batches", SCOPE_CROSS_BATCH,
    )
    rebuilt_b1 = run_pipeline(storage, config, clock, "B1")
    b2_run = run_pipeline(storage, config, clock, "B2")

    # B2's review queue no longer contains the inherited pair.
    b2_queue = next_review_pairs(storage, "B2", 100)
    assert all(pair["pair_key"] != pair_key for pair in b2_queue)

    # /runs exposes the inheritance and its source batch.
    summary = get_run_summary(storage, b2_run["run_id"])
    inherited = summary["inheritances"]
    assert len(inherited) == 1
    hit = inherited[0]
    assert hit["pair_key"] == pair_key
    assert hit["decision"] == "same"
    assert hit["source_batch_id"] == "B1"
    # The source batch binds through its own-cross channel, not inheritance.
    assert rebuilt_b1["inherited_decisions"] == []

    # The inherited same-edge participates in clustering on B2.
    history, _ = effective_decision_history(storage, "B2")
    entry = next(item for item in history if item["pair_key"] == pair_key)
    assert entry["inherited"] is True
    assert entry["source_batch_id"] == "B1"
    edges = [dict(row) for row in storage.list_edges(b2_run["run_id"])]
    edge = next(item for item in edges if item["pair_key"] == pair_key)
    assert edge["kind"] == "human_same"
    details = json.loads(edge["details_json"])
    assert details["inherited"] is True
    assert details["source_batch_id"] == "B1"

    # The cross decision is listed under its source batch's review history.
    history_rows = cross_decision_dicts(storage, "B1")
    assert any(row["pair_key"] == pair_key and row["status"] == "active"
               for row in history_rows)


def test_batch_scoped_decision_never_leaks_to_other_batches(storage, config, clock):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "LB1", records)
    pair_key = pair["pair_key"]
    pair_records = _records_for_keys(
        records, [r["record_key"] for r in pair["records"]]
    )
    submit_decision(
        storage, clock, "LB1", pair_key, "different", "local-reviewer",
        "batch only", SCOPE_BATCH,
    )
    run_pipeline(storage, config, clock, "LB1")
    _gray_pair(storage, config, clock, "LB2", pair_records)
    # The batch-local decision is invisible to the second batch.
    assert any(pair["pair_key"] == pair_key
               for pair in next_review_pairs(storage, "LB2", 100))
    assert storage.list_active_cross_decisions() == []
    assert storage.list_inheritances("LB2") == []


def test_default_scope_remains_batch_local(storage, config, clock):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "LOCAL", records)
    result = submit_decision(
        storage, clock, "LOCAL", pair["pair_key"], "different",
        "reviewer", "note",  # scope omitted entirely
    )
    assert result["scope"] == SCOPE_BATCH
    row = storage.decision_history("LOCAL", pair["pair_key"])[0]
    assert row["scope"] == "batch"
    assert row["status"] == "active"
    assert storage.list_active_cross_decisions() == []


def test_identity_mismatch_blocks_blind_inheritance(storage, config, clock):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "REB1", records)
    pair_key = pair["pair_key"]
    keys = [r["record_key"] for r in pair["records"]]
    submit_decision(
        storage, clock, "REB1", pair_key, "same", "cross-reviewer",
        "cross batch", SCOPE_CROSS_BATCH,
    )
    run_pipeline(storage, config, clock, "REB1")
    cross = storage.get_active_cross_decision(pair_key)
    assert cross is not None
    # The source batch's own cross decision binds through the own-cross
    # channel, not the frozen inheritance snapshot.
    assert storage.list_inheritances("REB1") == []

    # Rebuild: same batch name, same business keys, but the content behind one
    # record changes. The pair_key string survives; the fingerprint does not.
    storage.delete_batch("REB1")
    rebuilt = []
    for record in _records_for_keys(records, keys):
        record = dict(record)
        key = make_record_key(record["source"], record["record_id"])
        if key == keys[0]:
            record["title"] = record["title"] + " — retracted and replaced edition"
        rebuilt.append(record)
    ingest_jsonl(storage, config, clock, _payload(rebuilt), batch_id="REB1")
    run = run_pipeline(storage, config, clock, "REB1")

    # No frozen inheritance: identity validation refused the stale decision.
    assert storage.list_inheritances("REB1") == []
    queue = next_review_pairs(storage, "REB1", 100)
    item = next(candidate for candidate in queue if candidate["pair_key"] == pair_key)
    historical = item["historical_decision"]
    assert historical["inherited"] is False
    assert historical["mismatch_reason"] == "record_content_mismatch"
    assert historical["source_batch_id"] == "REB1"
    assert historical["decision"] == "same"

    # The mismatch is audited on the rebuilt run.
    from citefuse.audit import run_events
    events = run_events(storage, run["run_id"])
    assert any(event["event_type"] == "inheritance_identity_mismatch"
               and event["detail"]["pair_key"] == pair_key for event in events)

    # The pair falls back to a fresh gray-zone decision, which now wins.
    submit_decision(
        storage, clock, "REB1", pair_key, "different", "re-reviewer",
        "records are genuinely different now", SCOPE_BATCH,
    )
    run_pipeline(storage, config, clock, "REB1")
    latest, _ = effective_decision_history(storage, "REB1")
    entry = next(item for item in latest if item["pair_key"] == pair_key)
    assert entry["decision"] == "different"
    assert "inherited" not in entry


def test_identical_rebuild_re_inherits_through_identity_check(storage, config, clock):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "SAME1", records)
    pair_key = pair["pair_key"]
    keys = [r["record_key"] for r in pair["records"]]
    submit_decision(
        storage, clock, "SAME1", pair_key, "same", "cross-reviewer",
        "cross", SCOPE_CROSS_BATCH,
    )
    run_pipeline(storage, config, clock, "SAME1")

    storage.delete_batch("SAME1")
    same_records = [dict(r) for r in _records_for_keys(records, keys)]
    ingest_jsonl(storage, config, clock, _payload(same_records), batch_id="SAME1")
    run_pipeline(storage, config, clock, "SAME1")
    # Same content -> fingerprints match -> the batch's own cross decision
    # still binds (through the own-cross channel, identity re-validated).
    history, _ = effective_decision_history(storage, "SAME1")
    entry = next(item for item in history if item["pair_key"] == pair_key)
    assert entry["decision"] == "same"
    assert all(p["pair_key"] != pair_key for p in next_review_pairs(storage, "SAME1", 100))


def test_mutually_exclusive_concurrent_decisions_leave_one_effective(storage, config, clock):
    records = _seed_records()
    pair_b1 = _gray_pair(storage, config, clock, "CX1", records)
    pair_key = pair_b1["pair_key"]
    keys = [r["record_key"] for r in pair_b1["records"]]
    # Second batch containing the same business pair.
    ingest_jsonl(
        storage, config, clock,
        _payload(_records_for_keys(records, keys)), batch_id="CX2",
    )
    run_pipeline(storage, config, clock, "CX2")

    barrier = threading.Barrier(2)
    outcomes: list[dict] = []
    errors: list[Exception] = []

    def worker(batch_id, verdict):
        try:
            barrier.wait()
            outcomes.append(submit_decision(
                storage, clock, batch_id, pair_key, verdict,
                f"reviewer-{batch_id}", f"concurrent {verdict}",
                SCOPE_CROSS_BATCH,
            ))
        except Exception as exc:  # noqa: BLE001 - asserted below
            errors.append(exc)

    t1 = threading.Thread(target=worker, args=("CX1", "same"))
    t2 = threading.Thread(target=worker, args=("CX2", "different"))
    t1.start(); t2.start()
    t1.join(); t2.join()

    active = storage.list_active_cross_decisions([pair_key])
    assert len(active) == 1, "exactly one effective cross-batch decision may exist"
    winner = active[0]

    # Either one submission won outright, or the loser was rejected with a
    # DecisionConflict naming the winner. In every case only one row is active
    # and the loser's batch never believes its own verdict is effective.
    if errors:
        assert len(errors) == 1
        assert isinstance(errors[0], DecisionConflict)
        assert errors[0].winner["decision_id"] == winner["decision_id"]
        assert len(outcomes) == 1
        assert outcomes[0]["decision_id"] == winner["decision_id"]
    else:
        assert len(outcomes) == 2
        assert all(outcome["effective"] for outcome in outcomes)
        # The later submission superseded the earlier one atomically.
        history = storage.list_cross_decision_history()
        pair_rows = [row for row in history if row["pair_key"] == pair_key]
        assert {row["status"] for row in pair_rows} == {"active", "superseded"}
        assert sorted(row["decision"] for row in pair_rows) == ["different", "same"]

    # A fresh batch now inherits the single surviving verdict.
    ingest_jsonl(
        storage, config, clock,
        _payload(_records_for_keys(records, keys)), batch_id="CX3",
    )
    run = run_pipeline(storage, config, clock, "CX3")
    pins = storage.list_inheritances("CX3")
    assert len(pins) == 1
    assert pins[0]["decision"] == winner["decision"]
    assert run["inherited_decisions"][0]["source_batch_id"] == winner["source_batch_id"]


def test_newer_cross_decision_supersedes_older_and_keeps_batches_reproducible(
    storage, config, clock
):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "SUP1", records)
    pair_key = pair["pair_key"]
    keys = [r["record_key"] for r in pair["records"]]
    pair_records = _records_for_keys(records, keys)
    first = submit_decision(
        storage, clock, "SUP1", pair_key, "same", "r1", "old", SCOPE_CROSS_BATCH,
    )
    run_pipeline(storage, config, clock, "SUP1")

    # SUP2 only *inherits* the decision; it never submits one itself.
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="SUP2")
    sup2_first = run_pipeline(storage, config, clock, "SUP2")
    assert storage.list_inheritances("SUP2")[0]["decision"] == "same"
    sup2_clusters = [tuple(row) for row in storage.list_clusters(sup2_first["run_id"])]
    sup2_golden = [
        json.loads(row["golden_json"])
        for row in storage.list_clusters(sup2_first["run_id"])
    ]

    # A mutually exclusive later decision (from a third batch) wins by
    # submission ordering and supersedes the older global row.
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="SUP3")
    run_pipeline(storage, config, clock, "SUP3")
    second = submit_decision(
        storage, clock, "SUP3", pair_key, "different", "r2", "new",
        SCOPE_CROSS_BATCH,
    )
    assert second["superseded_decision_id"] == first["decision_id"]
    assert storage.get_active_cross_decision(pair_key)["decision"] == "different"

    # SUP2's frozen snapshot is immutable: rerun keeps the original run_id and
    # byte-identical clusters/golden despite the newer global verdict.
    sup2_again = run_pipeline(storage, config, clock, "SUP2")
    assert sup2_again["run_id"] == sup2_first["run_id"]
    assert storage.list_inheritances("SUP2")[0]["decision"] == "same"
    assert [tuple(row) for row in storage.list_clusters(sup2_again["run_id"])] \
        == sup2_clusters
    assert [
        json.loads(row["golden_json"]) for row in storage.list_clusters(sup2_again["run_id"])
    ] == sup2_golden

    # A brand-new batch resolves against current global state and gets the
    # newer 'different' verdict via a fresh frozen snapshot.
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="SUP4")
    sup4_run = run_pipeline(storage, config, clock, "SUP4")
    pin = storage.list_inheritances("SUP4")[0]
    assert pin["decision"] == "different"
    assert pin["source_batch_id"] == "SUP3"
    assert sup4_run["inherited_decisions"][0]["source_decision_id"] == second["decision_id"]


def test_source_batch_own_cross_decision_tracks_latest_active_row(storage, config, clock):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "OWN", records)
    pair_key = pair["pair_key"]
    submit_decision(
        storage, clock, "OWN", pair_key, "same", "r1", "old", SCOPE_CROSS_BATCH,
    )
    run_pipeline(storage, config, clock, "OWN")

    # Superseding the batch's own cross verdict rebinds the batch itself.
    submit_decision(
        storage, clock, "OWN", pair_key, "different", "r2", "new", SCOPE_CROSS_BATCH,
    )
    rerun = run_pipeline(storage, config, clock, "OWN")
    history, _ = effective_decision_history(storage, "OWN")
    entry = next(item for item in history if item["pair_key"] == pair_key)
    assert entry["decision"] == "different"
    assert entry.get("origin") == "own_cross_batch"
    # Own-cross decisions never occupy the frozen inheritance table.
    assert storage.list_inheritances("OWN") == []
    assert all(p["pair_key"] != pair_key for p in next_review_pairs(storage, "OWN", 100))
    assert rerun["inherited_decisions"] == []


def test_old_batch_rerun_is_byte_identical_after_later_inheritance(storage, config, clock):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "OLD", records)
    pair_key = pair["pair_key"]
    keys = [r["record_key"] for r in pair["records"]]
    pair_records = _records_for_keys(records, keys)
    first = run_pipeline(storage, config, clock, "OLD")
    first_clusters = [tuple(row) for row in storage.list_clusters(first["run_id"])]
    first_golden = [
        json.loads(row["golden_json"]) for row in storage.list_clusters(first["run_id"])
    ]
    first_edges = [tuple(row) for row in storage.list_edges(first["run_id"])]

    # A *different* batch later submits a cross-batch decision on the pair and
    # a third batch inherits it. OLD never participates.
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="SRC")
    run_pipeline(storage, config, clock, "SRC")
    submit_decision(
        storage, clock, "SRC", pair_key, "same", "r", "cross", SCOPE_CROSS_BATCH,
    )
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="NEW")
    new_run = run_pipeline(storage, config, clock, "NEW")
    assert new_run["inherited_decisions"][0]["source_batch_id"] == "SRC"

    # OLD's rerun keeps the same run_id and byte-identical stored outputs:
    # its frozen snapshot was empty and stays empty.
    again = run_pipeline(storage, config, clock, "OLD")
    assert again["run_id"] == first["run_id"]
    assert [tuple(row) for row in storage.list_clusters(again["run_id"])] == first_clusters
    assert [
        json.loads(row["golden_json"]) for row in storage.list_clusters(again["run_id"])
    ] == first_golden
    assert [tuple(row) for row in storage.list_edges(again["run_id"])] == first_edges
    assert storage.list_inheritances("OLD") == []


def test_conflicting_insert_surfaces_winner_and_keeps_one_active(
    storage, config, clock, monkeypatch
):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "CF", records)
    pair_key = pair["pair_key"]

    # A competing active row committed by another transaction.
    submit_decision(
        storage, clock, "CF", pair_key, "different", "racer",
        "won the race", SCOPE_CROSS_BATCH,
    )
    winner = dict(storage.get_active_cross_decision(pair_key))

    # Force the next INSERT to hit the partial unique index, as a true
    # concurrent transaction would make it do. We wrap the storage connection
    # because the C-level sqlite3.Connection type cannot be monkeypatched.
    real_conn = storage.conn
    real_execute = real_conn.execute

    class _FlakyConnection:
        def __getattr__(self, name):
            return getattr(real_conn, name)

        def execute(self, sql, parameters=()):
            if sql.lstrip().upper().startswith("INSERT INTO CROSS_BATCH_DECISIONS"):
                raise sqlite3.IntegrityError(
                    "UNIQUE constraint failed: cross_batch_decisions.pair_key"
                )
            return real_execute(sql, parameters)

    monkeypatch.setattr(storage, "conn", _FlakyConnection())
    with pytest.raises(DecisionConflict) as exc_info:
        submit_decision(
            storage, clock, "CF", pair_key, "same", "loser",
            "lost the race", SCOPE_CROSS_BATCH,
        )
    assert exc_info.value.winner["decision_id"] == winner["decision_id"]

    # The loser's supersede was rolled back: the original winner stays active.
    still_active = storage.list_active_cross_decisions([pair_key])
    assert len(still_active) == 1
    assert still_active[0]["decision"] == "different"
    assert still_active[0]["decision_id"] == winner["decision_id"]


def test_api_returns_409_on_cross_batch_conflict(client, seeded_paths, monkeypatch):
    import sqlite3

    payload = seeded_paths[0].read_bytes()
    client.post("/batches?batch_id=CX", content=payload,
                headers={"content-type": "application/x-ndjson"})
    client.post("/runs", json={"batch_id": "CX"})
    pair_key = client.get(
        "/review/next", params={"batch_id": "CX", "n": 1}
    ).json()["pairs"][0]["pair_key"]
    first = client.post("/review/decisions", json={
        "batch_id": "CX", "pair_key": pair_key, "decision": "same",
        "scope": "cross_batch",
    })
    assert first.status_code == 201
    winner_id = first.json()["decision"]["decision_id"]

    # The app keeps its Storage in a closure. Patch the cross-decision insert
    # on the connection class the running app actually uses by wrapping the
    # singleton connection reachable through the lifespan state.
    app_storage = _app_storage(client)
    real_conn = app_storage.conn
    real_execute = real_conn.execute

    class _Flaky:
        def __getattr__(self, name):
            return getattr(real_conn, name)

        def execute(self, sql, parameters=()):
            if sql.lstrip().upper().startswith("INSERT INTO CROSS_BATCH_DECISIONS"):
                raise sqlite3.IntegrityError("UNIQUE constraint failed")
            return real_execute(sql, parameters)

    monkeypatch.setattr(app_storage, "conn", _Flaky())
    response = client.post("/review/decisions", json={
        "batch_id": "CX", "pair_key": pair_key, "decision": "different",
        "scope": "cross_batch",
    })
    assert response.status_code == 409
    body = response.json()
    assert body["detail"]["error"] == "cross_batch_decision_conflict"
    assert body["detail"]["pair_key"] == pair_key
    assert body["detail"]["effective_decision"]["decision_id"] == winner_id


def _app_storage(client):
    """Return the Storage instance owned by the running FastAPI app.

    ``create_app`` closes over ``storage`` in its routes; the only handle
    reachable from a TestClient is the open SQLite file. The client fixture
    uses a unique tmp_path, so the single matching file is the app's DB.
    """
    from citefuse.storage import Storage

    # FastAPI stores the lifespan-shutdown finalizer; simpler to walk the
    # route closures, which capture the same Storage object.
    for route in client.app.routes:
        endpoint = getattr(route, "endpoint", None)
        closure = getattr(endpoint, "__closure__", None)
        if not closure:
            continue
        for cell in closure:
            try:
                value = cell.cell_contents
            except ValueError:
                continue
            if isinstance(value, Storage):
                return value
    raise AssertionError("could not reach app Storage from route closures")


def test_rerun_does_not_duplicate_inheritance_audit_events(storage, config, clock):
    from citefuse.audit import run_events

    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "AU1", records)
    pair_key = pair["pair_key"]
    pair_records = _records_for_keys(
        records, [r["record_key"] for r in pair["records"]]
    )
    submit_decision(
        storage, clock, "AU1", pair_key, "same", "r", "cross", SCOPE_CROSS_BATCH,
    )
    run_pipeline(storage, config, clock, "AU1")
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="AU2")
    first = run_pipeline(storage, config, clock, "AU2")

    def inherited_events(run_id):
        return [
            event for event in run_events(storage, run_id)
            if event["event_type"] == "decision_inherited"
        ]

    assert len(inherited_events(first["run_id"])) == 1
    # Reruns append attempts but must not re-emit inheritance audit events.
    again = run_pipeline(storage, config, clock, "AU2")
    assert again["run_id"] == first["run_id"]
    assert len(inherited_events(first["run_id"])) == 1
    # Attempts still accumulate.
    assert get_run_summary(storage, first["run_id"])["attempt_count"] == 2


def test_failed_first_run_does_not_freeze_empty_snapshot(
    storage, config, clock, monkeypatch
):
    # A cross decision exists before the batch ever completes a run.
    records = _seed_records()
    source_pair = _gray_pair(storage, config, clock, "FSRC", records)
    pair_key = source_pair["pair_key"]
    pair_records = _records_for_keys(
        records, [r["record_key"] for r in source_pair["records"]]
    )
    submit_decision(
        storage, clock, "FSRC", pair_key, "same", "r", "cross", SCOPE_CROSS_BATCH,
    )
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="FDST")

    # The destination's first run fails inside blocking (before the freeze).
    def boom(*_args, **_kwargs):
        raise RuntimeError("synthetic blocking failure")

    monkeypatch.setattr("citefuse.pipeline.generate_candidates", boom)
    with pytest.raises(RuntimeError, match="synthetic blocking failure"):
        run_pipeline(storage, config, clock, "FDST")
    monkeypatch.undo()

    # The failed attempt must not have frozen an empty snapshot: the retry
    # still resolves the existing cross decision.
    retry = run_pipeline(storage, config, clock, "FDST")
    pins = storage.list_inheritances("FDST")
    assert len(pins) == 1
    assert pins[0]["pair_key"] == pair_key
    assert retry["inherited_decisions"][0]["source_batch_id"] == "FSRC"
    assert all(p["pair_key"] != pair_key
               for p in next_review_pairs(storage, "FDST", 100))


def test_late_global_decision_does_not_hide_old_batch_queue(storage, config, clock):
    records = _seed_records()
    pair = _gray_pair(storage, config, clock, "LATE", records)
    pair_key = pair["pair_key"]
    # First run completes with an empty frozen snapshot; the pair stays gray.
    assert any(p["pair_key"] == pair_key
               for p in next_review_pairs(storage, "LATE", 100))

    # Another batch later submits a cross decision on the same pair.
    pair_records = _records_for_keys(
        records, [r["record_key"] for r in pair["records"]]
    )
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="L2")
    run_pipeline(storage, config, clock, "L2")
    submit_decision(
        storage, clock, "L2", pair_key, "same", "r", "late", SCOPE_CROSS_BATCH,
    )

    # The old batch's queue must still offer the pair (clustering never bound
    # it), now annotated with the historical decision as provenance.
    queue = next_review_pairs(storage, "LATE", 100)
    item = next(p for p in queue if p["pair_key"] == pair_key)
    hist = item["historical_decision"]
    assert hist["source_batch_id"] == "L2"
    assert hist["decision"] == "same"
    # And its frozen snapshot / stored run remain untouched.
    assert storage.list_inheritances("LATE") == []


def test_drifted_frozen_pin_is_dropped_not_blindly_applied(storage, config, clock):
    # A foreign cross decision is inherited and frozen on the destination.
    records = _seed_records()
    source_pair = _gray_pair(storage, config, clock, "DSRC", records)
    pair_key = source_pair["pair_key"]
    keys = [r["record_key"] for r in source_pair["records"]]
    submit_decision(
        storage, clock, "DSRC", pair_key, "same", "r", "cross", SCOPE_CROSS_BATCH,
    )
    pair_records = _records_for_keys(records, keys)
    ingest_jsonl(storage, config, clock, _payload(pair_records), batch_id="DDST")
    first = run_pipeline(storage, config, clock, "DDST")
    assert len(storage.list_inheritances("DDST")) == 1

    # Simulate content drift behind the frozen pair_key without a full batch
    # rebuild (additive re-ingest path): mutate one record's normalized content.
    row = storage.query(
        "SELECT * FROM records WHERE batch_id=? LIMIT 1", ("DDST",)
    )[0]
    import json as _json
    norm = _json.loads(row["norm_json"])
    norm["title"] = norm["title"] + " materially revised content"
    storage.execute(
        "UPDATE records SET norm_json=? WHERE record_pk=?",
        (_json.dumps(norm, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
         row["record_pk"]),
    )
    history, dropped = effective_decision_history(
        storage, "DDST",
        [{"pair_key": p["pair_key"], "rec_a_pk": p["rec_a_pk"],
          "rec_b_pk": p["rec_b_pk"]}
         for p in storage.list_pairs(first["run_id"])],
    )
    assert all(item["pair_key"] != pair_key for item in history)
    assert any(item["pin"]["pair_key"] == pair_key for item in dropped)


def test_legacy_decision_rows_need_no_migration(tmp_path, config, clock):
    """A database created by the old schema keeps batch-scoped semantics."""
    import sqlite3

    db_path = tmp_path / "legacy.sqlite3"
    old_conn = sqlite3.connect(db_path)
    old_conn.executescript(
        """
        CREATE TABLE batches (
          batch_id TEXT PRIMARY KEY, name TEXT, input_hash TEXT NOT NULL,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          total_lines INTEGER NOT NULL DEFAULT 0, valid_lines INTEGER NOT NULL DEFAULT 0,
          bad_lines INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'ingested'
        );
        CREATE TABLE decisions (
          decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id TEXT NOT NULL, pair_key TEXT NOT NULL, rec_a_key TEXT NOT NULL,
          rec_b_key TEXT NOT NULL, decision TEXT NOT NULL, reviewer TEXT NOT NULL,
          note TEXT NOT NULL, submitted_at TEXT NOT NULL
        );
        """
    )
    old_conn.execute(
        "INSERT INTO batches(batch_id,input_hash,created_at,updated_at) "
        "VALUES('LEG','x','2024-01-01T00:00:00Z','2024-01-01T00:00:00Z')"
    )
    old_conn.execute(
        "INSERT INTO decisions(batch_id,pair_key,rec_a_key,rec_b_key,decision,"
        "reviewer,note,submitted_at) VALUES('LEG','k','a::1','b::2','same',"
        "'legacy','','2024-01-01T00:00:00Z')"
    )
    old_conn.commit()
    old_conn.close()

    # Opening with the current Storage runs additive migrations.
    from citefuse.storage import Storage
    storage = Storage(db_path)
    try:
        rows = storage.decision_history("LEG")
        assert len(rows) == 1
        assert rows[0]["scope"] == "batch"
        assert rows[0]["status"] == "active"
        # Legacy rows never appear as global cross-batch decisions.
        assert storage.list_active_cross_decisions() == []
        assert storage.list_inheritances("LEG") == []
    finally:
        storage.close()
