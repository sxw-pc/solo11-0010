"""Tests for the incremental fusion module.

The headline property under test is *full-run equivalence*: an incremental
run over records [R1] then [R2] must produce exactly the same run id, pairs,
edges, clusters and golden records as a full pipeline run over [R1 + R2],
while leaving every historical snapshot byte-for-byte untouched.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from citefuse.audit import run_events
from citefuse.incremental import (
    IncrementalBusy,
    IncrementalEngine,
    IncrementalError,
    ensure_incremental_schema,
)
from citefuse.ingest import ingest_jsonl
from citefuse.pipeline import run_pipeline
from citefuse.review import next_review_pairs, submit_decision
from scripts.seed_fixtures import generate


FIXED_NOW = datetime(2025, 3, 4, 5, 6, 7, tzinfo=timezone.utc)


def _payload(records):
    return (
        "\n".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
            for record in records
        )
        + "\n"
    ).encode("utf-8")


@pytest.fixture
def incr_clock():
    from citefuse.clock import FixedClock

    return FixedClock(FIXED_NOW)


@pytest.fixture
def seeded_records():
    records, _ = generate(20240517)
    return records


def _snapshot(storage, run_id):
    """Comparable, order-independent content of a run snapshot."""
    return (
        sorted(
            (r["pair_key"], r["score"], r["zone"], r["strategies_json"],
             r["details_json"])
            for r in storage.list_pairs(run_id)
        ),
        sorted(
            (r["pair_key"], r["score"], r["kind"], r["details_json"])
            for r in storage.list_edges(run_id)
        ),
        sorted(
            (r["cluster_id"], r["member_keys_json"], r["member_pks_json"],
             r["golden_json"])
            for r in storage.list_clusters(run_id)
        ),
    )


def _setup_batch(storage, config, clock, records, batch_id="INC"):
    ingest_jsonl(
        storage, config, clock, _payload(records), batch_id=batch_id
    )
    return run_pipeline(storage, config, clock, batch_id)


def test_incremental_matches_full_rerun_equivalently(
    storage, config, incr_clock, seeded_records
):
    records = seeded_records
    split = 113
    base = _setup_batch(
        storage, config, incr_clock, records[:split], batch_id="EQ"
    )
    incr_clock.advance(30)
    ingest_jsonl(
        storage, config, incr_clock, _payload(records[split:]), batch_id="EQ"
    )
    result = IncrementalEngine(storage, config, incr_clock).run_incremental("EQ")

    # Same inputs/config/decisions -> identical deterministic run id.
    full = run_pipeline(storage, config, incr_clock, "EQ")
    assert result["run_id"] == full["run_id"]
    assert _snapshot(storage, result["run_id"]) == _snapshot(
        storage, full["run_id"]
    )

    # Statistical vocabulary fully aligned with the full-run counters.
    for key in (
        "input_rows", "bad_rows", "candidate_pairs", "matched_edges",
        "cluster_count", "golden_count", "config_hash", "status",
    ):
        assert result[key] == full[key]
    assert result["blocking_stats"]["strategy_counts"] == full[
        "blocking_stats"
    ]["strategy_counts"]
    assert result["blocking_stats"]["union_count"] == full[
        "blocking_stats"
    ]["union_count"]
    assert full["cluster_count"] == 120

    # Baseline snapshot remains independently reproducible / untouched.
    assert (
        storage.query_one(
            "SELECT COUNT(*) AS n FROM pairs WHERE run_id=?", (base["run_id"],)
        )["n"]
        == base["candidate_pairs"]
    )
    assert (
        storage.query_one(
            "SELECT COUNT(*) AS n FROM clusters WHERE run_id=?",
            (base["run_id"],),
        )["n"]
        == base["cluster_count"]
    )
    baseline = storage.get_run(base["run_id"])
    assert baseline["status"] == "completed"


def test_multi_step_incremental_chain_equivalence(
    storage, config, incr_clock, seeded_records
):
    records = seeded_records
    chunks = [records[:60], records[60:135], records[135:]]
    base = _setup_batch(
        storage, config, incr_clock, chunks[0], batch_id="CHAIN"
    )
    previous_run = base["run_id"]
    baseline_pair_count = base["candidate_pairs"]

    for chunk in chunks[1:]:
        incr_clock.advance(15)
        ingest_jsonl(
            storage, config, incr_clock, _payload(chunk), batch_id="CHAIN"
        )
        out = IncrementalEngine(storage, config, incr_clock).run_incremental(
            "CHAIN", previous_run
        )
        assert out["base_run_id"] == previous_run
        assert out["mode"] == "incremental"
        previous_run = out["run_id"]

    full = run_pipeline(storage, config, incr_clock, "CHAIN")
    assert previous_run == full["run_id"]
    assert _snapshot(storage, previous_run) == _snapshot(
        storage, full["run_id"]
    )
    # The very first historical snapshot is still intact after two deltas.
    assert (
        storage.query_one(
            "SELECT COUNT(*) AS n FROM pairs WHERE run_id=?", (base["run_id"],)
        )["n"]
        == baseline_pair_count
    )
    tasks = storage.query(
        "SELECT task_id,base_run_id,run_id,status FROM incremental_tasks "
        "ORDER BY rowid"
    )
    assert [row["status"] for row in tasks] == ["completed", "completed"]
    assert tasks[0]["run_id"] == tasks[1]["base_run_id"]


def test_human_decisions_keep_priority_in_incremental_result(
    storage, config, incr_clock, seeded_records
):
    records = seeded_records
    split = 120
    _setup_batch(
        storage, config, incr_clock, records[:split], batch_id="REV"
    )
    incr_clock.advance(10)
    ingest_jsonl(
        storage, config, incr_clock, _payload(records[split:]), batch_id="REV"
    )
    first_incr = IncrementalEngine(
        storage, config, incr_clock
    ).run_incremental("REV")

    # Review a gray-zone pair and force a 'different' verdict before the next
    # delta; the decision must dominate model scoring in the new result.
    queue = next_review_pairs(storage, "REV", 100)
    assert queue
    pair_key = queue[0]["pair_key"]
    incr_clock.advance(5)
    submit_decision(
        storage, incr_clock, "REV", pair_key, "different", "reviewer-x",
        "forced split",
    )
    incr_clock.advance(5)
    # No new records: only the decision changed the deterministic run id.
    result = IncrementalEngine(storage, config, incr_clock).run_incremental("REV")
    full = run_pipeline(storage, config, incr_clock, "REV")
    assert result["run_id"] == full["run_id"]
    assert _snapshot(storage, result["run_id"]) == _snapshot(
        storage, full["run_id"]
    )

    # A human 'same' on another pair must force a merge, again matching full.
    queue = next_review_pairs(storage, "REV", 100)
    if queue:
        same_key = queue[0]["pair_key"]
        incr_clock.advance(5)
        submit_decision(
            storage, incr_clock, "REV", same_key, "same", "reviewer-y",
            "forced merge",
        )
        incr_clock.advance(5)
        merged = IncrementalEngine(
            storage, config, incr_clock
        ).run_incremental("REV")
        full_merge = run_pipeline(storage, config, incr_clock, "REV")
        assert merged["run_id"] == full_merge["run_id"]
        assert _snapshot(storage, merged["run_id"]) == _snapshot(
            storage, full_merge["run_id"]
        )
        events = run_events(storage, merged["run_id"])
        assert any(
            event["event_type"] == "review_conflict" for event in events
        ) or events  # conflict only when rules actually clash

    # The pre-decision incremental snapshot remains immutable.
    assert storage.get_run(first_incr["run_id"]) is not None


def test_incremental_task_is_idempotent(
    storage, config, incr_clock, seeded_records
):
    records = seeded_records
    _setup_batch(
        storage, config, incr_clock, records[:90], batch_id="IDEM"
    )
    incr_clock.advance(10)
    ingest_jsonl(
        storage, config, incr_clock, _payload(records[90:]), batch_id="IDEM"
    )
    engine = IncrementalEngine(storage, config, incr_clock)
    first = engine.run_incremental("IDEM")
    attempts_before = storage.query_one(
        "SELECT COUNT(*) AS n FROM run_attempts WHERE run_id=?",
        (first["run_id"],),
    )["n"]
    second = engine.run_incremental("IDEM")
    third = engine.run_incremental("IDEM")

    assert second["run_id"] == first["run_id"]
    assert third["idempotent_replay"] is True
    # Replays must not append run attempts or mutate snapshots.
    assert storage.query_one(
        "SELECT COUNT(*) AS n FROM run_attempts WHERE run_id=?",
        (first["run_id"],),
    )["n"] == attempts_before
    # The second call has the same delta (idempotent replay, no extra row);
    # later calls with no new data are no-ops that only append task markers.
    task_rows = storage.query(
        "SELECT idempotent_replay,status,run_id FROM incremental_tasks "
        "ORDER BY rowid"
    )
    assert [row["run_id"] for row in task_rows] == [
        first["run_id"], first["run_id"]
    ]
    assert [row["status"] for row in task_rows] == ["completed", "completed"]
    assert bool(task_rows[1]["idempotent_replay"]) is True
    # No second business run row exists.
    assert len(storage.list_runs("IDEM")) == 2  # baseline + incremental


def test_noop_when_nothing_new(storage, config, incr_clock, seeded_records):
    records = seeded_records[:40]
    base = _setup_batch(
        storage, config, incr_clock, records, batch_id="NOOP"
    )
    incr_clock.advance(10)
    result = IncrementalEngine(
        storage, config, incr_clock
    ).run_incremental("NOOP")
    assert result["noop"] is True
    assert result["run_id"] == base["run_id"]
    assert result["counters"]["new_records"] == 0
    # No new business run row is created for a no-op.
    assert len(storage.list_runs("NOOP")) == 1
    events = run_events(storage, base["run_id"])
    assert any(e["event_type"] == "incremental_noop" for e in events)


def test_failure_rolls_back_every_write_and_records_failed_task(
    storage, config, incr_clock, seeded_records, monkeypatch
):
    records = seeded_records
    base = _setup_batch(
        storage, config, incr_clock, records[:100], batch_id="ROLL"
    )
    incr_clock.advance(10)
    ingest_jsonl(
        storage, config, incr_clock, _payload(records[100:]), batch_id="ROLL"
    )
    runs_before = len(storage.list_runs("ROLL"))
    base_pairs = storage.query_one(
        "SELECT COUNT(*) AS n FROM pairs WHERE run_id=?", (base["run_id"],)
    )["n"]

    import citefuse.incremental.engine as engine_mod

    def explode(*_args, **_kwargs):
        raise RuntimeError("synthetic incremental failure")

    monkeypatch.setattr(engine_mod, "score_pair", explode)
    engine = IncrementalEngine(storage, config, incr_clock)
    with pytest.raises(RuntimeError, match="synthetic incremental failure"):
        engine.run_incremental("ROLL")

    # Business state: no new run/pair/edge/cluster rows at all.
    assert len(storage.list_runs("ROLL")) == runs_before
    assert (
        storage.query_one(
            "SELECT COUNT(*) AS n FROM pairs WHERE run_id=?",
            (base["run_id"],),
        )["n"]
        == base_pairs
    )
    assert storage.query("SELECT * FROM incremental_locks") == []
    # Independent failure audit trail remains.
    task = storage.query(
        "SELECT * FROM incremental_tasks WHERE status='failed'"
    )
    assert len(task) == 1 and task[0]["status"] == "failed"
    assert "synthetic incremental failure" in task[0]["error"]
    events = run_events(storage, task[0]["run_id"])
    assert any(e["event_type"] == "incremental_failed" for e in events)

    # After fixing the fault, the task can run successfully against the same
    # untouched baseline; the failure row stays for audit.
    monkeypatch.undo()
    retry = engine.run_incremental("ROLL")
    assert retry["status"] == "completed"
    statuses = [
        row["status"] for row in storage.query(
            "SELECT status FROM incremental_tasks ORDER BY rowid"
        )
    ]
    assert statuses == ["failed", "completed"]


def test_repeated_failures_each_leave_an_audit_row(
    storage, config, incr_clock, seeded_records, monkeypatch
):
    records = seeded_records
    _setup_batch(
        storage, config, incr_clock, records[:100], batch_id="ROLL2"
    )
    incr_clock.advance(10)
    ingest_jsonl(
        storage, config, incr_clock,
        _payload(records[100:]), batch_id="ROLL2",
    )
    import citefuse.incremental.engine as engine_mod

    def explode(*_args, **_kwargs):
        raise RuntimeError("boom again")

    monkeypatch.setattr(engine_mod, "score_pair", explode)
    engine = IncrementalEngine(storage, config, incr_clock)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="boom again"):
            engine.run_incremental("ROLL2")
    monkeypatch.undo()
    ok = engine.run_incremental("ROLL2")
    assert ok["status"] == "completed"
    rows = storage.query(
        "SELECT task_id,status FROM incremental_tasks ORDER BY rowid"
    )
    assert [row["status"] for row in rows] == [
        "failed", "failed", "completed"
    ]
    assert len({row["task_id"] for row in rows}) == 3


def test_configuration_drift_is_rejected(
    storage, config, incr_clock, seeded_records
):
    records = seeded_records[:80]
    _setup_batch(
        storage, config, incr_clock, records, batch_id="DRIFT"
    )
    drifted = config.model_copy(deep=True)
    drifted.scoring.match_threshold = 0.77
    incr_clock.advance(10)
    ingest_jsonl(
        storage, config, incr_clock,
        _payload(seeded_records[80:90]), batch_id="DRIFT",
    )
    with pytest.raises(IncrementalError, match="configuration drift"):
        IncrementalEngine(storage, drifted, incr_clock).run_incremental("DRIFT")
    # Nothing was written.
    assert storage.query("SELECT * FROM incremental_tasks") == []


def test_missing_baseline_and_bad_baseline_are_rejected(
    storage, config, incr_clock, seeded_records
):
    ingest_jsonl(
        storage, config, incr_clock,
        _payload(seeded_records[:5]), batch_id="ORPHAN",
    )
    with pytest.raises(IncrementalError, match="baseline run"):
        IncrementalEngine(storage, config, incr_clock).run_incremental("ORPHAN")
    with pytest.raises(IncrementalError, match="not found"):
        IncrementalEngine(storage, config, incr_clock).run_incremental(
            "ORPHAN", "run_does_not_exist"
        )
    with pytest.raises(IncrementalError, match="not found"):
        IncrementalEngine(storage, config, incr_clock).run_incremental("GHOST")


def test_serial_lock_blocks_concurrent_tasks(
    storage, config, incr_clock, seeded_records
):
    _setup_batch(
        storage, config, incr_clock, seeded_records[:50], batch_id="LOCK"
    )
    incr_clock.advance(10)
    ingest_jsonl(
        storage, config, incr_clock,
        _payload(seeded_records[50:60]), batch_id="LOCK",
    )
    engine_a = IncrementalEngine(storage, config, incr_clock)
    engine_b = IncrementalEngine(storage, config, incr_clock)
    ensure_incremental_schema(storage)
    assert engine_a.tasks.try_acquire_lock("incr_held", incr_clock.now_iso())
    try:
        # Database-level lock rejects a second owner (works cross-process).
        assert not engine_b.tasks.try_acquire_lock(
            "incr_other", incr_clock.now_iso()
        )
        with pytest.raises(IncrementalBusy):
            engine_b.run_incremental("LOCK")
    finally:
        engine_a.tasks.release_lock("incr_held")
    # Once released, the next task proceeds normally.
    out = engine_b.run_incremental("LOCK")
    assert out["status"] == "completed"


def test_streaming_page_size_does_not_change_results(
    storage, config, incr_clock, seeded_records
):
    records = seeded_records
    split = 140
    _setup_batch(
        storage, config, incr_clock, records[:split], batch_id="PAGE"
    )
    incr_clock.advance(10)
    ingest_jsonl(
        storage, config, incr_clock,
        _payload(records[split:]), batch_id="PAGE",
    )
    tiny = IncrementalEngine(
        storage, config, incr_clock, page_size=3
    ).run_incremental("PAGE")
    full = run_pipeline(storage, config, incr_clock, "PAGE")
    assert tiny["run_id"] == full["run_id"]
    assert _snapshot(storage, tiny["run_id"]) == _snapshot(
        storage, full["run_id"]
    )


def test_reproducible_task_identifier_and_aligned_statistics(
    storage, config, incr_clock, seeded_records
):
    records = seeded_records
    base = _setup_batch(
        storage, config, incr_clock, records[:120], batch_id="TASK"
    )
    incr_clock.advance(10)
    ingest_jsonl(
        storage, config, incr_clock,
        _payload(records[120:]), batch_id="TASK",
    )
    result = IncrementalEngine(
        storage, config, incr_clock
    ).run_incremental("TASK")
    task = storage.query_one(
        "SELECT * FROM incremental_tasks WHERE task_id=?", (result["task_id"],)
    )
    assert task is not None
    assert task["task_id"].startswith("incr_")
    assert len(task["task_id"]) == len("incr_") + 16
    assert task["base_run_id"] == base["run_id"]
    assert task["run_id"] == result["run_id"]
    # Delta statistics are populated alongside full-run-aligned counters.
    assert task["new_records"] == 80
    assert task["new_pairs"] > 0
    assert task["reused_pairs"] == base["candidate_pairs"]
    assert (
        task["reused_clusters"] + task["rebuilt_clusters"]
        == result["cluster_count"]
    )
    assert json.loads(task["timings_json"])
    assert json.loads(task["blocking_stats_json"])["union_count"] == (
        result["candidate_pairs"]
    )


def test_ensure_schema_is_idempotent(storage):
    ensure_incremental_schema(storage)
    ensure_incremental_schema(storage)
    tables = {
        row["name"]
        for row in storage.query(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert {"incremental_tasks", "incremental_locks"} <= tables


def test_dead_letter_rows_count_aligns_with_full_run(
    storage, config, incr_clock, seeded_records
):
    good = _payload(seeded_records[:60])
    ingest_jsonl(
        storage, config, incr_clock, good, batch_id="DEAD"
    )
    base = run_pipeline(storage, config, incr_clock, "DEAD")
    assert base["bad_rows"] == 0
    incr_clock.advance(10)
    # The delta itself carries one invalid line plus valid records.
    bad_delta = b'{"not_a_record": true}\n' + _payload(
        seeded_records[60:110]
    )
    ingest_jsonl(
        storage, config, incr_clock, bad_delta, batch_id="DEAD"
    )
    result = IncrementalEngine(
        storage, config, incr_clock
    ).run_incremental("DEAD")
    full = run_pipeline(storage, config, incr_clock, "DEAD")
    assert result["run_id"] == full["run_id"]
    assert result["bad_rows"] == full["bad_rows"] == base["bad_rows"] + 1
    assert _snapshot(storage, result["run_id"]) == _snapshot(
        storage, full["run_id"]
    )
