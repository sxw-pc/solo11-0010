"""Tests for the batch-level, versioned instant-decision reflux view.

Covers the four acceptance checks:
1. a committed decision removes the pair from /review/next immediately;
2. before any rerun the next run's candidate_pairs count changes (same pairs
   become human edges, different pairs leave the pool) and metrics reflect it;
3. concurrent reviewers submitting mutually exclusive verdicts leave exactly
   one effective decision per pair;
4. two parallel runs of the same batch pin the same view version and the
   untouched 200-record fixture still yields 120 clusters with F1 = 1.0.
"""
from __future__ import annotations

import json
import threading

import pytest

from citefuse.decision_view import load_view, pin_version, view_at
from citefuse.ingest import ingest_jsonl
from citefuse.metrics import build_report
from citefuse.pipeline import run_pipeline
from citefuse.review import next_review_pairs, submit_decision
from citefuse.storage import Storage
from scripts.seed_fixtures import generate


def _payload():
    records, _ = generate(20240517)
    return (
        "\n".join(json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records)
        + "\n"
    ).encode("utf-8")


def _seed(storage, config, clock, batch_id="reflux"):
    ingest_jsonl(storage, config, clock, _payload(), batch_id=batch_id)
    return run_pipeline(storage, config, clock, batch_id)


def _gold_by_key(storage, batch_id):
    return {
        f"{row['source']}::{row['record_id']}": row["gold_cluster"]
        for row in storage.list_records(batch_id)
    }


def _membership(storage, run_id):
    result = {}
    for cluster in storage.list_clusters(run_id):
        for key in json.loads(cluster["member_keys_json"]):
            result[key] = cluster["cluster_id"]
    return result


def _gray_pair(queue, gold, membership, same_gold: bool):
    for pair in queue:
        keys = [record["record_key"] for record in pair["records"]]
        is_same_gold = gold[keys[0]] == gold[keys[1]]
        is_same_cluster = membership[keys[0]] == membership[keys[1]]
        if is_same_gold == same_gold and is_same_cluster == same_gold:
            return pair
    raise AssertionError("no suitable gray-zone pair found")


# --- acceptance 1: instant queue filtering without a rerun -------------------

def test_decision_filters_review_queue_immediately(storage, config, clock):
    first = _seed(storage, config, clock)
    queue_before = next_review_pairs(storage, "reflux", 100)
    assert queue_before
    target = queue_before[0]

    result = submit_decision(
        storage, clock, "reflux", target["pair_key"], "different",
        "reviewer-a", "instant filter",
    )
    assert result["view_version"] == 1

    # No rerun happened: the latest run is still the original one.
    assert storage.latest_run_id("reflux") == first["run_id"]
    queue_after = next_review_pairs(storage, "reflux", 100)
    assert all(pair["pair_key"] != target["pair_key"] for pair in queue_after)
    assert len(queue_after) == len(queue_before) - 1

    version, rows = load_view(storage, "reflux")
    assert version == 1 and len(rows) == 1
    assert rows[0]["pair_key"] == target["pair_key"]
    assert rows[0]["decision"] == "different"


# --- acceptance 2: reflux changes candidate_pairs before scoring -------------

def test_reflux_changes_candidate_counts_on_next_run(storage, config, clock, tmp_path):
    first = _seed(storage, config, clock)
    gold = _gold_by_key(storage, "reflux")
    membership = _membership(storage, first["run_id"])
    queue = next_review_pairs(storage, "reflux", 100)

    same_pair = _gray_pair(queue, gold, membership, same_gold=True)
    diff_pair = _gray_pair(queue, gold, membership, same_gold=False)

    submit_decision(storage, clock, "reflux", same_pair["pair_key"], "same",
                    "reviewer-a", "force merge")
    submit_decision(storage, clock, "reflux", diff_pair["pair_key"], "different",
                    "reviewer-b", "force split")

    # Decisions are live but no rerun has occurred yet.
    assert storage.latest_run_id("reflux") == first["run_id"]
    second = run_pipeline(storage, config, clock, "reflux")

    stats = second["blocking_stats"]["decision_reflux"]
    assert stats["forced_same_edges"] == 1
    assert stats["excluded_different_pairs"] == 1
    assert diff_pair["pair_key"] in stats["excluded_pair_keys"]
    # Two candidate pairs were removed from the scored candidate pool.
    assert second["candidate_pairs"] == first["candidate_pairs"] - 2

    stored_pairs = {row["pair_key"] for row in storage.list_pairs(second["run_id"])}
    assert diff_pair["pair_key"] not in stored_pairs
    assert same_pair["pair_key"] not in stored_pairs

    edge_pairs = {row["pair_key"]: row for row in storage.list_edges(second["run_id"])}
    assert edge_pairs[same_pair["pair_key"]]["kind"] == "human_same"
    # The forced-same edge keeps an auditable model score even though the
    # verdict bypassed the scored candidate pool.
    assert edge_pairs[same_pair["pair_key"]]["score"] > 0.0
    assert diff_pair["pair_key"] not in edge_pairs

    # Metrics on the new run stay consistent with the refluxed clustering.
    report = build_report(storage, config, tmp_path / "reports", second["run_id"])
    assert report["pairwise"]["f1"] == 1.0


def test_only_different_decision_reduces_candidate_count(storage, config, clock):
    first = _seed(storage, config, clock, batch_id="onlydiff")
    gold = _gold_by_key(storage, "onlydiff")
    membership = _membership(storage, first["run_id"])
    queue = next_review_pairs(storage, "onlydiff", 100)
    diff_pair = _gray_pair(queue, gold, membership, same_gold=False)

    submit_decision(storage, clock, "onlydiff", diff_pair["pair_key"], "different",
                    "reviewer-a", "split")
    second = run_pipeline(storage, config, clock, "onlydiff")
    assert second["candidate_pairs"] == first["candidate_pairs"] - 1
    assert second["blocking_stats"]["decision_reflux"]["forced_same_edges"] == 0


# --- acceptance 3: concurrency / idempotent effective decision ---------------

def test_concurrent_conflicting_decisions_leave_one_effective_row(
    storage, config, clock
):
    _seed(storage, config, clock, batch_id="conc")
    target = next_review_pairs(storage, "conc", 100)[0]
    barrier = threading.Barrier(2)
    errors: list[Exception] = []

    def submit(decision: str, reviewer: str):
        try:
            barrier.wait()
            submit_decision(
                storage, clock, "conc", target["pair_key"], decision,
                reviewer, f"concurrent {decision}",
            )
        except Exception as exc:  # pragma: no cover - surfaced via assertion
            errors.append(exc)

    threads = [
        threading.Thread(target=submit, args=("same", "reviewer-a")),
        threading.Thread(target=submit, args=("different", "reviewer-b")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors

    version, rows = load_view(storage, "conc")
    effective = [row for row in rows if row["pair_key"] == target["pair_key"]]
    assert len(effective) == 1
    assert effective[0]["decision"] in {"same", "different"}

    # The append-only audit trail keeps both submissions; the view primary key
    # guarantees no pair can have two effective rows in any version.
    history = storage.decision_history("conc", target["pair_key"])
    assert len(history) == 2
    assert {row["decision"] for row in history} == {"same", "different"}
    for old_version in range(1, version + 1):
        old_rows = [
            row for row in view_at(storage, "conc", old_version)
            if row["pair_key"] == target["pair_key"]
        ]
        assert len(old_rows) == 1


def test_duplicate_same_decision_is_idempotent_in_view(storage, config, clock):
    _seed(storage, config, clock, batch_id="idem")
    target = next_review_pairs(storage, "idem", 100)[0]
    submit_decision(storage, clock, "idem", target["pair_key"], "same",
                    "reviewer-a", "first")
    submit_decision(storage, clock, "idem", target["pair_key"], "same",
                    "reviewer-a", "duplicate")
    version, rows = load_view(storage, "idem")
    assert version == 2
    effective = [row for row in rows if row["pair_key"] == target["pair_key"]]
    assert len(effective) == 1 and effective[0]["decision"] == "same"


# --- acceptance 4: parallel runs see one immutable snapshot -------------------

def test_parallel_runs_pin_identical_view_version(storage, config, clock):
    first = _seed(storage, config, clock, batch_id="parallel")
    queue = next_review_pairs(storage, "parallel", 100)
    for pair in queue[:3]:
        submit_decision(storage, clock, "parallel", pair["pair_key"], "different",
                        "reviewer-a", "batch decision")

    pinned: list[int] = []
    barrier = threading.Barrier(2)

    def run():
        barrier.wait()
        pinned.append(pin_version(storage, "parallel"))

    threads = [threading.Thread(target=run), threading.Thread(target=run)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert pinned == [3, 3]

    runs = [
        run_pipeline(storage, config, clock, "parallel"),
        run_pipeline(storage, config, clock, "parallel"),
    ]
    assert {run["decision_view_version"] for run in runs} == {3}
    # Same pinned state and no new decisions -> identical run identity.
    assert runs[0]["run_id"] == runs[1]["run_id"]
    assert runs[0]["run_id"] != first["run_id"]
    assert runs[0]["candidate_pairs"] == runs[1]["candidate_pairs"]


def test_clean_fixture_remains_120_clusters_with_f1_one(storage, config, clock, tmp_path):
    run = _seed(storage, config, clock, batch_id="clean")
    assert run["cluster_count"] == 120
    assert run["candidate_pairs"] == 218
    assert run["blocking_stats"]["decision_reflux"] == {
        "decision_view_version": 0,
        "effective_decisions": 0,
        "forced_same_edges": 0,
        "excluded_different_pairs": 0,
        "excluded_pair_keys": [],
    }
    report = build_report(storage, config, tmp_path / "reports", run["run_id"])
    assert report["pairwise"]["f1"] == 1.0
    assert report["predicted_clusters"] == 120


# --- upgrade compatibility: historical decisions project into version 1 ------

def test_historical_decisions_are_materialized_without_rereview(
    storage, config, clock
):
    _seed(storage, config, clock, batch_id="legacy")
    target = next_review_pairs(storage, "legacy", 100)[0]

    # Simulate a pre-upgrade database: rows exist only in the append-only
    # decisions table, no view has ever been published.
    submit_decision(storage, clock, "legacy", target["pair_key"], "different",
                    "legacy-reviewer", "written before upgrade")
    storage.execute("DELETE FROM decision_views")
    storage.execute("DELETE FROM decision_view_versions")
    assert storage.current_decision_version("legacy") is None

    # Lazy materialization on the next read projects history into version 1.
    version, rows = load_view(storage, "legacy")
    assert version == 1
    effective = [row for row in rows if row["pair_key"] == target["pair_key"]]
    assert len(effective) == 1
    assert effective[0]["decision"] == "different"

    # And the refluxed run applies the historical decision without re-review.
    run = run_pipeline(storage, config, clock, "legacy")
    assert run["decision_view_version"] == 1
    assert run["blocking_stats"]["decision_reflux"]["excluded_different_pairs"] == 1


# --- drift resilience: re-alignment through logical record keys ---------------

def test_view_realigns_when_record_primary_keys_drift(storage, config, clock):
    _seed(storage, config, clock, batch_id="drift")
    target = next_review_pairs(storage, "drift", 100)[0]
    submit_decision(storage, clock, "drift", target["pair_key"], "different",
                    "reviewer-a", "before retransmission")

    # Simulate primary-key drift caused by record retransmission: delete and
    # re-insert the two records so their record_pk values change while the
    # logical (source, record_id) identities stay identical.
    pair_keys = target["pair_key"].split("||")
    placeholders = ",".join(("?",) * len(pair_keys))
    rows = storage.query(
        f"SELECT * FROM records WHERE batch_id='drift' AND "
        f"(source||'::'||record_id) IN ({placeholders}) ORDER BY record_pk",
        tuple(pair_keys),
    )
    assert len(rows) == 2
    old_pks = [row["record_pk"] for row in rows]
    payload = [dict(row) for row in rows]
    for row in payload:
        storage.execute("DELETE FROM records WHERE record_pk=?", (row["record_pk"],))
    for row in payload:
        storage.execute(
            """INSERT INTO records(batch_id,source,record_id,line_no,raw_json,
                 norm_json,title_norm,authors_norm,venue_norm,year_norm,
                 volume_norm,issue_norm,pages_norm,doi_norm,gold_cluster)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (row["batch_id"], row["source"], row["record_id"], row["line_no"],
             row["raw_json"], row["norm_json"], row["title_norm"],
             row["authors_norm"], row["venue_norm"], row["year_norm"],
             row["volume_norm"], row["issue_norm"], row["pages_norm"],
             row["doi_norm"], row["gold_cluster"]),
        )
    new_pks = [
        row["record_pk"]
        for row in storage.query(
            f"SELECT record_pk FROM records WHERE batch_id='drift' AND "
            f"(source||'::'||record_id) IN ({placeholders}) ORDER BY record_pk",
            tuple(pair_keys),
        )
    ]
    assert new_pks != old_pks

    # A new decision version re-aligns through logical keys and stays effective.
    run = run_pipeline(storage, config, clock, "drift")
    stats = run["blocking_stats"]["decision_reflux"]
    assert stats["effective_decisions"] == 1
    assert stats["excluded_different_pairs"] == 1
    version, rows = load_view(storage, "drift")
    assert rows[0]["pair_key"] == target["pair_key"]
    assert rows[0]["decision"] == "different"


def test_re_review_after_different_exclusion_flips_to_same(storage, config, clock):
    """A pair excluded by an earlier 'different' can be re-judged 'same'."""
    first = _seed(storage, config, clock, batch_id="flip")
    gold = _gold_by_key(storage, "flip")
    membership = _membership(storage, first["run_id"])
    same_pair = _gray_pair(
        next_review_pairs(storage, "flip", 100), gold, membership, same_gold=True
    )
    keys = [record["record_key"] for record in same_pair["records"]]

    submit_decision(storage, clock, "flip", same_pair["pair_key"], "different",
                    "reviewer-a", "first call: split")
    split_run = run_pipeline(storage, config, clock, "flip")
    split_map = _membership(storage, split_run["run_id"])
    assert split_map[keys[0]] != split_map[keys[1]]
    # The pair is no longer present in the latest run's pairs table.
    assert storage.get_pair(split_run["run_id"], same_pair["pair_key"]) is None

    # Re-review must still validate and persist (logical-key fallback).
    submit_decision(storage, clock, "flip", same_pair["pair_key"], "same",
                    "reviewer-b", "second call: merge")
    version, rows = load_view(storage, "flip")
    assert version == 2
    assert next(
        row for row in rows if row["pair_key"] == same_pair["pair_key"]
    )["decision"] == "same"

    merged_run = run_pipeline(storage, config, clock, "flip")
    merged_map = _membership(storage, merged_run["run_id"])
    assert merged_map[keys[0]] == merged_map[keys[1]]
    edge_kinds = {
        row["pair_key"]: row["kind"] for row in storage.list_edges(merged_run["run_id"])
    }
    assert edge_kinds[same_pair["pair_key"]] == "human_same"


@pytest.fixture
def independent_storage(tmp_path):
    instance = Storage(tmp_path / "citefuse-independent.sqlite3")
    yield instance
    instance.close()


def test_two_storage_connections_share_published_view(
    independent_storage, config, clock
):
    """A second connection (e.g. a parallel worker) sees the committed version."""
    import sqlite3

    _seed(independent_storage, config, clock, batch_id="shared")
    target = next_review_pairs(independent_storage, "shared", 100)[0]
    submit_decision(independent_storage, clock, "shared", target["pair_key"],
                    "different", "reviewer-a", "committed")

    other = sqlite3.connect(str(independent_storage.path))
    try:
        version = other.execute(
            "SELECT current_version FROM decision_view_versions WHERE batch_id='shared'"
        ).fetchone()[0]
        assert version == 1
        rows = other.execute(
            "SELECT decision FROM decision_views WHERE batch_id='shared' AND view_version=1"
        ).fetchall()
        assert rows == [("different",)]
    finally:
        other.close()
