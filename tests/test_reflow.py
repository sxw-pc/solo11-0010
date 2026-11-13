"""Acceptance tests for the batch-level instant decision reflow view.

Covers the four acceptance checks:
1. a submitted decision filters the pair out of /review/next immediately,
   without any pipeline rerun;
2. while decisions are effective but no rerun happened, the offline projection
   and the next run agree on candidate_pairs (different -> out of pool,
   same -> forced edge);
3. concurrent mutually exclusive submissions for one pair leave exactly one
   effective decision row;
4. two concurrent runs of one batch pin the same decision view version, and the
   200-record fixture still yields 120 clusters with pairwise F1 = 1.0.
"""
from __future__ import annotations

import json
import threading

from citefuse.ingest import ingest_jsonl
from citefuse.metrics import evaluate_groups
from citefuse.models import record_from_db
from citefuse.pipeline import run_pipeline
from citefuse.reflow import (
    canonical_record_keys,
    ensure_decision_view,
    is_pair_decided,
    load_snapshot,
    project_reflow,
    snapshot_fingerprint,
)
from citefuse.review import next_review_pairs, review_view_status, submit_decision
from scripts.seed_fixtures import generate


def _payload() -> bytes:
    records, _ = generate(20240517)
    return (
        "\n".join(json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records)
        + "\n"
    ).encode("utf-8")


def _seed(storage, config, clock, batch_id: str = "reflow"):
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


def _scored_pairs(storage, run_id):
    return [
        {
            "pair_key": row["pair_key"],
            "rec_a_pk": row["rec_a_pk"],
            "rec_b_pk": row["rec_b_pk"],
            "score": row["score"],
            "zone": row["zone"],
            "details": json.loads(row["details_json"]),
        }
        for row in storage.list_pairs(run_id)
    ]


def test_decision_filters_queue_immediately_without_rerun(storage, config, clock):
    first = _seed(storage, config, clock, "instant")
    run_before = storage.latest_run_id("instant")
    queue = next_review_pairs(storage, "instant", 100)
    assert queue
    target = queue[0]

    submit_decision(
        storage, clock, "instant", target["pair_key"], "different",
        "reviewer-1", "offline filter",
    )

    # No rerun happened: the latest run is still the original one.
    assert storage.latest_run_id("instant") == run_before == first["run_id"]
    remaining = next_review_pairs(storage, "instant", 100)
    assert all(pair["pair_key"] != target["pair_key"] for pair in remaining)

    snapshot = load_snapshot(storage, "instant")
    assert snapshot.version >= 1
    assert is_pair_decided(snapshot, target["pair_key"])
    status = review_view_status(storage, "instant")
    assert status["effective_decisions"] == 1
    assert status["different"] == 1 and status["same"] == 0


def test_reflow_projection_matches_next_run_candidate_counts(storage, config, clock):
    first = _seed(storage, config, clock, "counts")
    baseline_pairs = len(_scored_pairs(storage, first["run_id"]))
    queue = next_review_pairs(storage, "counts", 100)
    gold = _gold_by_key(storage, "counts")
    membership = _membership(storage, first["run_id"])

    true_pair = next(
        pair for pair in queue
        if gold[pair["records"][0]["record_key"]] == gold[pair["records"][1]["record_key"]]
        and membership[pair["records"][0]["record_key"]]
        == membership[pair["records"][1]["record_key"]]
    )
    false_pair = next(
        pair for pair in queue
        if gold[pair["records"][0]["record_key"]] != gold[pair["records"][1]["record_key"]]
        and membership[pair["records"][0]["record_key"]]
        != membership[pair["records"][1]["record_key"]]
    )

    # different -> pair leaves the candidate pool; same -> forced edge.
    submit_decision(
        storage, clock, "counts", true_pair["pair_key"], "different", "r-a", ""
    )
    submit_decision(
        storage, clock, "counts", false_pair["pair_key"], "same", "r-b", ""
    )

    records = [record_from_db(row) for row in storage.list_records("counts")]
    snapshot = load_snapshot(storage, "counts")
    projection = project_reflow(records, _scored_pairs(storage, first["run_id"]), snapshot)

    assert len(projection.candidate_pairs) == baseline_pairs - 1
    assert true_pair["pair_key"] in projection.excluded_pair_keys
    forced = {edge["pair_key"] for edge in projection.forced_same_edges}
    assert false_pair["pair_key"] in forced

    # Still no rerun at this point.
    assert storage.latest_run_id("counts") == first["run_id"]

    next_run = run_pipeline(storage, config, clock, "counts")
    assert next_run["candidate_pairs"] == baseline_pairs - 1
    assert next_run["candidate_pairs"] == len(projection.candidate_pairs)
    assert next_run["matched_edges"] == len(projection.applied_edges)
    assert next_run["cluster_count"] == len(projection.groups)
    assert next_run["decision_view_version"] == snapshot.version

    # The excluded pair is persisted but flagged, and excluded from clustering.
    excluded_row = storage.get_pair(next_run["run_id"], true_pair["pair_key"])
    assert excluded_row["excluded_by_decision"] == 1
    live_row = storage.get_pair(next_run["run_id"], false_pair["pair_key"])
    assert live_row["excluded_by_decision"] == 0


def test_concurrent_mutually_exclusive_decisions_leave_one_effective_row(
    storage, config, clock
):
    _seed(storage, config, clock, "race")
    target = next_review_pairs(storage, "race", 1)[0]
    errors: list[Exception] = []

    def submit(decision: str, reviewer: str) -> None:
        try:
            submit_decision(
                storage, clock, "race", target["pair_key"], decision, reviewer, ""
            )
        except Exception as exc:  # pragma: no cover - surfaced via assertion
            errors.append(exc)

    threads = [
        threading.Thread(target=submit, args=("same", "reviewer-x")),
        threading.Thread(target=submit, args=("different", "reviewer-y")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors

    state = storage.query(
        "SELECT * FROM decision_state WHERE batch_id=?", ("race",)
    )
    assert len(state) == 1
    effective = state[0]
    assert effective["decision"] in {"same", "different"}

    # History retains both submissions (audit); the view keeps one winner.
    history = storage.decision_history("race", target["pair_key"])
    assert len(history) == 2
    assert {row["decision"] for row in history} == {"same", "different"}
    latest = max(history, key=lambda row: row["decision_id"])
    assert latest["decision"] == effective["decision"]
    assert effective["decision_id"] == latest["decision_id"]

    # A third, identical submission is idempotent on the effective view.
    submit_decision(
        storage, clock, "race", target["pair_key"],
        effective["decision"], "reviewer-z", "repeat",
    )
    assert len(storage.query("SELECT * FROM decision_state WHERE batch_id=?", ("race",))) == 1


def test_concurrent_runs_pin_identical_snapshot_and_fixture_quality_holds(
    storage, config, clock
):
    _seed(storage, config, clock, "parallel")
    queue = next_review_pairs(storage, "parallel", 100)
    for pair in queue[:3]:
        submit_decision(
            storage, clock, "parallel", pair["pair_key"], "different", "r", ""
        )

    results: list[dict] = []
    errors: list[Exception] = []

    def run() -> None:
        try:
            results.append(run_pipeline(storage, config, clock, "parallel"))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=run), threading.Thread(target=run)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len(results) == 2

    snapshots = [
        load_snapshot(storage, "parallel"),
        load_snapshot(storage, "parallel"),
    ]
    assert snapshots[0].version == snapshots[1].version
    assert snapshot_fingerprint(snapshots[0]) == snapshot_fingerprint(snapshots[1])
    # Both runs pinned the same immutable view, so they derive the same run_id.
    assert len({run["run_id"] for run in results}) == 1
    assert results[0]["decision_view_version"] == results[1]["decision_view_version"]
    assert results[0]["candidate_pairs"] == results[1]["candidate_pairs"]

    # Undisturbed fixture baseline: no decisions on a fresh batch keeps the
    # documented 120 clusters and perfect pairwise F1.
    clean = _seed(storage, config, clock, "clean-baseline")
    assert clean["cluster_count"] == 120
    records = [record_from_db(row) for row in storage.list_records("clean-baseline")]
    by_pk = {record.pk: record for record in records}
    clustered: list[list] = []
    for cluster in storage.list_clusters(clean["run_id"]):
        pks = json.loads(cluster["member_pks_json"])
        clustered.append([by_pk[pk] for pk in pks])
    metrics = evaluate_groups(records, clustered)
    assert metrics["pairwise"]["f1"] == 1.0
    assert metrics["predicted_clusters"] == 120


def test_historical_decisions_are_backfilled_without_rereview(storage, config, clock):
    # Simulate a pre-upgrade database: decisions exist but no decision_state.
    _seed(storage, config, clock, "legacy")
    target = next_review_pairs(storage, "legacy", 1)[0]
    submit_decision(storage, clock, "legacy", target["pair_key"], "different", "old", "")
    storage.execute("DELETE FROM decision_state WHERE batch_id=?", ("legacy",))
    assert storage.decision_state_version("legacy") is None

    version = ensure_decision_view(storage, "legacy")
    assert version == 1
    rows = storage.decision_state_rows("legacy")
    assert len(rows) == 1
    assert rows[0]["decision"] == "different"
    # The backfilled view immediately drives offline queue filtering.
    assert all(
        pair["pair_key"] != target["pair_key"]
        for pair in next_review_pairs(storage, "legacy", 100)
    )


def test_pair_key_drift_still_matches_on_canonical_record_keys(
    storage, config, clock
):
    _seed(storage, config, clock, "drift")
    target = next_review_pairs(storage, "drift", 1)[0]
    submit_decision(storage, clock, "drift", target["pair_key"], "different", "r", "")

    # Rewrite the view's display pair_key to simulate primary-key drift after a
    # retransmission; identity must remain the canonical record-key pair.
    left, right = canonical_record_keys(
        *target["pair_key"].split("||", 1)
    )
    storage.execute(
        "UPDATE decision_state SET pair_key=? WHERE batch_id=? AND rec_a_key=? AND rec_b_key=?",
        ("DRIFTED||KEY", "drift", left, right),
    )
    snapshot = load_snapshot(storage, "drift")
    assert is_pair_decided(snapshot, target["pair_key"])
    assert all(
        pair["pair_key"] != target["pair_key"]
        for pair in next_review_pairs(storage, "drift", 100)
    )


def test_upgrade_from_legacy_schema_migrates_and_keeps_decisions_effective(
    tmp_path, config, clock
):
    # Build a database with the pre-upgrade schema (no decision_state, no new
    # columns), then reopen it through the current Storage to prove the
    # idempotent migration plus history backfill keep old decisions effective.
    import sqlite3

    legacy_path = tmp_path / "legacy.sqlite3"
    raw = sqlite3.connect(str(legacy_path))
    raw.executescript(
        """
        CREATE TABLE batches (
          batch_id TEXT PRIMARY KEY, name TEXT, input_hash TEXT NOT NULL,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          total_lines INTEGER NOT NULL DEFAULT 0, valid_lines INTEGER NOT NULL DEFAULT 0,
          bad_lines INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'ingested'
        );
        CREATE TABLE records (
          record_pk INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id TEXT NOT NULL, source TEXT NOT NULL, record_id TEXT NOT NULL,
          line_no INTEGER NOT NULL, raw_json TEXT NOT NULL, norm_json TEXT NOT NULL,
          title_norm TEXT NOT NULL, authors_norm TEXT NOT NULL,
          venue_norm TEXT, year_norm INTEGER, volume_norm TEXT, issue_norm TEXT,
          pages_norm TEXT, doi_norm TEXT, gold_cluster TEXT,
          UNIQUE(batch_id, source, record_id)
        );
        CREATE TABLE dead_letters (
          dead_id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL,
          line_no INTEGER NOT NULL, reason TEXT NOT NULL, raw_text TEXT NOT NULL,
          UNIQUE(batch_id, line_no, reason, raw_text)
        );
        CREATE TABLE runs (
          run_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, config_hash TEXT NOT NULL,
          input_rows INTEGER NOT NULL, bad_rows INTEGER NOT NULL,
          candidate_pairs INTEGER NOT NULL, matched_edges INTEGER NOT NULL,
          cluster_count INTEGER NOT NULL, golden_count INTEGER NOT NULL,
          timings_json TEXT NOT NULL, blocking_stats_json TEXT NOT NULL,
          status TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE run_attempts (
          attempt_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
          batch_id TEXT NOT NULL, config_hash TEXT NOT NULL,
          input_rows INTEGER NOT NULL, bad_rows INTEGER NOT NULL,
          candidate_pairs INTEGER NOT NULL, matched_edges INTEGER NOT NULL,
          cluster_count INTEGER NOT NULL, golden_count INTEGER NOT NULL,
          timings_json TEXT NOT NULL, status TEXT NOT NULL,
          started_at TEXT NOT NULL, finished_at TEXT NOT NULL
        );
        CREATE TABLE pairs (
          run_id TEXT NOT NULL, pair_key TEXT NOT NULL,
          rec_a_pk INTEGER NOT NULL, rec_b_pk INTEGER NOT NULL,
          score REAL NOT NULL, zone TEXT NOT NULL,
          strategies_json TEXT NOT NULL, details_json TEXT NOT NULL,
          PRIMARY KEY(run_id, pair_key)
        );
        CREATE TABLE edges (
          run_id TEXT NOT NULL, pair_key TEXT NOT NULL,
          rec_a_pk INTEGER NOT NULL, rec_b_pk INTEGER NOT NULL,
          score REAL NOT NULL, kind TEXT NOT NULL, details_json TEXT NOT NULL,
          PRIMARY KEY(run_id, pair_key)
        );
        CREATE TABLE clusters (
          run_id TEXT NOT NULL, cluster_id TEXT NOT NULL, batch_id TEXT NOT NULL,
          member_keys_json TEXT NOT NULL, member_pks_json TEXT NOT NULL,
          golden_json TEXT NOT NULL, PRIMARY KEY(run_id, cluster_id)
        );
        CREATE TABLE decisions (
          decision_id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL,
          pair_key TEXT NOT NULL, rec_a_key TEXT NOT NULL, rec_b_key TEXT NOT NULL,
          decision TEXT NOT NULL, reviewer TEXT NOT NULL, note TEXT NOT NULL,
          submitted_at TEXT NOT NULL
        );
        CREATE TABLE audit_events (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT,
          batch_id TEXT NOT NULL, event_type TEXT NOT NULL,
          detail_json TEXT NOT NULL, created_at TEXT NOT NULL
        );
        INSERT INTO batches(batch_id,name,input_hash,created_at,updated_at,
          total_lines,valid_lines,bad_lines,status)
        VALUES('legacy-upgrade','legacy','h','2024-12-31T00:00:00Z',
          '2024-12-31T00:00:00Z',2,2,0,'ingested');
        INSERT INTO decisions(batch_id,pair_key,rec_a_key,rec_b_key,decision,
          reviewer,note,submitted_at)
        VALUES('legacy-upgrade','A::1||B::2','A::1','B::2','different',
          'veteran','pre-upgrade call','2024-12-31T00:00:00Z');
        """
    )
    raw.commit()
    raw.close()

    from citefuse.storage import Storage

    upgraded = Storage(legacy_path)
    # Migration added the columns without losing history; the view is then
    # backfilled lazily from the append-only decisions table on first touch.
    cols_pairs = {r[1] for r in upgraded.query("PRAGMA table_info(pairs)")}
    assert "excluded_by_decision" in cols_pairs
    cols_runs = {r[1] for r in upgraded.query("PRAGMA table_info(runs)")}
    assert "decision_view_version" in cols_runs
    assert upgraded.decision_state_version("legacy-upgrade") is None

    ensure_decision_view(upgraded, "legacy-upgrade")
    view = upgraded.decision_state_rows("legacy-upgrade")
    assert len(view) == 1 and view[0]["decision"] == "different"
    assert upgraded.decision_state_version("legacy-upgrade") == 1
    upgraded.close()

    # Reopening again is idempotent (no error, version unchanged).
    again = Storage(legacy_path)
    assert again.decision_state_version("legacy-upgrade") == 1
    assert len(again.decision_state_rows("legacy-upgrade")) == 1
    again.close()

