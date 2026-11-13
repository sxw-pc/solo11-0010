"""Batch-level, versioned decision view and instant offline reflow.

The materialized ``decision_state`` view holds exactly one *effective* decision
per canonical record-key pair per batch. It is the projection that review
queue filtering and pipeline runs read; ``decisions`` remains the append-only
audit source of truth and is used to (re)build the view idempotently.

Why a versioned snapshot instead of dynamically joining ``decisions`` to
``pairs`` on every request:

* pair identity here is the canonical record-key pair
  (``rec_a_key``/``rec_b_key``), never the possibly-drifting ``pair_key``, so a
  retransmission that changes a pair_key cannot silently orphan a decision;
* each view carries a monotonically increasing ``view_version``. A run pins a
  single (version, rows) snapshot inside one locked transaction, so two
  concurrent runs of the same batch observe byte-identical decision state;
* ``/review/next`` reads an immutable version instead of recomputing against a
  moving pairs table, so its response cannot drift between calls.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .clustering import cluster_records
from .models import Record, make_pair_key, pair_order
from .storage import Storage


@dataclass(frozen=True)
class DecisionSnapshot:
    """An immutable, versioned pin of one batch's effective decision view."""

    batch_id: str
    version: int
    rows: tuple[dict[str, Any], ...]

    def canonical(self) -> list[tuple[str, str, str]]:
        """Stable (rec_a_key, rec_b_key, decision) fingerprint, sorted."""
        return sorted(
            (row["rec_a_key"], row["rec_b_key"], row["decision"]) for row in self.rows
        )


def canonical_record_keys(rec_a_key: str, rec_b_key: str) -> tuple[str, str]:
    """Order two record keys by the documented canonical rule (record_id, source)."""
    first, second = sorted((rec_a_key, rec_b_key), key=pair_order)
    return first, second


def _history_to_state_rows(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse append-only history to one effective row per canonical pair.

    Newest (submitted_at, decision_id) wins, mirroring resolve_decisions. Rows
    whose stored rec keys are absent/blank fall back to parsing pair_key so
    legacy decisions keep working after the upgrade.
    """
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for decision in sorted(
        history, key=lambda item: (item["submitted_at"], item["decision_id"])
    ):
        left = decision.get("rec_a_key") or ""
        right = decision.get("rec_b_key") or ""
        if not left or not right:
            parts = decision["pair_key"].split("||", 1)
            if len(parts) != 2:
                continue
            left, right = parts
        left, right = canonical_record_keys(left, right)
        latest[(left, right)] = decision
    return [
        {
            "pair_key": make_pair_key(left, right)[2],
            "rec_a_key": left,
            "rec_b_key": right,
            "decision": row["decision"],
            "reviewer": row["reviewer"],
            "note": row["note"],
            "decision_id": row["decision_id"],
            "submitted_at": row["submitted_at"],
        }
        for (left, right), row in sorted(latest.items())
    ]


def ensure_decision_view(storage: Storage, batch_id: str) -> int:
    """Make sure the materialized view exists for a batch; return its version.

    On first touch (including an upgraded database that already holds decision
    history) the view is rebuilt inside one transaction from ``decisions``, so
    every historically submitted decision stays effective with no re-review.
    """
    with storage.transaction() as conn:
        existing = conn.execute(
            "SELECT MAX(view_version) AS v FROM decision_state WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if existing is not None and existing["v"] is not None:
            return int(existing["v"])
        history = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM decisions WHERE batch_id=? ORDER BY submitted_at,decision_id",
                (batch_id,),
            ).fetchall()
        ]
        rows = _history_to_state_rows(history)
        storage.replace_decision_state(conn, batch_id, rows)
        return 1


def load_snapshot(storage: Storage, batch_id: str) -> DecisionSnapshot:
    """Pin the current effective view as an immutable versioned snapshot.

    Version and rows are read inside one locked transaction, so a decision
    committing between two reads can never mix an old version number with new
    rows. A batch that has never been touched gets a lazily built version 1
    (possibly empty).
    """
    with storage.transaction() as conn:
        existing = conn.execute(
            "SELECT MAX(view_version) AS v FROM decision_state WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if existing is None or existing["v"] is None:
            history = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM decisions WHERE batch_id=?"
                    " ORDER BY submitted_at,decision_id",
                    (batch_id,),
                ).fetchall()
            ]
            storage.replace_decision_state(conn, batch_id, _history_to_state_rows(history))
            version = 1
        else:
            version = int(existing["v"])
        rows = tuple(
            dict(row)
            for row in conn.execute(
                "SELECT * FROM decision_state WHERE batch_id=?"
                " ORDER BY rec_a_key,rec_b_key",
                (batch_id,),
            ).fetchall()
        )
    return DecisionSnapshot(batch_id=batch_id, version=version, rows=rows)


def snapshot_fingerprint(snapshot: DecisionSnapshot) -> str:
    """Deterministic fingerprint of the effective view, independent of pair_key."""
    material = json.dumps(
        [[snapshot.version, *item] for item in snapshot.canonical()],
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _index_by_pair_key(snapshot: DecisionSnapshot) -> dict[str, dict[str, Any]]:
    return {row["pair_key"]: row for row in snapshot.rows}


def _index_by_record_keys(snapshot: DecisionSnapshot) -> dict[tuple[str, str], dict[str, Any]]:
    return {(row["rec_a_key"], row["rec_b_key"]): row for row in snapshot.rows}


def is_pair_decided(snapshot: DecisionSnapshot, pair_key: str) -> bool:
    """Whether a queue pair already has an effective decision.

    Matches on the canonical record-key pair derived from pair_key, so a
    pair_key drift between runs still resolves to the right decision.
    """
    parts = pair_key.split("||", 1)
    if len(parts) != 2:
        return pair_key in _index_by_pair_key(snapshot)
    left, right = canonical_record_keys(parts[0], parts[1])
    return (left, right) in _index_by_record_keys(snapshot)


@dataclass(frozen=True)
class ReflowProjection:
    """Offline result of applying the decision view before a rerun.

    candidate_pairs counts pairs still in the candidate pool after human
    'different' pairs are removed (human 'same' pairs stay in the pool but are
    forced into the edge set). effective_decisions is the pinned view.
    """

    snapshot: DecisionSnapshot
    candidate_pairs: list[dict[str, Any]]
    forced_same_edges: list[dict[str, Any]]
    excluded_pair_keys: frozenset[str]
    groups: list[list[Record]]
    applied_edges: list[dict[str, Any]]
    conflicts: list[dict[str, Any]]


def project_reflow(
    records: list[Record],
    scored_pairs: list[dict[str, Any]],
    snapshot: DecisionSnapshot,
) -> ReflowProjection:
    """Apply effective decisions to scored candidates without a pipeline rerun.

    * human 'different': the pair is removed from the candidate pool;
    * human 'same': the pair stays a candidate but is forced into the edge set
      before model scoring edges are considered;
    * everything else is untouched. Clustering then runs exactly as a real run
      would, so the projection matches the next /runs result deterministically.
    """
    by_key = {record.key: record for record in records}
    by_pk = {record.pk: record for record in records}
    by_pair_key = _index_by_pair_key(snapshot)
    by_record_keys = _index_by_record_keys(snapshot)

    def decided_for(pair: dict[str, Any]) -> dict[str, Any] | None:
        direct = by_pair_key.get(pair["pair_key"])
        if direct is not None:
            return direct
        left = by_pk.get(pair["rec_a_pk"])
        right = by_pk.get(pair["rec_b_pk"])
        if left is None or right is None:
            return None
        canon = canonical_record_keys(left.key, right.key)
        return by_record_keys.get(canon)

    candidate_pool: list[dict[str, Any]] = []
    excluded: set[str] = set()
    model_edges: list[dict[str, Any]] = []
    for pair in scored_pairs:
        decision = decided_for(pair)
        if decision is not None and decision["decision"] == "different":
            excluded.add(pair["pair_key"])
            continue
        candidate_pool.append(pair)
        if decision is None and pair["zone"] == "match":
            model_edges.append({
                "pair_key": pair["pair_key"],
                "rec_a_pk": pair["rec_a_pk"],
                "rec_b_pk": pair["rec_b_pk"],
                "score": pair["score"],
                "details": pair["details"],
            })

    # Human same pairs become mandatory edges, even if blocking never produced
    # them as candidates (a reflow decision can force a previously unseen pair).
    pair_scores = {pair["pair_key"]: pair["score"] for pair in scored_pairs}
    same_rows = sorted(
        (row for row in snapshot.rows if row["decision"] == "same"),
        key=lambda row: (row["submitted_at"], row["decision_id"]),
    )
    forced_same_edges: list[dict[str, Any]] = []
    for row in same_rows:
        left, right = by_key.get(row["rec_a_key"]), by_key.get(row["rec_b_key"])
        if left is None or right is None:
            continue
        pair_key = make_pair_key(left.key, right.key)[2]
        forced_same_edges.append({
            "pair_key": pair_key,
            "rec_a_pk": left.pk,
            "rec_b_pk": right.pk,
            "score": pair_scores.get(pair_key, 0.0),
            "details": {"decision_id": row["decision_id"]},
        })

    # Reuse the production union-find semantics by replaying history-shaped rows.
    history = [
        {
            "decision_id": row["decision_id"],
            "pair_key": row["pair_key"],
            "rec_a_key": row["rec_a_key"],
            "rec_b_key": row["rec_b_key"],
            "decision": row["decision"],
            "reviewer": row["reviewer"],
            "note": row["note"],
            "submitted_at": row["submitted_at"],
        }
        for row in snapshot.rows
    ]
    groups, applied_edges, conflicts = cluster_records(records, model_edges, history)
    return ReflowProjection(
        snapshot=snapshot,
        candidate_pairs=candidate_pool,
        forced_same_edges=forced_same_edges,
        excluded_pair_keys=frozenset(excluded),
        groups=groups,
        applied_edges=applied_edges,
        conflicts=conflicts,
    )


__all__ = [
    "DecisionSnapshot",
    "ReflowProjection",
    "canonical_record_keys",
    "ensure_decision_view",
    "is_pair_decided",
    "load_snapshot",
    "project_reflow",
    "snapshot_fingerprint",
]
