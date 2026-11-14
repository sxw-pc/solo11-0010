"""Human review queue, decision persistence and cross-batch inheritance.

Decision scopes
---------------
* ``batch`` (default): a decision stored in ``decisions`` that only affects
  its submitting batch. Legacy rows keep this behavior with no migration.
* ``cross_batch``: a global decision stored in ``cross_batch_decisions``. It
  survives source-batch deletion and may be inherited by later batches whose
  pair matches both the pair_key *and* both business records (source/id plus
  a normalized-content fingerprint). A partial unique index guarantees a
  single active cross-batch decision per pair_key; a newer mutually
  exclusive submission supersedes the older one by submission ordering.

Inheritance is frozen per batch
--------------------------------
When a pipeline run materializes an inherited decision it writes a snapshot
row to ``decision_inheritances``. Reruns of that batch read only the frozen
snapshot, so later cross-batch submissions can never change an older
batch's reproducible pairs/clusters/golden. Local batch decisions always
take precedence over inheritance.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from .clock import Clock
from .models import (
    SCOPE_BATCH,
    SCOPE_CROSS_BATCH,
    make_pair_key,
    pair_order,
    record_from_db,
    row_fingerprint,
)
from .repository import DecisionConflict
from .storage import Storage

# Dict keys used to tag an inherited decision for clustering and auditing.
INHERITED_FLAG = "inherited"
SOURCE_BATCH_KEY = "source_batch_id"
SOURCE_DECISION_KEY = "source_decision_id"


def decision_dicts(storage: Storage, batch_id: str) -> list[dict[str, Any]]:
    """Batch-scoped decision rows for ``batch_id`` (legacy contract)."""
    return [dict(row) for row in storage.decision_history(batch_id)]


def cross_decision_dicts(
    storage: Storage, source_batch_id: str | None = None
) -> list[dict[str, Any]]:
    return [dict(row) for row in storage.list_cross_decision_history(source_batch_id)]


def latest_decision_map(history: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for decision in sorted(
        history, key=lambda item: (item["submitted_at"], item["decision_id"])
    ):
        latest[decision["pair_key"]] = decision
    return latest


def _records_for_pair(storage: Storage, left_pk: int, right_pk: int):
    rows = storage.query(
        "SELECT * FROM records WHERE record_pk IN (?,?) ORDER BY source,record_id",
        (left_pk, right_pk),
    )
    return [record_from_db(row) for row in rows]


def _canonical_pair(rows: list[sqlite3.Row]) -> tuple[sqlite3.Row, sqlite3.Row, str, str]:
    """Return (first_row, second_row, rec_a_key, rec_b_key) in canonical order.

    Uses the same (record_id, source) ordering rule as
    :func:`models.make_pair_key`, so identity validation can never disagree
    with the pair_key's own side ordering.
    """
    def key(row: sqlite3.Row) -> tuple[str, str]:
        return pair_order(f"{row['source']}::{row['record_id']}")

    first, second = sorted(rows, key=key)
    rec_a_key = f"{first['source']}::{first['record_id']}"
    rec_b_key = f"{second['source']}::{second['record_id']}"
    return first, second, rec_a_key, rec_b_key


def _pair_identity_matches(
    cross: dict[str, Any], first: sqlite3.Row, second: sqlite3.Row, pair_key: str
) -> tuple[bool, str | None]:
    """Validate pair_key + both business identities against a cross decision.

    Returns (matched, reason). A mismatch means the pair_key was recycled by
    a delete/rebuild and the decision must not be blindly inherited.
    """
    rec_a_key = f"{first['source']}::{first['record_id']}"
    rec_b_key = f"{second['source']}::{second['record_id']}"
    _, _, canonical_pair_key = make_pair_key(rec_a_key, rec_b_key)
    if pair_key != cross["pair_key"] or canonical_pair_key != cross["pair_key"]:
        return False, "pair_key_mismatch"
    if rec_a_key != cross["rec_a_key"] or rec_b_key != cross["rec_b_key"]:
        return False, "record_key_mismatch"
    fp_a, fp_b = row_fingerprint(first), row_fingerprint(second)
    if fp_a != cross["rec_a_fingerprint"] or fp_b != cross["rec_b_fingerprint"]:
        return False, "record_content_mismatch"
    return True, None


def resolve_inheritances(
    storage: Storage,
    batch_id: str,
    candidate_pairs: list[Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Resolve cross-batch decisions against a batch's candidate pairs.

    ``candidate_pairs`` are row-like objects exposing ``pair_key``,
    ``rec_a_pk`` and ``rec_b_pk`` (stored pair rows or in-memory candidates),
    which lets the pipeline resolve inheritance before pairs are persisted.

    Returns (pins, mismatches, history_hits) where:

    * ``pins`` are cross decisions whose pair_key and both business identities
      match a candidate pair in this run; they become the frozen inheritance
      snapshot and feed clustering;
    * ``mismatches`` are pair_keys present in this run whose stored cross
      decision fails identity validation — these fall back to fresh gray
      pairs but carry the historical decision provenance in the review queue;
    * ``history_hits`` are the active cross decision rows involved, for
      provenance display.
    """
    pairs = {row["pair_key"]: row for row in candidate_pairs}
    active = {
        row["pair_key"]: dict(row)
        for row in storage.list_active_cross_decisions(list(pairs))
    }
    local_keys = {row["pair_key"] for row in storage.decision_history(batch_id)}
    pins: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    history_hits: list[dict[str, Any]] = []
    for pair_key, cross in active.items():
        pair = pairs.get(pair_key)
        if pair is None or pair_key in local_keys:
            # Local decisions always win; a pair no longer candidate cannot
            # be inherited either.
            continue
        if cross["source_batch_id"] == batch_id:
            # This batch's own cross-batch decisions bind through the
            # "own cross" channel, never through the frozen snapshot.
            continue
        rows = storage.query(
            "SELECT * FROM records WHERE record_pk IN (?,?) ORDER BY source,record_id",
            (pair["rec_a_pk"], pair["rec_b_pk"]),
        )
        if len(rows) != 2:
            mismatches.append({"pair_key": pair_key, "reason": "records_missing",
                               "cross_decision": cross})
            history_hits.append(cross)
            continue
        first, second, _, _ = _canonical_pair(rows)
        matched, reason = _pair_identity_matches(cross, first, second, pair_key)
        history_hits.append(cross)
        if not matched:
            mismatches.append({"pair_key": pair_key, "reason": reason,
                               "cross_decision": cross})
            continue
        pins.append({
            "source_decision_id": cross["decision_id"],
            "source_batch_id": cross["source_batch_id"],
            "pair_key": pair_key,
            "rec_a_key": cross["rec_a_key"],
            "rec_b_key": cross["rec_b_key"],
            "rec_a_fingerprint": cross["rec_a_fingerprint"],
            "rec_b_fingerprint": cross["rec_b_fingerprint"],
            "decision": cross["decision"],
            "submitted_at": cross["submitted_at"],
        })
    pins.sort(key=lambda pin: (pin["submitted_at"], pin["source_decision_id"],
                               pin["pair_key"]))
    mismatches.sort(key=lambda item: item["pair_key"])
    history_hits.sort(key=lambda item: (item["submitted_at"], item["decision_id"]))
    return pins, mismatches, history_hits


def own_cross_identity_mismatches(
    storage: Storage, batch_id: str, candidate_pairs: list[Any]
) -> list[dict[str, Any]]:
    """Active own-batch cross decisions whose business identities no longer match.

    After a same-name delete-and-rebuild the cross decision row survives but
    the records behind a recycled pair_key may have changed. Such decisions
    must not bind and are reported so the pipeline can audit them and the
    review queue can re-offer the pair as a fresh gray item.
    """
    pair_by_key = {row["pair_key"]: row for row in candidate_pairs}
    mismatches: list[dict[str, Any]] = []
    for row in storage.list_cross_decision_history(batch_id):
        cross = dict(row)
        if cross["status"] != "active":
            continue
        pair = pair_by_key.get(cross["pair_key"])
        if pair is None:
            continue
        matched, reason = _cross_decision_matches_batch(
            storage, batch_id, cross, pair
        )
        if not matched:
            mismatches.append({
                "pair_key": cross["pair_key"],
                "reason": reason,
                "cross_decision": cross,
            })
    mismatches.sort(key=lambda item: item["pair_key"])
    return mismatches


def _identity_for_candidate(
    storage: Storage, cross: dict[str, Any], pair: Any
) -> tuple[bool, str | None]:
    rows = storage.query(
        "SELECT * FROM records WHERE record_pk IN (?,?) ORDER BY source,record_id",
        (pair["rec_a_pk"], pair["rec_b_pk"]),
    )
    if len(rows) != 2:
        return False, "records_missing"
    first, second, _, _ = _canonical_pair(rows)
    return _pair_identity_matches(cross, first, second, cross["pair_key"])


def _cross_decision_matches_batch(
    storage: Storage, batch_id: str, cross: dict[str, Any], candidate: Any = None
) -> tuple[bool, str | None]:
    """Identity-check one active cross decision against live batch records.

    Used for the source batch's own cross decisions, which are not part of
    the frozen snapshot and therefore must be re-validated on every run
    (notably after a same-name delete-and-rebuild). ``candidate`` may carry
    an in-memory pair row before pairs are persisted.
    """
    pair = candidate
    if pair is None:
        run_id = storage.latest_run_id(batch_id)
        if run_id is None:
            return False, "no_run"
        pair = storage.get_pair(run_id, cross["pair_key"])
        if pair is None:
            return False, "not_candidate"
    return _identity_for_candidate(storage, cross, pair)


def effective_decision_history(
    storage: Storage, batch_id: str, candidate_pairs: list[Any] | None = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """All decisions that bind clustering for a batch, plus dropped pins.

    Returns ``(history, dropped_frozen)``. Precedence, highest first:

    1. local batch-scoped rows in ``decisions`` (including legacy rows);
    2. cross-batch decisions *submitted by this batch* — they behave like
       local decisions and reflect the latest active row, but every run
       re-validates the business identities so a same-name rebuild that
       recycled the pair_key cannot blindly rebind;
    3. the frozen inheritance snapshot of cross decisions submitted by other
       batches. The snapshot is the only inheritance input, but each frozen
       pin is still identity-checked against the live candidate pair: if the
       records' content drifted the stale pin is dropped (returned in
       ``dropped_frozen``) instead of silently overriding a higher-priority
       channel that refused to bind.

    ``candidate_pairs`` carries in-memory pairs before they are persisted;
    when omitted the latest stored run's pairs are used (metrics/reruns).
    """
    pair_by_key: dict[str, Any] = {}
    if candidate_pairs is not None:
        pair_by_key = {row["pair_key"]: row for row in candidate_pairs}
    else:
        run_id = storage.latest_run_id(batch_id)
        if run_id is not None:
            pair_by_key = {
                row["pair_key"]: row for row in storage.list_pairs(run_id)
            }

    history = decision_dicts(storage, batch_id)
    claimed = {row["pair_key"] for row in history}
    dropped_frozen: list[dict[str, Any]] = []

    own_cross = [
        dict(row)
        for row in storage.list_cross_decision_history(batch_id)
        if row["status"] == "active"
    ]
    for row in own_cross:
        if row["pair_key"] in claimed:
            continue
        candidate = pair_by_key.get(row["pair_key"])
        matched = False
        if candidate is not None:
            matched, _ = _cross_decision_matches_batch(
                storage, batch_id, row, candidate
            )
        if not matched:
            # Rebuilt records no longer match: the historical decision must
            # not bind; the pair falls back to a fresh gray-zone review item.
            continue
        row[INHERITED_FLAG] = False
        row["origin"] = "own_cross_batch"
        claimed.add(row["pair_key"])
        history.append(row)

    for row in storage.list_inheritances(batch_id):
        pin = dict(row)
        if pin["pair_key"] in claimed:
            continue
        candidate = pair_by_key.get(pin["pair_key"])
        if candidate is not None:
            matched, reason = _identity_for_candidate(storage, pin, candidate)
            if not matched:
                # Content drifted behind the frozen pair_key: never let the
                # stale pin bind clustering unconditionally.
                dropped_frozen.append({"pin": pin, "reason": reason})
                continue
        pin["decision_id"] = pin["source_decision_id"]
        pin[INHERITED_FLAG] = True
        pin["origin"] = "inherited"
        history.append(pin)
    history.sort(key=lambda item: (item["submitted_at"], item["decision_id"]))
    return history, dropped_frozen


def _provenance_for_pair(
    cross: dict[str, Any], *, inherited: bool, mismatch_reason: str | None
) -> dict[str, Any]:
    return {
        "scope": SCOPE_CROSS_BATCH,
        "inherited": inherited,
        "mismatch_reason": mismatch_reason,
        "source_batch_id": cross["source_batch_id"],
        "source_decision_id": cross["decision_id"],
        "decision": cross["decision"],
        "reviewer": cross["reviewer"],
        "note": cross["note"],
        "submitted_at": cross["submitted_at"],
    }


def next_review_pairs(
    storage: Storage, batch_id: str, limit: int
) -> list[dict[str, Any]]:
    """Return unresolved gray-zone pairs from the batch's latest pipeline run.

    A pair is hidden only through the same channels that bind clustering:

    1. a local batch-scoped decision;
    2. a cross-batch decision submitted by *this* batch whose business
       identities still validate (the own-cross channel);
    3. a row in the batch's frozen inheritance snapshot.

    A global cross-batch decision that arrived after the batch's first run is
    deliberately *not* a binding channel here: hiding the pair would make the
    review queue disagree with clustering and let later global state mutate an
    older batch. Such a pair stays reviewable and merely carries the historical
    decision as provenance. Identity failures (a rebuild recycled the
    pair_key) likewise keep the pair as a fresh gray item.
    """
    run_id = storage.latest_run_id(batch_id)
    if not run_id:
        return []
    local = latest_decision_map(decision_dicts(storage, batch_id))
    frozen = {
        row["pair_key"]: dict(row) for row in storage.list_inheritances(batch_id)
    }
    own_cross = {
        row["pair_key"]: dict(row)
        for row in storage.list_cross_decision_history(batch_id)
        if row["status"] == "active"
    }
    review_pairs = storage.list_pairs(run_id, zone="review")
    # Only load the global rows that can actually annotate this queue, rather
    # than scanning every active cross decision on every request.
    review_keys = [pair["pair_key"] for pair in review_pairs]
    relevant = {
        row["pair_key"]: dict(row)
        for row in storage.list_active_cross_decisions(review_keys)
    }
    output: list[dict[str, Any]] = []
    for pair in review_pairs:
        pair_key = pair["pair_key"]
        if pair_key in local or pair_key in frozen:
            continue
        own = own_cross.get(pair_key)
        if own is not None:
            matched, _ = _cross_decision_matches_batch(storage, batch_id, own, pair)
            if matched:
                continue  # bound through the own-cross channel
        records = _records_for_pair(storage, pair["rec_a_pk"], pair["rec_b_pk"])
        item = {
            "pair_key": pair_key,
            "score": pair["score"],
            "zone": pair["zone"],
            "strategies": json.loads(pair["strategies_json"]),
            "details": json.loads(pair["details_json"]),
            "records": [record.public() for record in records],
        }
        cross = relevant.get(pair_key)
        if cross is not None:
            rows = storage.query(
                "SELECT * FROM records WHERE record_pk IN (?,?) "
                "ORDER BY source,record_id",
                (pair["rec_a_pk"], pair["rec_b_pk"]),
            )
            reason = "records_missing"
            if len(rows) == 2:
                first, second, _, _ = _canonical_pair(rows)
                _, reason = _pair_identity_matches(cross, first, second, pair_key)
            is_frozen = pair_key in frozen
            item["historical_decision"] = _provenance_for_pair(
                cross,
                inherited=is_frozen,
                # None reason means identity holds but the decision only
                # arrived after this batch's snapshot was frozen.
                mismatch_reason=None if reason is None else reason,
            )
        output.append(item)
        if len(output) >= limit:
            break
    return output


def _load_candidate_pair(
    storage: Storage, batch_id: str, pair_key: str
) -> tuple[str, sqlite3.Row, sqlite3.Row, str, str]:
    run_id = storage.latest_run_id(batch_id)
    if not run_id:
        raise LookupError(f"batch {batch_id!r} has no pipeline run")
    pair = storage.get_pair(run_id, pair_key)
    if pair is None:
        raise LookupError(f"pair {pair_key!r} is not a candidate in the latest run")
    rows = storage.query(
        "SELECT * FROM records WHERE record_pk IN (?,?) ORDER BY source,record_id",
        (pair["rec_a_pk"], pair["rec_b_pk"]),
    )
    if len(rows) != 2:
        raise LookupError("pair references missing records")
    first, second, rec_a_key, rec_b_key = _canonical_pair(rows)
    return run_id, first, second, rec_a_key, rec_b_key


def submit_decision(
    storage: Storage,
    clock: Clock,
    batch_id: str,
    pair_key: str,
    decision: str,
    reviewer: str,
    note: str,
    scope: str = SCOPE_BATCH,
) -> dict[str, Any]:
    """Validate a pair against the latest run and persist a review decision.

    Batch-scoped decisions land in ``decisions`` exactly as before.
    Cross-batch decisions land in the global table: any prior active decision
    on the same pair_key is superseded inside the same transaction, and the
    partial unique index makes concurrent mutually exclusive submissions
    collapse to a single effective row.
    """
    if decision not in {"same", "different"}:
        raise ValueError("decision must be 'same' or 'different'")
    if scope not in (SCOPE_BATCH, SCOPE_CROSS_BATCH):
        raise ValueError(f"scope must be one of {SCOPE_BATCH!r}, {SCOPE_CROSS_BATCH!r}")
    _, first, second, rec_a_key, rec_b_key = _load_candidate_pair(
        storage, batch_id, pair_key
    )
    reviewer_name = reviewer.strip() or "anonymous"
    note_text = note.strip()
    submitted_at = clock.now_iso()

    if scope == SCOPE_BATCH:
        decision_id = storage.add_decision(
            batch_id, pair_key, rec_a_key, rec_b_key, decision,
            reviewer_name, note_text, submitted_at,
        )
        return {
            "decision_id": decision_id,
            "batch_id": batch_id,
            "pair_key": pair_key,
            "decision": decision,
            "reviewer": reviewer,
            "note": note,
            "submitted_at": submitted_at,
            "scope": SCOPE_BATCH,
            "effective": True,
        }

    fp_a, fp_b = row_fingerprint(first), row_fingerprint(second)
    with storage.transaction() as conn:
        previous = conn.execute(
            "SELECT * FROM cross_batch_decisions WHERE pair_key=? AND status='active'",
            (pair_key,),
        ).fetchone()
        # A savepoint lets us roll back only our own supersede+insert when a
        # concurrent transaction wins the partial-unique-index race, leaving
        # the winner row readable and the outer transaction committable.
        conn.execute("SAVEPOINT submit_cross_decision")
        conn.execute(
            "UPDATE cross_batch_decisions SET status='superseded' "
            "WHERE pair_key=? AND status='active'",
            (pair_key,),
        )
        try:
            cursor = conn.execute(
                """INSERT INTO cross_batch_decisions(source_batch_id,pair_key,
                     rec_a_key,rec_b_key,rec_a_fingerprint,rec_b_fingerprint,
                     decision,reviewer,note,submitted_at,status)
                   VALUES(?,?,?,?,?,?,?,?,?,?, 'active')""",
                (batch_id, pair_key, rec_a_key, rec_b_key, fp_a, fp_b, decision,
                 reviewer_name, note_text, submitted_at),
            )
        except (sqlite3.IntegrityError, sqlite3.OperationalError) as exc:
            # IntegrityError: a concurrent transaction won the partial unique
            # index race. OperationalError("database is locked"): another
            # process held the WAL write lock past busy_timeout. In both
            # cases roll back to the savepoint and surface the winning row.
            conn.execute("ROLLBACK TO SAVEPOINT submit_cross_decision")
            winner = conn.execute(
                "SELECT * FROM cross_batch_decisions WHERE pair_key=? "
                "AND status='active'",
                (pair_key,),
            ).fetchone()
            conn.execute("RELEASE SAVEPOINT submit_cross_decision")
            fallback = {
                "decision_id": None,
                "decision": "unknown",
                "source_batch_id": "unknown",
            }
            raise DecisionConflict(
                pair_key, dict(winner) if winner else fallback
            ) from exc
        conn.execute("RELEASE SAVEPOINT submit_cross_decision")
        decision_id = int(cursor.lastrowid)
        superseded = dict(previous) if previous is not None else None
    return {
        "decision_id": decision_id,
        "batch_id": batch_id,
        "pair_key": pair_key,
        "decision": decision,
        "reviewer": reviewer,
        "note": note,
        "submitted_at": submitted_at,
        "scope": SCOPE_CROSS_BATCH,
        "effective": True,
        "superseded_decision_id": None if superseded is None else superseded["decision_id"],
        "superseded_decision": None if superseded is None else superseded["decision"],
    }
