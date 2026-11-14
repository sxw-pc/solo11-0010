"""Human review queue and decision persistence.

The review queue is filtered offline against the batch's versioned decision
view (see :mod:`citefuse.decision_view`): the instant a decision is committed
the next ``/review/next`` call stops returning the pair, without waiting for a
pipeline rerun. View versions are immutable, so the filter never drifts with
pairs-table rebuilds.
"""
from __future__ import annotations

import json
from typing import Any

from .clock import Clock
from .decision_view import load_view, publish_after_decision
from .models import KEY_SEPARATOR, PAIR_SEPARATOR, record_from_db
from .storage import Storage


def decision_dicts(storage: Storage, batch_id: str) -> list[dict[str, Any]]:
    return [dict(row) for row in storage.decision_history(batch_id)]


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


def next_review_pairs(
    storage: Storage, batch_id: str, limit: int
) -> list[dict[str, Any]]:
    """Return unresolved gray-zone pairs from the batch's latest pipeline run.

    Filtering uses the current decision view rather than a live join against
    the decisions/pairs tables, so the response is stable for the lifetime of a
    view version even while the pairs table is being rebuilt.
    """
    run_id = storage.latest_run_id(batch_id)
    if not run_id:
        return []
    _, view_rows = load_view(storage, batch_id)
    decided_keys = {row["pair_key"] for row in view_rows}
    output: list[dict[str, Any]] = []
    for pair in storage.list_pairs(run_id, zone="review"):
        if pair["pair_key"] in decided_keys:
            continue
        records = _records_for_pair(storage, pair["rec_a_pk"], pair["rec_b_pk"])
        output.append({
            "pair_key": pair["pair_key"],
            "score": pair["score"],
            "zone": pair["zone"],
            "strategies": json.loads(pair["strategies_json"]),
            "details": json.loads(pair["details_json"]),
            "records": [record.public() for record in records],
        })
        if len(output) >= limit:
            break
    return output


def submit_decision(
    storage: Storage,
    clock: Clock,
    batch_id: str,
    pair_key: str,
    decision: str,
    reviewer: str,
    note: str,
) -> dict[str, Any]:
    """Validate a pair against the latest run and persist a review decision.

    The decision is appended to the immutable history and a new versioned view
    is published in the same transaction; no pipeline rerun is triggered, so
    the call returns in constant-ish time regardless of batch size.
    """
    if decision not in {"same", "different"}:
        raise ValueError("decision must be 'same' or 'different'")
    run_id = storage.latest_run_id(batch_id)
    if not run_id:
        raise LookupError(f"batch {batch_id!r} has no pipeline run")
    pair = storage.get_pair(run_id, pair_key)
    if pair is not None:
        rows = storage.query(
            "SELECT * FROM records WHERE record_pk IN (?,?) ORDER BY source,record_id",
            (pair["rec_a_pk"], pair["rec_b_pk"]),
        )
    else:
        # The pair may have left the latest run's pairs table because instant
        # reflux already excluded a prior decision on it (different pairs are
        # dropped from the candidate pool before scoring). Re-review must still
        # work: resolve the pair through its logical record keys, which also
        # survives record primary-key drift after re-ingestion. Pairs that were
        # never candidates (and never reviewed) keep failing at get_pair above.
        if pair_key.count(PAIR_SEPARATOR) != 1 or any(
            KEY_SEPARATOR not in side for side in pair_key.split(PAIR_SEPARATOR)
        ):
            raise LookupError(
                f"pair {pair_key!r} is not a candidate in the latest run"
            )
        keys = pair_key.split(PAIR_SEPARATOR)
        placeholders = ",".join(("?",) * len(keys))
        rows = storage.query(
            f"SELECT * FROM records WHERE batch_id=? AND "
            f"(source||'{KEY_SEPARATOR}'||record_id) IN ({placeholders})"
            " ORDER BY source,record_id",
            (batch_id, *keys),
        )
        found = {f"{row['source']}::{row['record_id']}" for row in rows}
        if found != set(keys):
            raise LookupError(
                f"pair {pair_key!r} is not a candidate in the latest run"
            )
    if len(rows) != 2:
        raise LookupError("pair references missing records")
    first, second = sorted((rows[0], rows[1]), key=lambda row: f"{row['source']}::{row['record_id']}")
    submitted_at = clock.now_iso()
    reviewer_name = reviewer.strip() or "anonymous"
    note_text = note.strip()
    decision_row = {
        "pair_key": pair_key,
        "rec_a_key": f"{first['source']}::{first['record_id']}",
        "rec_b_key": f"{second['source']}::{second['record_id']}",
        "decision": decision,
        "reviewer": reviewer_name,
        "note": note_text,
        "submitted_at": submitted_at,
    }
    decision_id, view_version = publish_after_decision(storage, batch_id, decision_row)
    return {
        "decision_id": decision_id,
        "batch_id": batch_id,
        "pair_key": pair_key,
        "decision": decision,
        "reviewer": reviewer_name,
        "note": note_text,
        "submitted_at": submitted_at,
        "view_version": view_version,
    }
