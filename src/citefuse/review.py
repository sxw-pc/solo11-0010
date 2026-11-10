"""Human review queue and decision persistence."""
from __future__ import annotations

import json
from typing import Any

from .clock import Clock
from .models import record_from_db
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
    """Return unresolved gray-zone pairs from the batch's latest pipeline run."""
    run_id = storage.latest_run_id(batch_id)
    if not run_id:
        return []
    latest = latest_decision_map(decision_dicts(storage, batch_id))
    output: list[dict[str, Any]] = []
    for pair in storage.list_pairs(run_id, zone="review"):
        if pair["pair_key"] in latest:
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
    """Validate a pair against the latest run and persist a review decision."""
    if decision not in {"same", "different"}:
        raise ValueError("decision must be 'same' or 'different'")
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
    first, second = sorted((rows[0], rows[1]), key=lambda row: f"{row['source']}::{row['record_id']}")
    submitted_at = clock.now_iso()
    decision_id = storage.add_decision(
        batch_id, pair_key, f"{first['source']}::{first['record_id']}",
        f"{second['source']}::{second['record_id']}", decision, reviewer.strip() or "anonymous",
        note.strip(), submitted_at,
    )
    return {
        "decision_id": decision_id,
        "batch_id": batch_id,
        "pair_key": pair_key,
        "decision": decision,
        "reviewer": reviewer,
        "note": note,
        "submitted_at": submitted_at,
    }
