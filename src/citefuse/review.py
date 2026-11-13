"""Human review queue and decision persistence.

Decisions are written to the append-only ``decisions`` audit table and, in the
same locked transaction, projected into the versioned ``decision_state`` view.
The view is what the review queue and pipeline runs read, so a submitted
decision is effective immediately without waiting for the next run.
"""
from __future__ import annotations

import json
from typing import Any

from .clock import Clock
from .models import record_from_db
from .reflow import is_pair_decided, load_snapshot
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

    Filtering reads the materialized, versioned decision view rather than
    recomputing a join against pairs, so a decision submitted moments ago takes
    effect offline and a drifting pair_key still matches on canonical keys.
    """
    run_id = storage.latest_run_id(batch_id)
    if not run_id:
        return []
    snapshot = load_snapshot(storage, batch_id)
    output: list[dict[str, Any]] = []
    for pair in storage.list_pairs(run_id, zone="review"):
        if is_pair_decided(snapshot, pair["pair_key"]):
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


def review_view_status(storage: Storage, batch_id: str) -> dict[str, Any]:
    """Public, versioned summary of the batch's instant-reflow decision view."""
    snapshot = load_snapshot(storage, batch_id)
    same = sum(1 for row in snapshot.rows if row["decision"] == "same")
    different = sum(1 for row in snapshot.rows if row["decision"] == "different")
    return {
        "batch_id": batch_id,
        "view_version": snapshot.version,
        "effective_decisions": len(snapshot.rows),
        "same": same,
        "different": different,
        "decisions": [
            {
                "pair_key": row["pair_key"],
                "rec_a_key": row["rec_a_key"],
                "rec_b_key": row["rec_b_key"],
                "decision": row["decision"],
                "reviewer": row["reviewer"],
                "note": row["note"],
                "decision_id": row["decision_id"],
                "submitted_at": row["submitted_at"],
                "view_version": row["view_version"],
            }
            for row in snapshot.rows
        ],
    }


def submit_decision(
    storage: Storage,
    clock: Clock,
    batch_id: str,
    pair_key: str,
    decision: str,
    reviewer: str,
    note: str,
) -> dict[str, Any]:
    """Validate a pair and atomically persist decision + effective view row.

    The history insert and the view upsert share one transaction, so readers
    never observe a decision that is not yet in the view. The view's primary
    key on the canonical record-key pair makes concurrent mutually exclusive
    submissions idempotent: exactly one effective row survives.
    """
    if decision not in {"same", "different"}:
        raise ValueError("decision must be 'same' or 'different'")
    if storage.get_batch(batch_id) is None:
        raise LookupError(f"batch {batch_id!r} not found")
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
    first, second = sorted(
        (rows[0], rows[1]),
        key=lambda row: f"{row['source']}::{row['record_id']}",
    )
    rec_a_key = f"{first['source']}::{first['record_id']}"
    rec_b_key = f"{second['source']}::{second['record_id']}"
    canonical_pair_key = f"{rec_a_key}||{rec_b_key}"
    submitted_at = clock.now_iso()
    clean_reviewer = reviewer.strip() or "anonymous"
    clean_note = note.strip()

    with storage.transaction() as conn:
        # Lazily materialize any pre-upgrade history before applying this write.
        existing_version = conn.execute(
            "SELECT MAX(view_version) AS v FROM decision_state WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if existing_version is None or existing_version["v"] is None:
            history = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM decisions WHERE batch_id=?"
                    " ORDER BY submitted_at,decision_id",
                    (batch_id,),
                ).fetchall()
            ]
            from .reflow import _history_to_state_rows

            storage.replace_decision_state(conn, batch_id, _history_to_state_rows(history))
            next_version = 2
        else:
            next_version = int(existing_version["v"]) + 1

        cursor = conn.execute(
            """INSERT INTO decisions(batch_id,pair_key,rec_a_key,rec_b_key,decision,
                 reviewer,note,submitted_at) VALUES(?,?,?,?,?,?,?,?)""",
            (batch_id, canonical_pair_key, rec_a_key, rec_b_key, decision,
             clean_reviewer, clean_note, submitted_at),
        )
        decision_id = int(cursor.lastrowid)

        previous = conn.execute(
            """SELECT decision FROM decision_state
               WHERE batch_id=? AND rec_a_key=? AND rec_b_key=?""",
            (batch_id, rec_a_key, rec_b_key),
        ).fetchone()
        storage.upsert_decision_state(
            conn, batch_id, canonical_pair_key, rec_a_key, rec_b_key, decision,
            clean_reviewer, clean_note, decision_id, submitted_at, next_version,
        )
        if previous is not None and previous["decision"] != decision:
            conn.execute(
                "INSERT INTO audit_events(run_id,batch_id,event_type,detail_json,"
                "created_at) VALUES(?,?,?,?,?)",
                (run_id, batch_id, "review_conflict",
                 json.dumps(
                     {
                         "type": "superseded_decision",
                         "pair_key": canonical_pair_key,
                         "older": previous["decision"],
                         "newer": decision,
                         "decision_id": decision_id,
                     },
                     sort_keys=True,
                     separators=(",", ":"),
                 ),
                 submitted_at),
            )
        view_version = next_version

    return {
        "decision_id": decision_id,
        "batch_id": batch_id,
        "pair_key": canonical_pair_key,
        "decision": decision,
        "reviewer": clean_reviewer,
        "note": clean_note,
        "submitted_at": submitted_at,
        "view_version": view_version,
    }
