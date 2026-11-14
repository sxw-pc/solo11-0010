"""Batch-level, versioned view of the currently effective review decisions.

The append-only ``decisions`` table remains the audit source of truth and keeps
its historical pair_key semantics untouched. This module projects that history
into an immutable per-batch snapshot ("decision view"):

* every pair appears at most once in a given version (latest decision per pair
  wins by ``(submitted_at, decision_id)``), so concurrent reviewers can never
  leave two mutually exclusive same/different rows effective at once;
* each published version is never mutated, which lets parallel pipeline runs
  pin one version and observe byte-identical decision state;
* effective rows are re-aligned through the logical record keys
  (``source::record_id``) and their canonical pair_key is recomputed against
  the records currently stored for the batch, so a decision keeps working even
  when record primary keys drift after re-ingestion.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from .models import make_pair_key
from .storage import Storage


def effective_decisions(
    history: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse append-only history into one effective row per pair.

    Ordering matches ``clustering.resolve_decisions``: newest ``submitted_at``
    wins, with the auto-increment ``decision_id`` as a deterministic tiebreak.
    """
    latest: dict[str, dict[str, Any]] = {}
    for decision in sorted(
        history, key=lambda item: (item["submitted_at"], item["decision_id"])
    ):
        latest[decision["pair_key"]] = decision
    return [latest[pair_key] for pair_key in sorted(latest)]


def _record_index(storage: Storage, batch_id: str) -> dict[str, sqlite3.Row]:
    return {
        f"{row['source']}::{row['record_id']}": row
        for row in storage.list_records(batch_id)
    }


def build_view_rows(
    storage: Storage, batch_id: str, history: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Project history onto the batch's current records.

    Rows whose logical record keys no longer resolve are kept verbatim: they
    remain visible as audit state and are simply inert downstream, mirroring
    the pre-existing tolerance of the clustering layer.
    """
    records_by_key = _record_index(storage, batch_id)
    rows: list[dict[str, Any]] = []
    for decision in effective_decisions(history):
        rec_a_key, rec_b_key = decision["rec_a_key"], decision["rec_b_key"]
        if rec_a_key in records_by_key and rec_b_key in records_by_key:
            first, second, pair_key = make_pair_key(rec_a_key, rec_b_key)
            rec_a_key, rec_b_key = first, second
        else:
            pair_key = decision["pair_key"]
        rows.append({
            "pair_key": pair_key,
            "rec_a_key": rec_a_key,
            "rec_b_key": rec_b_key,
            "decision": decision["decision"],
            "decision_id": int(decision["decision_id"]),
            "reviewer": decision["reviewer"],
            "note": decision["note"],
            "submitted_at": decision["submitted_at"],
        })
    return rows


def ensure_view(storage: Storage, batch_id: str) -> int:
    """Return the current view version, materializing history on first use.

    Called lazily by every read path, so databases created before this feature
    upgrade automatically: historical decisions are projected into version 1
    without any manual re-review. A batch without any decision stays at
    version 0 and no snapshot rows are published, so the first real decision
    produces view version 1.
    """
    version = storage.current_decision_version(batch_id)
    if version is not None:
        return version
    history = [dict(row) for row in storage.decision_history(batch_id)]
    if not history:
        return 0
    view_version = 1
    storage.replace_decision_view(
        batch_id, view_version, build_view_rows(storage, batch_id, history)
    )
    return view_version


def load_view(storage: Storage, batch_id: str) -> tuple[int, list[dict[str, Any]]]:
    """Return ``(version, effective decision dicts)`` for the batch."""
    version = ensure_view(storage, batch_id)
    return version, [
        dict(row) for row in storage.list_decision_view(batch_id, version)
    ]


def pin_version(storage: Storage, batch_id: str) -> int:
    """Immutable read point for a pipeline run."""
    return ensure_view(storage, batch_id)


def view_at(storage: Storage, batch_id: str, view_version: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in storage.list_decision_view(batch_id, view_version)
    ]


def publish_after_decision(
    storage: Storage,
    batch_id: str,
    decision: dict[str, Any],
) -> tuple[int, int]:
    """Append a decision and atomically publish the next view version.

    Returns ``(decision_id, new_view_version)``. Version allocation and the
    write run under the storage's serialization lock, so two reviewers
    submitting the same pair concurrently each get a distinct append-only
    history row while exactly one of their verdicts survives in the newly
    published view — never two contradictory effective rows.
    """
    with storage.locked():
        current = storage.current_decision_version(batch_id)
        next_version = (current or 0) + 1

        def _build() -> list[dict[str, Any]]:
            history = [dict(row) for row in storage.decision_history(batch_id)]
            return build_view_rows(storage, batch_id, history)

        decision_id = storage.insert_decision_and_rebuild_view(
            batch_id, decision, next_version, _build
        )
    return decision_id, next_version
