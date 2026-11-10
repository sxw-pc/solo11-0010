"""Audit-event helpers layered on top of the run/attempt tables."""
from __future__ import annotations

import json
from typing import Any

from .clock import Clock
from .storage import Storage


def record_event(
    storage: Storage,
    clock: Clock,
    batch_id: str,
    run_id: str | None,
    event_type: str,
    detail: Any,
) -> None:
    storage.add_audit_event(batch_id, run_id, event_type, detail, clock.now_iso())


def run_events(storage: Storage, run_id: str) -> list[dict[str, Any]]:
    return [
        {
            "event_id": row["event_id"],
            "run_id": row["run_id"],
            "batch_id": row["batch_id"],
            "event_type": row["event_type"],
            "detail": json.loads(row["detail_json"]),
            "created_at": row["created_at"],
        }
        for row in storage.list_audit_events(run_id)
    ]


def run_statistics(run: dict[str, Any]) -> dict[str, Any]:
    """Stable subset of run counters suitable for reports and audit consumers."""
    return {
        "run_id": run["run_id"],
        "batch_id": run["batch_id"],
        "config_hash": run["config_hash"],
        "input_rows": run["input_rows"],
        "bad_rows": run["bad_rows"],
        "candidate_pairs": run["candidate_pairs"],
        "matched_edges": run["matched_edges"],
        "cluster_count": run["cluster_count"],
        "golden_count": run["golden_count"],
        "status": run["status"],
    }
