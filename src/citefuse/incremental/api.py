"""HTTP routes for the incremental fusion module.

Kept separate from the full-run routes in ``api.py`` so the existing API
surface stays untouched. The routes only validate input and delegate to
:class:`~citefuse.incremental.engine.IncrementalEngine`; all domain logic
remains transport-independent.
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ..clock import Clock
from ..config import AppConfig
from . import IncrementalBusy, IncrementalEngine, IncrementalError
from ..storage import Storage


class IncrementalRunRequest(BaseModel):
    batch_id: str = Field(min_length=1, max_length=64)
    base_run_id: str | None = Field(default=None, max_length=80)


def _task_public(row: Any) -> dict[str, Any]:
    import json

    return {
        "task_id": row["task_id"],
        "batch_id": row["batch_id"],
        "base_run_id": row["base_run_id"],
        "run_id": row["run_id"],
        "config_hash": row["config_hash"],
        "status": row["status"],
        "new_records": row["new_records"],
        "new_pairs": row["new_pairs"],
        "new_model_edges": row["new_model_edges"],
        "reused_pairs": row["reused_pairs"],
        "reused_clusters": row["reused_clusters"],
        "rebuilt_clusters": row["rebuilt_clusters"],
        "idempotent_replay": bool(row["idempotent_replay"]),
        "timings": json.loads(row["timings_json"]),
        "blocking_stats": json.loads(row["blocking_stats_json"]),
        "error": row["error"],
        "created_at": row["created_at"],
        "finished_at": row["finished_at"],
    }


def register_incremental_routes(
    app: FastAPI,
    storage: Storage,
    config: AppConfig,
    clock: Clock,
) -> None:
    engine = IncrementalEngine(storage, config, clock)

    @app.post("/incremental/runs", status_code=201)
    def create_incremental_run(body: IncrementalRunRequest) -> dict[str, Any]:
        try:
            return engine.run_incremental(body.batch_id, body.base_run_id)
        except IncrementalBusy as exc:
            raise HTTPException(409, str(exc)) from exc
        except IncrementalError as exc:
            # Missing baseline/run -> 404; precondition/validation -> 400.
            message = str(exc)
            if "not found" in message:
                raise HTTPException(404, message) from exc
            raise HTTPException(400, message) from exc

    @app.get("/incremental/tasks")
    def list_incremental_tasks(
        batch_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return [
            _task_public(row) for row in engine.tasks.list_tasks(batch_id)
        ]

    @app.get("/incremental/tasks/{task_id}")
    def get_incremental_task(task_id: str) -> dict[str, Any]:
        row = engine.tasks.get_task(task_id)
        if row is None:
            raise HTTPException(404, f"incremental task {task_id!r} not found")
        return _task_public(row)
