"""FastAPI route definitions.

Routes only parse inputs, call the domain layer, and serialize outputs; all
matching logic lives in the algorithm modules and is independent of HTTP.
"""
from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response

from .api_views import (
    batch_public, cluster_detail, cluster_summaries, export_golden, run_public,
)
from .audit import record_event, run_events
from .clock import Clock, SystemClock
from .config import AppConfig, PROJECT_ROOT, load_config
from .drift.guard import DriftGuard
from .drift.store import DriftStore
from .ingest import IngestError, ingest_jsonl
from .metrics import MetricsError, build_report, scan_thresholds
from .models import DecisionRequest, RunRequest, record_from_db
from .pipeline import run_pipeline
from .review import decision_dicts, next_review_pairs, submit_decision
from .storage import Storage


def _resolve_run(storage: Storage, run_id: str | None, batch_id: str | None) -> str:
    if run_id:
        if storage.get_run(run_id) is None:
            raise HTTPException(404, f"run {run_id!r} not found")
        return run_id
    if batch_id:
        latest = storage.latest_run_id(batch_id)
        if latest is None:
            raise HTTPException(404, f"batch {batch_id!r} has no pipeline run")
        return latest
    row = storage.query_one(
        "SELECT run_id FROM run_attempts ORDER BY attempt_id DESC LIMIT 1"
    )
    if row is None:
        raise HTTPException(404, "no pipeline runs exist")
    return str(row["run_id"])


def _pipeline_public(result: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "run_id", "batch_id", "config_hash", "input_rows", "bad_rows",
        "candidate_pairs", "matched_edges", "cluster_count", "golden_count",
        "timings", "blocking_stats", "drift_summary", "status", "conflicts",
    ]
    return {key: result.get(key) for key in keys}


def _parse_envelope(raw: bytes) -> tuple[bytes, str | None, str | None]:
    try:
        envelope = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(400, f"invalid JSON envelope: {exc}") from exc
    if not isinstance(envelope, dict) or not isinstance(envelope.get("jsonl"), str):
        raise HTTPException(400, "JSON body must be {'jsonl': '...'}")
    return (
        envelope["jsonl"].encode("utf-8"),
        envelope.get("batch_id"),
        envelope.get("name"),
    )


def create_app(
    config_path: str | Path | None = None,
    data_dir: str | Path | None = None,
    clock: Clock | None = None,
) -> FastAPI:
    config: AppConfig = load_config(config_path)
    root = Path(data_dir) if data_dir else Path(
        os.environ.get("CITEFUSE_DATA_DIR", str(PROJECT_ROOT / "data"))
    )
    reports_dir = root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    storage = Storage(root / "citefuse.sqlite3")
    active_clock = clock or SystemClock()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        storage.close()

    app = FastAPI(title="citefuse", version="0.1.0", lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "config_hash": config.fingerprint()}

    @app.post("/batches", status_code=201)
    async def create_batch(
        request: Request,
        batch_id: str | None = Query(default=None),
        name: str | None = Query(default=None),
    ) -> dict[str, Any]:
        raw = await request.body()
        if request.headers.get("content-type", "").startswith("application/json"):
            raw, envelope_id, envelope_name = _parse_envelope(raw)
            batch_id = batch_id or envelope_id
            name = name or envelope_name
        try:
            return ingest_jsonl(storage, config, active_clock, raw, batch_id, name)
        except IngestError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/batches")
    def list_batches() -> list[dict[str, Any]]:
        return [batch_public(storage, row) for row in storage.list_batches()]

    @app.get("/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict[str, Any]:
        row = storage.get_batch(batch_id)
        if row is None:
            raise HTTPException(404, f"batch {batch_id!r} not found")
        return batch_public(storage, row, include_dead=True)

    @app.delete("/batches/{batch_id}")
    def delete_batch(batch_id: str) -> dict[str, Any]:
        row = storage.get_batch(batch_id)
        if row is None:
            raise HTTPException(404, f"batch {batch_id!r} not found")
        run_ids = [run["run_id"] for run in storage.list_runs(batch_id)]
        if not storage.delete_batch(batch_id):
            raise HTTPException(404, f"batch {batch_id!r} not found")
        for run_id in run_ids:
            for suffix in (".json", "_thresholds.json"):
                (reports_dir / f"{run_id}{suffix}").unlink(missing_ok=True)
        return {"deleted": True, "batch_id": batch_id, "runs_removed": len(run_ids)}

    @app.post("/runs", status_code=201)
    def create_run(body: RunRequest) -> dict[str, Any]:
        try:
            result = run_pipeline(storage, config, active_clock, body.batch_id)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        return _pipeline_public(result)

    @app.get("/runs")
    def list_runs(batch_id: str | None = None) -> list[dict[str, Any]]:
        return [run_public(storage, row) for row in storage.list_runs(batch_id)]

    @app.get("/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        row = storage.get_run(run_id)
        if row is None:
            raise HTTPException(404, f"run {run_id!r} not found")
        return {**run_public(storage, row), "audit_events": run_events(storage, run_id)}

    @app.get("/review/next")
    def review_next(
        n: int | None = Query(default=None),
        batch_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        limit = n if n is not None else config.review.default_page_size
        if limit < 1 or limit > config.review.max_page_size:
            raise HTTPException(400, "n is outside the configured page-size limits")
        selected = batch_id
        if selected is None:
            row = storage.query_one(
                "SELECT batch_id FROM run_attempts ORDER BY attempt_id DESC LIMIT 1"
            )
            selected = row["batch_id"] if row else None
        if selected is None:
            return {"batch_id": None, "pairs": []}
        if storage.get_batch(selected) is None:
            raise HTTPException(404, f"batch {selected!r} not found")
        return {"batch_id": selected, "pairs": next_review_pairs(storage, selected, limit)}

    @app.post("/review/decisions", status_code=201)
    def review_decision(body: DecisionRequest) -> dict[str, Any]:
        try:
            decision = submit_decision(
                storage, active_clock, body.batch_id, body.pair_key,
                body.decision, body.reviewer, body.note,
            )
            rebuilt = run_pipeline(storage, config, active_clock, body.batch_id)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return {"decision": decision, "run": _pipeline_public(rebuilt)}

    @app.get("/review/decisions")
    def list_decisions(batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is None:
            raise HTTPException(400, "batch_id is required")
        if storage.get_batch(batch_id) is None:
            raise HTTPException(404, f"batch {batch_id!r} not found")
        return [dict(row) for row in decision_dicts(storage, batch_id)]

    @app.get("/metrics/report")
    def metrics_report(run_id: str) -> dict[str, Any]:
        try:
            return build_report(storage, config, reports_dir, run_id)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except MetricsError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/metrics/thresholds")
    def metrics_thresholds(
        run_id: str,
        grid: str = Query(default="0.50,0.60,0.70,0.75,0.80,0.85,0.90"),
    ) -> dict[str, Any]:
        try:
            thresholds = [float(value) for value in grid.split(",") if value.strip()]
        except ValueError as exc:
            raise HTTPException(400, "grid must be comma-separated numbers") from exc
        try:
            return scan_thresholds(storage, config, reports_dir, run_id, thresholds)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except MetricsError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/clusters")
    def list_clusters(
        run_id: str | None = None, batch_id: str | None = None
    ) -> dict[str, Any]:
        resolved = _resolve_run(storage, run_id, batch_id)
        return {"run_id": resolved, "clusters": cluster_summaries(storage, resolved)}

    @app.get("/clusters/{cluster_id}")
    def get_cluster(
        cluster_id: str, run_id: str | None = None, batch_id: str | None = None
    ) -> dict[str, Any]:
        resolved = _resolve_run(storage, run_id, batch_id)
        try:
            return cluster_detail(storage, resolved, cluster_id)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/golden/export")
    def golden_export(
        format: str = Query(default="jsonl"),
        run_id: str | None = None,
        batch_id: str | None = None,
    ) -> Response:
        resolved = _resolve_run(storage, run_id, batch_id)
        try:
            body, media_type, filename = export_golden(storage, resolved, format)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return Response(
            content=body,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # -- semantic drift guard -------------------------------------------------
    @app.get("/drift/quarantine")
    def drift_quarantine(batch_id: str | None = None) -> dict[str, Any]:
        drift_store = DriftStore(storage)
        items = drift_store.list_quarantine(batch_id)
        return {"count": len(items), "items": items}

    @app.get("/drift/dispositions")
    def drift_dispositions(
        batch_id: str,
        tier: str | None = Query(default=None),
        n: int = Query(default=100, ge=1, le=5000),
    ) -> dict[str, Any]:
        if storage.get_batch(batch_id) is None:
            raise HTTPException(404, f"batch {batch_id!r} not found")
        if tier is not None and tier not in ("clean", "minor", "medium", "heavy"):
            raise HTTPException(400, "tier must be one of clean/minor/medium/heavy")
        drift_store = DriftStore(storage)
        items = drift_store.list_dispositions(batch_id, tier, n)
        return {"batch_id": batch_id, "count": len(items), "items": items}

    @app.get("/drift/summary")
    def drift_summary(
        run_id: str | None = None, batch_id: str | None = None
    ) -> dict[str, Any]:
        resolved = _resolve_run(storage, run_id, batch_id)
        drift_store = DriftStore(storage)
        payload = drift_store.get_drift_run(resolved)
        if payload is None:
            raise HTTPException(404, f"no drift summary for run {resolved!r}")
        return payload

    @app.get("/drift/baseline")
    def drift_baseline() -> dict[str, Any]:
        drift_store = DriftStore(storage)
        versions = drift_store.list_baseline_versions()
        latest = drift_store.load_latest_baseline()
        return {
            "latest_version": latest.version if latest else 0,
            "versions": versions,
            "fields": sorted(latest.stats) if latest else [],
        }

    @app.post("/drift/baseline/rebuild", status_code=201)
    def drift_baseline_rebuild(body: RunRequest) -> dict[str, Any]:
        if storage.get_batch(body.batch_id) is None:
            raise HTTPException(404, f"batch {body.batch_id!r} not found")
        drift_store = DriftStore(storage)
        drift_store.reset_baseline()
        guard = DriftGuard(storage, config, active_clock)
        records = [record_from_db(row) for row in storage.list_records(body.batch_id)]
        version = guard.bootstrap_from_records(records, source_batch_id=body.batch_id)
        record_event(
            storage, active_clock, body.batch_id, None, "drift_baseline_rebuilt",
            {"batch_id": body.batch_id, "records": len(records), "version": version},
        )
        return {"baseline_version": version, "absorbed_records": len(records)}

    return app
