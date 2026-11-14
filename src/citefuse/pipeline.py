"""End-to-end orchestration: candidates -> scores -> clusters -> golden."""
from __future__ import annotations

import hashlib
import json
from typing import Any

from .audit import record_event
from .blocking import generate_candidates
from .clock import Clock
from .clustering import cluster_records
from .config import AppConfig
from .decision_view import pin_version, view_at
from .models import Record, record_from_db
from .review import decision_dicts
from .scoring import score_pair
from .storage import Storage
from .survivorship import build_golden


def _run_id(batch: Any, config: AppConfig, decisions: list[dict[str, Any]]) -> str:
    decision_fp = json.dumps(
        [
            [item["pair_key"], item["decision"], item["submitted_at"], item["decision_id"]]
            for item in decisions
        ],
        separators=(",", ":"),
    )
    material = "|".join(
        [batch["batch_id"], batch["input_hash"], config.fingerprint(), decision_fp]
    )
    return "run_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _cluster_id(batch_id: str, member_keys: list[str]) -> str:
    material = batch_id + "|" + "|".join(member_keys)
    return "cl_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _load_records(storage: Storage, batch_id: str) -> list[Record]:
    return [record_from_db(row) for row in storage.list_records(batch_id)]


def run_pipeline(
    storage: Storage, config: AppConfig, clock: Clock, batch_id: str
) -> dict[str, Any]:
    """Run one deterministic pipeline attempt and persist a reproducible run.

    Identical batch + config + decisions produce the same run_id and identical
    cluster/golden rows. Each invocation still appends a run_attempts audit row.

    The run pins an immutable decision-view version at entry. Instant human
    reflux is applied before scoring: candidates already judged ``different``
    leave the candidate pool entirely, while candidates judged ``same`` bypass
    scoring and go straight into the human edge set. Parallel runs started at
    the same view version therefore observe identical decision state.
    """
    batch = storage.get_batch(batch_id)
    if batch is None:
        raise LookupError(f"batch {batch_id!r} not found")
    records = _load_records(storage, batch_id)
    history = decision_dicts(storage, batch_id)
    view_version = pin_version(storage, batch_id)
    view_rows = view_at(storage, batch_id, view_version)
    view_by_key = {row["pair_key"]: row for row in view_rows}
    run_id = _run_id(batch, config, history)
    started_at = clock.now_iso()
    timings: dict[str, float] = {}
    last = start = clock.monotonic()

    def mark(stage: str) -> None:
        nonlocal last
        now = clock.monotonic()
        timings[stage] = round(now - last, 6)
        last = now

    try:
        candidates, blocking_stats = generate_candidates(records, config)
        mark("blocking")

        # Instant reflux at the candidate-pair boundary, before any scoring.
        # Forced-same pairs skip the scored candidate pool but still receive an
        # audit-only score below; the human verdict mandates the edge regardless
        # of thresholds, so the score never decides anything.
        forced_same: list[dict[str, Any]] = []
        reflux_different: list[str] = []
        open_candidates = []
        for pair in candidates:
            verdict = view_by_key.get(pair.pair_key)
            if verdict is None:
                open_candidates.append(pair)
            elif verdict["decision"] == "same":
                forced_same.append(pair)
            else:
                reflux_different.append(pair.pair_key)

        scored_pairs: list[dict[str, Any]] = []
        forced_scores: dict[str, float] = {}
        for pair in forced_same:
            forced_scores[pair.pair_key] = score_pair(
                pair.left, pair.right, config
            )["score"]
        for pair in open_candidates:
            result = score_pair(pair.left, pair.right, config)
            scored_pairs.append({
                "pair_key": pair.pair_key,
                "rec_a_pk": pair.left.pk,
                "rec_b_pk": pair.right.pk,
                "score": result["score"],
                "zone": result["zone"],
                "strategies": list(pair.strategies),
                "details": result,
            })
        mark("scoring")

        model_edges = [
            {
                "pair_key": item["pair_key"],
                "rec_a_pk": item["rec_a_pk"],
                "rec_b_pk": item["rec_b_pk"],
                "score": item["score"],
                "details": item["details"],
            }
            for item in scored_pairs
            if item["zone"] == "match"
        ]
        groups, applied_edges, conflicts = cluster_records(
            records, model_edges, history
        )
        pair_scores = {item["pair_key"]: item["score"] for item in scored_pairs}
        for edge in applied_edges:
            if edge["kind"] != "human_same":
                continue
            if edge["pair_key"] in pair_scores:
                edge["score"] = pair_scores[edge["pair_key"]]
            elif edge["pair_key"] in forced_scores:
                edge["score"] = forced_scores[edge["pair_key"]]
        mark("clustering")

        cluster_rows: list[dict[str, Any]] = []
        for group in groups:
            member_keys = [record.key for record in group]
            cluster_id = _cluster_id(batch_id, member_keys)
            cluster_rows.append({
                "cluster_id": cluster_id,
                "member_keys": member_keys,
                "member_pks": [record.pk for record in group],
                "golden": build_golden(group, config),
            })
        mark("golden")

        reflux_stats = {
            "decision_view_version": view_version,
            "effective_decisions": len(view_rows),
            "forced_same_edges": len(forced_same),
            "excluded_different_pairs": len(reflux_different),
            "excluded_pair_keys": reflux_different,
        }
        blocking_stats = {**blocking_stats, "decision_reflux": reflux_stats}

        run_common = {
            "run_id": run_id,
            "batch_id": batch_id,
            "config_hash": config.fingerprint(),
            "input_rows": int(batch["total_lines"]),
            "bad_rows": int(batch["bad_lines"]),
            "candidate_pairs": len(scored_pairs),
            "matched_edges": len(applied_edges),
            "cluster_count": len(cluster_rows),
            "golden_count": len(cluster_rows),
            "timings": timings,
            "blocking_stats": blocking_stats,
            "status": "completed",
            "created_at": started_at,
        }
        storage.save_run(run_common)
        storage.replace_pairs(run_id, scored_pairs)
        storage.replace_edges(run_id, applied_edges)
        storage.replace_clusters(run_id, batch_id, cluster_rows)
        mark("persistence")
        timings["total"] = round(clock.monotonic() - start, 6)
        storage.save_run(run_common)
        for conflict in conflicts:
            record_event(storage, clock, batch_id, run_id, "review_conflict", conflict)
        storage.add_attempt({
            **run_common,
            "started_at": started_at,
            "finished_at": clock.now_iso(),
        })
        record_event(
            storage, clock, batch_id, run_id, "run_completed",
            {"candidate_pairs": len(scored_pairs), "clusters": len(cluster_rows),
             "conflicts": len(conflicts), "decision_view_version": view_version},
        )
    except Exception as exc:
        failed = {
            "run_id": run_id, "batch_id": batch_id, "config_hash": config.fingerprint(),
            "input_rows": int(batch["total_lines"]), "bad_rows": int(batch["bad_lines"]),
            "candidate_pairs": 0, "matched_edges": 0, "cluster_count": 0,
            "golden_count": 0, "timings": timings, "blocking_stats": {},
            "status": "failed", "created_at": started_at,
        }
        storage.save_run(failed)
        storage.add_attempt({**failed, "started_at": started_at, "finished_at": clock.now_iso()})
        record_event(storage, clock, batch_id, run_id, "run_failed", {"error": str(exc)})
        raise

    return {
        **run_common,
        "timings": timings,
        "blocking_stats": blocking_stats,
        "conflicts": conflicts,
        "decision_view_version": view_version,
        "reflux": reflux_stats,
        "clusters": [
            {"cluster_id": row["cluster_id"], "member_keys": row["member_keys"],
             "golden": row["golden"]}
            for row in cluster_rows
        ],
    }


def get_run_summary(storage: Storage, run_id: str) -> dict[str, Any]:
    """API-facing run snapshot including the latest attempt's timings."""
    run = storage.get_run(run_id)
    if run is None:
        raise LookupError(f"run {run_id!r} not found")
    attempt = storage.latest_attempt(run_id)
    result = dict(run)
    for key in ("timings_json", "blocking_stats_json"):
        result[key.replace("_json", "")] = json.loads(result.pop(key))
    result["attempt_count"] = storage.query_one(
        "SELECT COUNT(*) AS n FROM run_attempts WHERE run_id=?", (run_id,)
    )["n"]
    if attempt is not None:
        result["last_attempt"] = {
            "started_at": attempt["started_at"],
            "finished_at": attempt["finished_at"],
            "timings": json.loads(attempt["timings_json"]),
            "status": attempt["status"],
        }
    return result
