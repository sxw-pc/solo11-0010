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
from .models import Record, record_from_db
from .reflow import load_snapshot, project_reflow, snapshot_fingerprint
from .review import decision_dicts
from .scoring import score_pair
from .storage import Storage
from .survivorship import build_golden


def _run_id(
    batch: Any,
    config: AppConfig,
    decisions: list[dict[str, Any]],
    view_fingerprint: str | None = None,
) -> str:
    # The view fingerprint is identity-based (canonical record-key pairs + view
    # version), so run_id no longer changes when a drifting pair_key is rewritten.
    # The decision history fingerprint is retained for backward traceability.
    decision_fp = json.dumps(
        [
            [item["pair_key"], item["decision"], item["submitted_at"], item["decision_id"]]
            for item in decisions
        ],
        separators=(",", ":"),
    )
    material = "|".join(
        [
            batch["batch_id"],
            batch["input_hash"],
            config.fingerprint(),
            decision_fp,
            view_fingerprint or "",
        ]
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
    """
    batch = storage.get_batch(batch_id)
    if batch is None:
        raise LookupError(f"batch {batch_id!r} not found")
    records = _load_records(storage, batch_id)
    history = decision_dicts(storage, batch_id)
    # Pin one immutable, versioned decision snapshot for the whole attempt. Two
    # concurrent runs of the same batch therefore see identical decision state:
    # the storage lock serializes snapshot loading and both pin the same version.
    snapshot = load_snapshot(storage, batch_id)
    view_fingerprint = snapshot_fingerprint(snapshot)
    run_id = _run_id(batch, config, history, view_fingerprint)
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

        scored_pairs: list[dict[str, Any]] = []
        for pair in candidates:
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

        # Instant reflow: effective decisions override candidate classification
        # before clustering. 'different' pairs leave the candidate pool; 'same'
        # pairs are forced into the edge set. The offline /review view projects
        # exactly this function, so queue filtering and the next run agree.
        reflow = project_reflow(records, scored_pairs, snapshot)
        excluded_keys = set(reflow.excluded_pair_keys)
        persisted_pairs = [
            {**pair, "excluded_by_decision": pair["pair_key"] in excluded_keys}
            for pair in scored_pairs
        ]
        effective_pairs = [
            pair for pair in scored_pairs if pair["pair_key"] not in excluded_keys
        ]
        model_edges = [
            {
                "pair_key": item["pair_key"],
                "rec_a_pk": item["rec_a_pk"],
                "rec_b_pk": item["rec_b_pk"],
                "score": item["score"],
                "details": item["details"],
            }
            for item in effective_pairs
            if item["zone"] == "match"
        ]
        groups, applied_edges, conflicts = cluster_records(records, model_edges, history)
        pair_scores = {item["pair_key"]: item["score"] for item in scored_pairs}
        for edge in applied_edges:
            if edge["kind"] == "human_same" and edge["pair_key"] in pair_scores:
                edge["score"] = pair_scores[edge["pair_key"]]
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

        run_common = {
            "run_id": run_id,
            "batch_id": batch_id,
            "config_hash": config.fingerprint(),
            "input_rows": int(batch["total_lines"]),
            "bad_rows": int(batch["bad_lines"]),
            # candidate_pairs is the post-reflow pool: human 'different' pairs
            # have been removed; human 'same' pairs remain counted as candidates
            # but enter the edge set regardless of their model score.
            "candidate_pairs": len(effective_pairs),
            "matched_edges": len(applied_edges),
            "cluster_count": len(cluster_rows),
            "golden_count": len(cluster_rows),
            "timings": timings,
            "blocking_stats": blocking_stats,
            "status": "completed",
            "decision_view_version": snapshot.version,
            "created_at": started_at,
        }
        storage.save_run(run_common)
        storage.replace_pairs(run_id, persisted_pairs)
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
            {"candidate_pairs": len(effective_pairs), "clusters": len(cluster_rows),
             "conflicts": len(conflicts),
             "decision_view_version": snapshot.version},
        )
    except Exception as exc:
        failed = {
            "run_id": run_id, "batch_id": batch_id, "config_hash": config.fingerprint(),
            "input_rows": int(batch["total_lines"]), "bad_rows": int(batch["bad_lines"]),
            "candidate_pairs": 0, "matched_edges": 0, "cluster_count": 0,
            "golden_count": 0, "timings": timings, "blocking_stats": {},
            "status": "failed", "decision_view_version": snapshot.version,
            "created_at": started_at,
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
