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
from .review import (
    INHERITED_FLAG,
    SOURCE_BATCH_KEY,
    SOURCE_DECISION_KEY,
    effective_decision_history,
    own_cross_identity_mismatches,
    resolve_inheritances,
)
from .scoring import score_pair
from .storage import Storage
from .survivorship import build_golden


def _decision_fp(decisions: list[dict[str, Any]]) -> str:
    return json.dumps(
        [
            [
                item["pair_key"], item["decision"], item["submitted_at"],
                item["decision_id"],
                bool(item.get(INHERITED_FLAG, False)),
                item.get(SOURCE_BATCH_KEY, item.get("batch_id")),
            ]
            for item in decisions
        ],
        separators=(",", ":"),
    )


def _run_id(
    batch: Any,
    config: AppConfig,
    decisions: list[dict[str, Any]],
    inheritances: list[dict[str, Any]],
) -> str:
    # Only immutable fields enter the fingerprint. The frozen inheritance
    # snapshot is part of the material, so a batch rerun keeps the same
    # run_id (and same clusters/golden) even if global cross-batch decisions
    # change afterward.
    inheritance_fp = json.dumps(
        [
            [
                pin["pair_key"], pin["decision"], pin["submitted_at"],
                pin["source_decision_id"], pin["source_batch_id"],
            ]
            for pin in inheritances
        ],
        separators=(",", ":"),
    )
    material = "|".join(
        [
            batch["batch_id"], batch["input_hash"], config.fingerprint(),
            _decision_fp(decisions), inheritance_fp,
        ]
    )
    return "run_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _cluster_id(batch_id: str, member_keys: list[str]) -> str:
    material = batch_id + "|" + "|".join(member_keys)
    return "cl_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _load_records(storage: Storage, batch_id: str) -> list[Record]:
    return [record_from_db(row) for row in storage.list_records(batch_id)]


def _resolve_and_freeze(
    storage: Storage,
    clock: Clock,
    batch_id: str,
    candidate_pairs: list[dict[str, Any]],
    inherited_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Match active cross-batch decisions and freeze this batch's snapshot."""
    pins, mismatches, _history_hits = resolve_inheritances(
        storage, batch_id, candidate_pairs
    )
    storage.replace_inheritances(batch_id, inherited_at, pins)
    return pins, mismatches


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
    started_at = clock.now_iso()
    timings: dict[str, float] = {}
    last = start = clock.monotonic()

    def mark(stage: str) -> None:
        nonlocal last
        now = clock.monotonic()
        timings[stage] = round(now - last, 6)
        last = now

    # A preliminary run_id keeps failure rows addressable even when blocking
    # raises before the inheritance snapshot can be rebuilt.
    pre_history, _ = effective_decision_history(storage, batch_id)
    pre_inheritances = [dict(row) for row in storage.list_inheritances(batch_id)]
    run_id = _run_id(batch, config, pre_history, pre_inheritances)

    try:
        candidates, blocking_stats = generate_candidates(records, config)
        mark("blocking")
        # Inheritance is frozen at most once per batch lineage. A batch that
        # already has pipeline runs reuses its frozen snapshot verbatim (an
        # empty snapshot means no cross decision existed at first-run time and
        # must stay empty), so later global submissions can never rewrite an
        # older batch's reproducible pairs/clusters/golden. Only a batch with
        # no runs yet (or a rebuilt batch whose runs were cascade-deleted)
        # resolves inheritance against the current global state.
        # Gate on a *completed* run: a failed first attempt persists run
        # bookkeeping but never reaches the freeze below, so a retry must
        # still resolve inheritance rather than inherit an empty snapshot.
        already_ran = storage.latest_completed_run_id(batch_id) is not None
        candidate_pairs = [
            {
                "pair_key": pair.pair_key,
                "rec_a_pk": pair.left.pk,
                "rec_b_pk": pair.right.pk,
            }
            for pair in candidates
        ]
        # Own-cross identity is re-validated every run, but its mismatch
        # audit is only emitted on a fresh freeze so reruns stay reproducible.
        own_mismatches_all = own_cross_identity_mismatches(
            storage, batch_id, candidate_pairs
        )
        if already_ran:
            # Reruns reuse the frozen snapshot verbatim and never re-resolve
            # global state, so no new inheritance audit events are produced.
            pins: list[dict[str, Any]] = [
                dict(row) for row in storage.list_inheritances(batch_id)
            ]
            mismatches: list[dict[str, Any]] = []
        else:
            pins, resolved_mismatches = _resolve_and_freeze(
                storage, clock, batch_id, candidate_pairs, started_at
            )
            mismatches = resolved_mismatches + own_mismatches_all
        inheritances = storage.list_inheritances(batch_id)
        history, dropped_frozen = effective_decision_history(
            storage, batch_id, candidate_pairs
        )
        if dropped_frozen:
            # Frozen pins whose records drifted are excluded from clustering.
            mismatches = mismatches + [
                {
                    "pair_key": item["pin"]["pair_key"],
                    "reason": item["reason"],
                    "cross_decision": item["pin"],
                }
                for item in dropped_frozen
            ]
        run_id = _run_id(batch, config, history, [dict(row) for row in inheritances])

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
        groups, applied_edges, conflicts = cluster_records(records, model_edges, history)
        pair_scores = {item["pair_key"]: item["score"] for item in scored_pairs}
        inherited_by_key = {
            row["pair_key"]: dict(row) for row in storage.list_inheritances(batch_id)
        }
        for edge in applied_edges:
            if edge["kind"] == "human_same" and edge["pair_key"] in pair_scores:
                edge["score"] = pair_scores[edge["pair_key"]]
            pin = inherited_by_key.get(edge["pair_key"])
            if pin is not None and edge["kind"] == "human_same":
                edge["details"].update({
                    INHERITED_FLAG: True,
                    SOURCE_BATCH_KEY: pin["source_batch_id"],
                    SOURCE_DECISION_KEY: pin["source_decision_id"],
                })
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
            "candidate_pairs": len(scored_pairs),
            "matched_edges": len(applied_edges),
            "cluster_count": len(cluster_rows),
            "golden_count": len(cluster_rows),
            "timings": timings,
            "blocking_stats": blocking_stats,
            "status": "completed",
            "created_at": started_at,
            "inheritances": [
                {
                    "pair_key": row["pair_key"],
                    "decision": row["decision"],
                    "source_batch_id": row["source_batch_id"],
                    "source_decision_id": row["source_decision_id"],
                    "submitted_at": row["submitted_at"],
                }
                for row in inheritances
            ],
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
        if not already_ran:
            # Inheritance/mismatch audit events describe the one-time freeze;
            # reruns reuse the snapshot and must not duplicate them.
            for pin in pins:
                record_event(
                    storage, clock, batch_id, run_id, "decision_inherited",
                    {
                        "pair_key": pin["pair_key"],
                        "decision": pin["decision"],
                        "source_batch_id": pin["source_batch_id"],
                        "source_decision_id": pin["source_decision_id"],
                    },
                )
            for mismatch in mismatches:
                record_event(
                    storage, clock, batch_id, run_id, "inheritance_identity_mismatch",
                    {
                        "pair_key": mismatch["pair_key"],
                        "reason": mismatch["reason"],
                        "source_batch_id": mismatch["cross_decision"]["source_batch_id"],
                        "source_decision_id": mismatch["cross_decision"]["decision_id"],
                    },
                )
        storage.add_attempt({
            **{key: value for key, value in run_common.items()
               if key != "inheritances"},
            "started_at": started_at,
            "finished_at": clock.now_iso(),
        })
        record_event(
            storage, clock, batch_id, run_id, "run_completed",
            {"candidate_pairs": len(scored_pairs), "clusters": len(cluster_rows),
             "conflicts": len(conflicts), "inherited_decisions": len(pins),
             "inheritance_mismatches": len(mismatches)},
        )
    except Exception as exc:
        failed = {
            "run_id": run_id, "batch_id": batch_id, "config_hash": config.fingerprint(),
            "input_rows": int(batch["total_lines"]), "bad_rows": int(batch["bad_lines"]),
            "candidate_pairs": 0, "matched_edges": 0, "cluster_count": 0,
            "golden_count": 0, "timings": timings, "blocking_stats": {},
            "status": "failed", "created_at": started_at,
            "inheritances": [],
        }
        storage.save_run(failed)
        storage.add_attempt({
            **{key: value for key, value in failed.items()
               if key != "inheritances"},
            "started_at": started_at, "finished_at": clock.now_iso(),
        })
        record_event(storage, clock, batch_id, run_id, "run_failed", {"error": str(exc)})
        raise

    return {
        **run_common,
        "timings": timings,
        "blocking_stats": blocking_stats,
        "conflicts": conflicts,
        "inherited_decisions": run_common["inheritances"],
        "inheritance_mismatches": [
            {
                "pair_key": mismatch["pair_key"],
                "reason": mismatch["reason"],
                "source_batch_id": mismatch["cross_decision"]["source_batch_id"],
            }
            for mismatch in mismatches
        ],
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
    result["inheritances"] = json.loads(result.pop("inheritance_json", "[]") or "[]")
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
