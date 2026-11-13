"""Pairwise precision/recall/F1, exact-cluster rate and threshold replay."""
from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path
from typing import Any

from .clustering import cluster_records
from .config import AppConfig
from .models import Record, record_from_db
from .review import decision_dicts
from .storage import Storage


class MetricsError(ValueError):
    """Raised when a run cannot be scored (for example, no gold labels)."""


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _predicted_maps(
    records: list[Record], groups: list[list[Record]]
) -> tuple[dict[str, str], dict[str, list[str]]]:
    membership: dict[str, str] = {}
    for index, group in enumerate(groups):
        label = f"pred_{index:05d}"
        for record in group:
            membership[record.key] = label
    by_cluster: dict[str, list[str]] = {}
    for key, label in membership.items():
        by_cluster.setdefault(label, []).append(key)
    return membership, by_cluster


def evaluate_groups(
    records: list[Record], groups: list[list[Record]]
) -> dict[str, Any]:
    """Compute pairwise P/R/F1 and exact-cluster rate against gold labels."""
    gold = {record.key: record.gold_cluster for record in records if record.gold_cluster}
    if not gold:
        raise MetricsError("no gold_cluster labels are present for this run")
    predicted, predicted_groups = _predicted_maps(records, groups)
    true_positive = false_positive = false_negative = 0

    for members in predicted_groups.values():
        labeled = [key for key in members if key in gold]
        for left, right in combinations(sorted(labeled), 2):
            if gold[left] == gold[right]:
                true_positive += 1
            else:
                false_positive += 1

    gold_groups: dict[str, list[str]] = {}
    for key, label in gold.items():
        gold_groups.setdefault(label, []).append(key)
    for members in gold_groups.values():
        for left, right in combinations(sorted(members), 2):
            if predicted.get(left) != predicted.get(right):
                false_negative += 1

    precision = _ratio(true_positive, true_positive + false_positive)
    recall = _ratio(true_positive, true_positive + false_negative)
    f1 = _ratio(2 * precision * recall, precision + recall) if precision + recall else 0.0
    gold_sets = {frozenset(members) for members in gold_groups.values()}
    labeled_clusters = [
        set(members) & set(gold) for members in predicted_groups.values()
        if set(members) & set(gold)
    ]
    exact = sum(1 for members in labeled_clusters if frozenset(members) in gold_sets)
    return {
        "pairwise": {
            "true_positive": true_positive,
            "false_positive": false_positive,
            "false_negative": false_negative,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        },
        "cluster_exact": {
            "exact": exact,
            "predicted_labeled_clusters": len(labeled_clusters),
            "gold_clusters": len(gold_sets),
            "exact_rate": _ratio(exact, len(labeled_clusters)),
        },
        "labeled_records": len(gold),
        "predicted_clusters": len(groups),
    }


def _records(storage: Storage, batch_id: str) -> list[Record]:
    return [record_from_db(row) for row in storage.list_records(batch_id)]


def _threshold_groups(
    storage: Storage, config: AppConfig, batch_id: str, run_id: str, threshold: float
) -> tuple[list[Record], list[list[Record]]]:
    records = _records(storage, batch_id)
    history = decision_dicts(storage, batch_id)
    # Honor the instant-reflow view: pairs removed from the candidate pool by a
    # human 'different' decision must not re-enter through threshold replay.
    edges = [
        {"pair_key": row["pair_key"], "rec_a_pk": row["rec_a_pk"],
         "rec_b_pk": row["rec_b_pk"], "score": row["score"]}
        for row in storage.list_pairs(run_id, include_excluded=False)
        if row["score"] >= threshold
    ]
    groups, _, _ = cluster_records(records, edges, history)
    return records, groups


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def build_report(
    storage: Storage, config: AppConfig, reports_dir: Path, run_id: str,
    threshold: float | None = None,
) -> dict[str, Any]:
    """Evaluate a stored run; does not rerun normalization, blocking or scoring."""
    run = storage.get_run(run_id)
    if run is None:
        raise LookupError(f"run {run_id!r} not found")
    applied_threshold = float(
        config.scoring.match_threshold if threshold is None else threshold
    )
    records, groups = _threshold_groups(
        storage, config, run["batch_id"], run_id, applied_threshold
    )
    result = evaluate_groups(records, groups)
    report = {
        "run_id": run_id,
        "batch_id": run["batch_id"],
        "config_hash": run["config_hash"],
        "threshold": applied_threshold,
        **result,
    }
    path = reports_dir / f"{run_id}.json"
    _write_report(path, report)
    report["report_path"] = str(path)
    return report


def scan_thresholds(
    storage: Storage,
    config: AppConfig,
    reports_dir: Path,
    run_id: str,
    thresholds: list[float],
) -> dict[str, Any]:
    """Replay stored pair scores over a threshold grid (no re-blocking)."""
    run = storage.get_run(run_id)
    if run is None:
        raise LookupError(f"run {run_id!r} not found")
    if not thresholds:
        raise MetricsError("threshold grid cannot be empty")
    if any(value < 0.0 or value > 1.0 for value in thresholds):
        raise MetricsError("thresholds must be within [0, 1]")
    rows: list[dict[str, Any]] = []
    for threshold in sorted(set(round(value, 6) for value in thresholds)):
        records, groups = _threshold_groups(
            storage, config, run["batch_id"], run_id, threshold
        )
        metrics = evaluate_groups(records, groups)
        rows.append({
            "threshold": threshold,
            "precision": metrics["pairwise"]["precision"],
            "recall": metrics["pairwise"]["recall"],
            "f1": metrics["pairwise"]["f1"],
            "cluster_exact_rate": metrics["cluster_exact"]["exact_rate"],
            "cluster_count": metrics["predicted_clusters"],
        })
    payload = {
        "run_id": run_id,
        "batch_id": run["batch_id"],
        "config_hash": run["config_hash"],
        "thresholds": rows,
    }
    path = reports_dir / f"{run_id}_thresholds.json"
    _write_report(path, payload)
    payload["report_path"] = str(path)
    return payload
