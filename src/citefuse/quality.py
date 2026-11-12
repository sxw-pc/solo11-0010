"""Source-level quality profiling.

Every completed pipeline run produces one profile row per source. A row
aggregates four signals that the run-level counters cannot attribute:

* bad-row rate      -- dead-letter lines attributed to the source / attempted
                        source lines (records this run clustered + attributed
                        dead letters).
* singleton rate    -- source records that end up alone in a cluster / source
                        records this run clustered.
* average candidate pair score -- mean score of every candidate pair touching
                        the source. A pair spanning two sources counts for both.
* overturn rate     -- human-ruled pairs touching the source where the human
                        disagreed with the model. Both "model match, human
                        different" and "model non-match, human same" count.
                        Pairs spanning two sources count for both.

Ratios whose denominator is zero are stored as NULL, never 0: a source with no
candidate pairs or no reviewed pairs has an *undefined* rate, not a perfect
one. The API serializes NULL as JSON null and skips threshold checks for it.

Idempotency: the profile is recomputed from the run's persisted pairs,
clusters and records and written with REPLACE semantics keyed by
(run_id, source). Re-running the same logical run (identical batch, config and
decision history) therefore overwrites the same rows instead of appending;
`source_quality` never grows by duplicate rows for one source in one run.

Cross-run cumulative policy (the trade-off this module deliberately fixes):

* Weight by denominator, not by a simple average of run-level ratios. A 3-row
  run and a 3000-row run should not carry equal weight; pooling the numerators
  and denominators yields the rate a reviewer would compute by hand over the
  combined population. Simple averaging is additionally undefined for any run
  whose denominator is zero.
* Only the latest completed run per batch is effective; older runs of the same
  batch are superseded. Every rerun recomputes the whole pipeline with the
  current decision history, so an older run is a stale snapshot of the same
  records. Including it would double count those records and let a
  before-review snapshot dilute the after-review picture. `superseded_runs`
  reports how many completed runs were excluded for this reason.
"""
from __future__ import annotations

import json
from typing import Any

from .clock import Clock
from .config import AppConfig
from .review import latest_decision_map
from .storage import Storage

class SourceQualityError(ValueError):
    """Raised when a profile cannot be computed (e.g. a failed run)."""


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _source_of(pk: int, sources_by_pk: dict[int, str]) -> str | None:
    return sources_by_pk.get(pk)


def compute_source_quality(
    storage: Storage,
    config: AppConfig,
    clock: Clock,
    run_id: str,
    decisions: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Compute and overwrite the per-source profile for one run.

    `decisions` is the decision history that was in effect for this run. The
    pipeline passes it explicitly so a historical run is profiled against its
    own snapshot. When omitted (lazy recomputation of a stored run) the batch's
    current history is used.
    """
    run = storage.get_run(run_id)
    if run is None:
        raise LookupError(f"run {run_id!r} not found")
    if run["status"] != "completed":
        raise SourceQualityError(
            f"run {run_id!r} has status {run['status']!r}; only completed runs are profiled"
        )
    batch_id = run["batch_id"]

    all_records = storage.list_records(batch_id)
    sources_by_pk = {int(row["record_pk"]): row["source"] for row in all_records}
    counts: dict[str, dict[str, Any]] = {}

    def bucket(source: str) -> dict[str, Any]:
        return counts.setdefault(source, {
            "source": source,
            "attempted_lines": 0,
            "bad_lines": 0,
            "record_count": 0,
            "singleton_count": 0,
            "candidate_pairs": 0,
            "candidate_score_sum": 0.0,
            "decided_pairs": 0,
            "overturned_pairs": 0,
        })

    # Record counts are scoped to the records this run actually clustered, so
    # a batch whose content was later re-ingested cannot inflate an old run's
    # singleton rate with records that run never saw.
    clusters = storage.list_clusters(run_id)
    for cluster in clusters:
        member_pks = json.loads(cluster["member_pks_json"])
        for raw_pk in member_pks:
            source = _source_of(int(raw_pk), sources_by_pk)
            if source is not None:
                item = bucket(source)
                item["record_count"] += 1
                item["attempted_lines"] += 1
        if len(member_pks) == 1:
            source = _source_of(int(member_pks[0]), sources_by_pk)
            if source is not None:
                bucket(source)["singleton_count"] += 1

    # Dead letters are batch-level (one structural batch, possibly re-posted).
    # Only attributed lines count toward a source; unattributed malformed lines
    # remain in the run-level bad_rows total.
    for row in storage.list_dead_letters(batch_id, limit=None):
        source = row["source"]
        if source:
            item = bucket(source)
            item["bad_lines"] += 1
            item["attempted_lines"] += 1

    # Candidate pairs: each endpoint's source is credited (same-source pairs once).
    for pair in storage.list_pairs(run_id):
        pair_sources = {
            source
            for source in (
                _source_of(int(pair["rec_a_pk"]), sources_by_pk),
                _source_of(int(pair["rec_b_pk"]), sources_by_pk),
            )
            if source is not None
        }
        for source in pair_sources:
            item = bucket(source)
            item["candidate_pairs"] += 1
            item["candidate_score_sum"] += float(pair["score"])

    # Overturn rate: only pairs that have a human ruling and exist in this run.
    history = decisions if decisions is not None else storage.decision_history(batch_id)
    latest = latest_decision_map(history)
    for pair in storage.list_pairs(run_id):
        decision = latest.get(pair["pair_key"])
        if decision is None:
            continue
        model_matched = pair["zone"] == "match"
        human_matched = decision["decision"] == "same"
        overturned = model_matched != human_matched
        pair_sources = {
            source
            for source in (
                _source_of(int(pair["rec_a_pk"]), sources_by_pk),
                _source_of(int(pair["rec_b_pk"]), sources_by_pk),
            )
            if source is not None
        }
        for source in pair_sources:
            item = bucket(source)
            item["decided_pairs"] += 1
            if overturned:
                item["overturned_pairs"] += 1

    computed_at = clock.now_iso()
    rows: list[dict[str, Any]] = []
    for source in sorted(counts):
        item = dict(counts[source])
        item["candidate_score_sum"] = round(item["candidate_score_sum"], 6)
        item["bad_row_rate"] = _ratio(item["bad_lines"], item["attempted_lines"])
        item["singleton_rate"] = _ratio(item["singleton_count"], item["record_count"])
        item["avg_candidate_score"] = _ratio(
            item["candidate_score_sum"], item["candidate_pairs"]
        )
        item["overturn_rate"] = _ratio(item["overturned_pairs"], item["decided_pairs"])
        item["computed_at"] = computed_at
        rows.append(item)

    storage.replace_source_quality(run_id, rows)
    return rows


def _thresholds(config: AppConfig) -> dict[str, float]:
    return {
        "bad_row_rate": config.source_quality.bad_row_rate_threshold,
        "overturn_rate": config.source_quality.overturn_rate_threshold,
    }


def _decorate(row: dict[str, Any], thresholds: dict[str, float]) -> dict[str, Any]:
    flags: list[str] = []
    if (
        row["bad_row_rate"] is not None
        and row["bad_row_rate"] > thresholds["bad_row_rate"]
    ):
        flags.append("high_bad_row_rate")
    if (
        row["overturn_rate"] is not None
        and row["overturn_rate"] > thresholds["overturn_rate"]
    ):
        flags.append("high_overturn_rate")
    payload = dict(row)
    payload["anomaly_flags"] = flags
    payload["anomalous"] = bool(flags)
    return payload


def ensure_source_quality(
    storage: Storage, config: AppConfig, clock: Clock, run_id: str
) -> list[dict[str, Any]]:
    """Return stored rows, computing them once if an older run lacks them."""
    rows = storage.list_source_quality(run_id)
    if not rows:
        compute_source_quality(storage, config, clock, run_id)
        rows = storage.list_source_quality(run_id)
    return rows


def run_quality_report(
    storage: Storage, config: AppConfig, clock: Clock, run_id: str
) -> dict[str, Any]:
    """Per-run source profile: the values for this run alone."""
    run = storage.get_run(run_id)
    if run is None:
        raise LookupError(f"run {run_id!r} not found")
    thresholds = _thresholds(config)
    rows = ensure_source_quality(storage, config, clock, run_id)
    return {
        "scope": "run",
        "run_id": run_id,
        "batch_id": run["batch_id"],
        "config_hash": run["config_hash"],
        "thresholds": thresholds,
        "sources": [_decorate(row, thresholds) for row in rows],
    }


def _effective_runs(storage: Storage, batch_id: str | None) -> tuple[list[Any], int]:
    """Latest completed run per batch plus the count of superseded runs."""
    completed = storage.query(
        """SELECT r.run_id, r.batch_id, r.created_at,
                  (SELECT MAX(a.attempt_id) FROM run_attempts a
                    WHERE a.run_id = r.run_id) AS last_attempt
             FROM runs r
            WHERE r.status = 'completed'"""
        + (" AND r.batch_id = ?" if batch_id else ""),
        (batch_id,) if batch_id else (),
    )
    by_batch: dict[str, Any] = {}
    for row in completed:
        key = (int(row["last_attempt"]) if row["last_attempt"] is not None else -1,
               row["created_at"], row["run_id"])
        current = by_batch.get(row["batch_id"])
        if current is None or key > (
            int(current["last_attempt"]) if current["last_attempt"] is not None else -1,
            current["created_at"],
            current["run_id"],
        ):
            by_batch[row["batch_id"]] = row
    effective = [by_batch[name] for name in sorted(by_batch)]
    return effective, len(completed) - len(effective)


def cumulative_quality_report(
    storage: Storage, config: AppConfig, clock: Clock, batch_id: str | None = None
) -> dict[str, Any]:
    """Pooled profile across the effective (latest completed) run per batch.

    Numerators and denominators are summed, then ratios recomputed; see the
    module docstring for why this beats a simple average and why superseded
    runs are excluded.
    """
    effective, superseded = _effective_runs(storage, batch_id)
    totals: dict[str, dict[str, Any]] = {}
    for run in effective:
        rows = ensure_source_quality(storage, config, clock, run["run_id"])
        for row in rows:
            item = totals.setdefault(row["source"], {
                "source": row["source"], "attempted_lines": 0, "bad_lines": 0,
                "record_count": 0, "singleton_count": 0, "candidate_pairs": 0,
                "candidate_score_sum": 0.0, "decided_pairs": 0,
                "overturned_pairs": 0,
            })
            for field in (
                "attempted_lines", "bad_lines", "record_count", "singleton_count",
                "candidate_pairs", "decided_pairs", "overturned_pairs",
            ):
                item[field] += int(row[field])
            item["candidate_score_sum"] += float(row["candidate_score_sum"])

    thresholds = _thresholds(config)
    sources: list[dict[str, Any]] = []
    for source in sorted(totals):
        item = totals[source]
        item["candidate_score_sum"] = round(item["candidate_score_sum"], 6)
        item["bad_row_rate"] = _ratio(item["bad_lines"], item["attempted_lines"])
        item["singleton_rate"] = _ratio(item["singleton_count"], item["record_count"])
        item["avg_candidate_score"] = _ratio(
            item["candidate_score_sum"], item["candidate_pairs"]
        )
        item["overturn_rate"] = _ratio(item["overturned_pairs"], item["decided_pairs"])
        sources.append(_decorate(item, thresholds))

    return {
        "scope": "cumulative",
        "batch_id": batch_id,
        "config_hash": config.fingerprint(),
        "thresholds": thresholds,
        "runs_included": [run["run_id"] for run in effective],
        "batches_included": [run["batch_id"] for run in effective],
        "superseded_runs": superseded,
        "sources": sources,
    }
