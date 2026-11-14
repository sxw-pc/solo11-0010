"""Field-quality preflight for a batch, computed before candidate generation.

The preflight measures, per core field, how many *valid normalized* records
lack a usable value (missing rate) and how many carry a formally present but
untrustworthy value (anomaly rate). Rates are graded against the configured
warn/critical thresholds and the resulting summary is embedded into the
run's ``blocking_stats`` so ``/runs`` and ``/metrics/report`` expose exactly
the same snapshot.

Placement decision (run time, not ingest time): the summary is computed in
``run_pipeline`` immediately before blocking, on purpose.

* Grading depends on the preflight thresholds, which live in the run's
  config; the config hash is part of ``run_id``. Computing at run time binds
  each summary to its own run, so two runs over the same batch with
  different thresholds keep independent summaries. Ingest-time computation
  would instead store a run-level judgement in batch metadata, where a
  re-posted batch (same ``input_hash``) short-circuits and never recomputes
  under new thresholds, and concurrent uploads of one batch could overwrite
  each other's stats.
* The preflight's only purpose is to explain candidate-pair volume, which is
  a run-level concern; batch metadata stays clean.
* Recomputation per run is a single O(n) pass over the records, negligible
  next to the O(sum(bucket_size^2)) blocking stage that follows it.

Definitions (all on normalized values, denominator = valid record count):

* missing: the normalized value is empty (None / "" / []).
* anomalous: a value exists but is formally untrustworthy, e.g. a
  placeholder title, a title with no usable signature token, authors
  without any given name, a venue outside the configured canonical set, or
  a DOI that does not look like one.

A batch with zero valid records yields a degraded summary (rates are null,
never a division by zero) and the pipeline records a ``preflight_degraded``
audit event; any unexpected preflight failure degrades the same way instead
of aborting the run.
"""
from __future__ import annotations

from typing import Any, Iterable

from .config import AppConfig
from .models import Record
from .normalize import normalize_text
from .similarity import tokenize

CORE_FIELDS = ("title", "authors", "venue", "year", "pages", "doi")

# Normalized placeholder titles carrying no bibliographic signal. Values are
# compared after normalization, so "N/A" is stored in its folded form "n a".
_PLACEHOLDER_TITLES = frozenset({
    "n a", "na", "none", "null", "unknown", "untitled", "no title",
    "not available", "tbd", "暂无", "无标题", "未知",
})


def _is_missing(record: Record, field: str) -> bool:
    if field == "title":
        return not record.title
    if field == "authors":
        return not record.authors
    return getattr(record, field) is None


def _title_anomalous(title: str, config: AppConfig) -> bool:
    if title in _PLACEHOLDER_TITLES:
        return True
    # A title whose tokens are all stopwords or too short cannot produce a
    # title_signature bucket key; that is exactly the degenerate case the
    # preflight exists to surface.
    block = config.blocking.title_signature
    stopwords = set(config.normalization.title_stopwords)
    tokens = [
        token
        for token in tokenize(title)
        if token not in stopwords and len(token) >= block.min_token_length
    ]
    return not tokens


def _authors_anomalous(authors: list[dict[str, str]]) -> bool:
    # Every parsed author reduced to a bare family name: the source strings
    # were present but carried no usable given name.
    return bool(authors) and all(not author.get("given") for author in authors)


def _venue_anomalous(venue: str, canonical_venues: set[str]) -> bool:
    # Unknown venue: it survives normalization but matches no configured
    # canonical venue. Skipped entirely when no aliases are configured.
    return bool(canonical_venues) and venue not in canonical_venues


def _doi_anomalous(doi: str) -> bool:
    return not doi.startswith("10.") or "/" not in doi


def _canonical_venues(config: AppConfig) -> set[str]:
    venues: set[str] = set()
    for canonical in config.normalization.venue_aliases:
        normalized = normalize_text(canonical, config.normalization)
        if normalized:
            venues.add(normalized)
    return venues


def _is_anomalous(record: Record, field: str, config: AppConfig, canonical_venues: set[str]) -> bool:
    if field == "title":
        return _title_anomalous(record.title, config)
    if field == "authors":
        return _authors_anomalous(record.authors)
    if field == "venue":
        return _venue_anomalous(record.venue or "", canonical_venues)
    if field == "doi":
        return _doi_anomalous(record.doi or "")
    # year and pages are already constrained by their normalizers
    # (range-checked year, canonical page shape), so no anomaly rule applies.
    return False


def _thresholds(config: AppConfig) -> dict[str, float]:
    preflight = config.preflight
    return {
        "warn_missing_rate": preflight.warn_missing_rate,
        "critical_missing_rate": preflight.critical_missing_rate,
        "warn_anomaly_rate": preflight.warn_anomaly_rate,
        "critical_anomaly_rate": preflight.critical_anomaly_rate,
    }


def degraded_summary(reason: str) -> dict[str, Any]:
    """Empty summary used when the preflight cannot produce real rates."""
    return {
        "enabled": True,
        "record_count": 0,
        "degraded": True,
        "reason": reason,
        "status": "degraded",
        "fields": {},
        "alerts": [],
        "thresholds": {},
    }


def _disabled_summary(record_count: int) -> dict[str, Any]:
    return {
        "enabled": False,
        "record_count": record_count,
        "degraded": False,
        "status": "disabled",
        "fields": {},
        "alerts": [],
        "thresholds": {},
    }


def preflight_summary(records: Iterable[Record], config: AppConfig) -> dict[str, Any]:
    """Compute the per-field quality summary for one run.

    Pure and deterministic: the same records and config always yield the
    same summary, which keeps reruns of an identical run byte-stable.
    """
    ordered = sorted(records, key=lambda item: item.key)
    if not config.preflight.enabled:
        return _disabled_summary(len(ordered))

    thresholds = _thresholds(config)
    if not ordered:
        summary = degraded_summary("no_valid_records")
        summary["fields"] = {
            field: {
                "missing": 0,
                "anomalous": 0,
                "missing_rate": None,
                "anomaly_rate": None,
            }
            for field in CORE_FIELDS
        }
        summary["thresholds"] = thresholds
        return summary

    canonical_venues = _canonical_venues(config)
    total = len(ordered)
    fields: dict[str, dict[str, Any]] = {}
    for field in CORE_FIELDS:
        missing = sum(1 for record in ordered if _is_missing(record, field))
        anomalous = sum(
            1
            for record in ordered
            if not _is_missing(record, field)
            and _is_anomalous(record, field, config, canonical_venues)
        )
        fields[field] = {
            "missing": missing,
            "anomalous": anomalous,
            "missing_rate": round(missing / total, 6),
            "anomaly_rate": round(anomalous / total, 6),
        }

    alerts: list[dict[str, Any]] = []
    for field in CORE_FIELDS:
        stats = fields[field]
        for metric, warn_key, critical_key in (
            ("missing_rate", "warn_missing_rate", "critical_missing_rate"),
            ("anomaly_rate", "warn_anomaly_rate", "critical_anomaly_rate"),
        ):
            value = stats[metric]
            if value is None:
                continue
            if value >= thresholds[critical_key]:
                level, threshold = "critical", thresholds[critical_key]
            elif value >= thresholds[warn_key]:
                level, threshold = "warning", thresholds[warn_key]
            else:
                continue
            alerts.append({
                "field": field,
                "metric": metric,
                "level": level,
                "value": value,
                "threshold": threshold,
            })
    alerts.sort(key=lambda item: (item["field"], item["metric"]))

    if any(alert["level"] == "critical" for alert in alerts):
        status = "critical"
    elif any(alert["level"] == "warning" for alert in alerts):
        status = "warning"
    else:
        status = "ok"
    return {
        "enabled": True,
        "record_count": total,
        "degraded": False,
        "status": status,
        "fields": fields,
        "alerts": alerts,
        "thresholds": thresholds,
    }
