"""Typed results emitted by the drift guard."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .config import ACTION_BY_TIER, TIER_RANK, SeverityConfig


def resolve_tier(
    score: float, severity: SeverityConfig, min_tier: str = "minor"
) -> str:
    """Resolve a continuous score to a tier, honoring a rule-mandated floor."""
    scored_tier = severity.tier_for(score)
    if TIER_RANK[scored_tier] >= TIER_RANK[min_tier]:
        return scored_tier
    return min_tier


@dataclass(frozen=True)
class FieldVerdict:
    """The drift decision for one field of one record.

    Detectors emit a continuous ``score`` and may declare a ``min_tier`` floor
    for rules whose semantics demand a minimum disposition regardless of the
    numeric cut-offs (e.g. an unparseable era must never auto-correct). The
    final tier is resolved centrally from the configured severity thresholds,
    keeping every cut-off tunable in YAML.
    """

    field_name: str
    score: float                   # [0, 1] drift score
    reason_code: str               # stable machine-readable code, e.g. era_imperial
    message: str                   # human-readable explanation (Chinese)
    observed: Any                  # original field value
    tier: str = "clean"            # resolved centrally by the guard
    min_tier: str = "minor"        # rule-mandated disposition floor
    normalized: Any = None         # value after rule-level normalization, if any
    suggestion: Any = None         # proposed corrected value (minor auto-fix)
    detector: str = ""             # name of the detector that produced the verdict

    @property
    def is_drift(self) -> bool:
        return self.tier != "clean"

    def as_dict(self) -> dict[str, Any]:
        return {
            "field": self.field_name,
            "tier": self.tier,
            "score": round(self.score, 6),
            "reason_code": self.reason_code,
            "message": self.message,
            "observed": self.observed,
            "normalized": self.normalized,
            "suggestion": self.suggestion,
            "detector": self.detector,
        }


@dataclass
class RecordDriftResult:
    """All field verdicts for a record plus the record-level disposition."""

    record_key: str
    batch_id: str
    source: str
    record_id: str
    verdicts: list[FieldVerdict] = field(default_factory=list)
    # Snapshot baseline version used for this decision (audit / reproducibility).
    baseline_version: int = 0
    # Per-field weights from DriftConfig, used to aggregate the record score.
    field_weights: dict[str, float] = field(default_factory=dict)

    @property
    def tier(self) -> str:
        """The record's tier is the most severe field tier."""
        if not self.verdicts:
            return "clean"
        return max((v.tier for v in self.verdicts), key=lambda t: TIER_RANK[t])

    @property
    def score(self) -> float:
        """Weighted aggregate of per-field drift scores.

        The weighted mean stays in [0, 1] while letting important fields
        dominate; the discrete ``tier`` remains the max-severity driver so a
        single heavy verdict always quarantines regardless of weights.
        """
        if not self.verdicts:
            return 0.0
        weighted_sum = 0.0
        weight_sum = 0.0
        for verdict in self.verdicts:
            weight = float(self.field_weights.get(verdict.field_name, 1.0))
            weighted_sum += weight * verdict.score
            weight_sum += weight
        return weighted_sum / weight_sum if weight_sum else max(
            v.score for v in self.verdicts
        )

    @property
    def action(self) -> str:
        return ACTION_BY_TIER[self.tier]

    @property
    def is_quarantined(self) -> bool:
        return self.tier == "heavy"

    @property
    def is_low_priority(self) -> bool:
        return self.tier == "medium"

    @property
    def corrected_fields(self) -> dict[str, Any]:
        """Minor-tier auto-corrections keyed by field name."""
        return {
            v.field_name: v.suggestion
            for v in self.verdicts
            if v.tier == "minor" and v.suggestion is not None
        }

    def drift_verdicts(self) -> list[FieldVerdict]:
        return [v for v in self.verdicts if v.is_drift]

    def as_dict(self) -> dict[str, Any]:
        return {
            "record_key": self.record_key,
            "batch_id": self.batch_id,
            "source": self.source,
            "record_id": self.record_id,
            "tier": self.tier,
            "score": round(self.score, 6),
            "action": self.action,
            "baseline_version": self.baseline_version,
            "corrected_fields": self.corrected_fields,
            "verdicts": [v.as_dict() for v in self.verdicts],
        }


@dataclass
class DriftSummary:
    """Aggregate statistics attached to every guarded run."""

    total_records: int = 0
    clean: int = 0
    minor: int = 0
    medium: int = 0
    heavy: int = 0
    corrected: int = 0
    quarantined: int = 0
    low_priority: int = 0
    degraded: bool = False
    degradation_reason: str | None = None
    baseline_version: int = 0
    by_field: dict[str, dict[str, int]] = field(default_factory=dict)
    by_reason: dict[str, int] = field(default_factory=dict)

    def add(self, result: RecordDriftResult) -> None:
        self.total_records += 1
        tier = result.tier
        setattr(self, tier, getattr(self, tier) + 1)
        if result.corrected_fields:
            self.corrected += 1
        if result.is_quarantined:
            self.quarantined += 1
        if result.is_low_priority:
            self.low_priority += 1
        for verdict in result.drift_verdicts():
            bucket = self.by_field.setdefault(
                verdict.field_name,
                {"clean": 0, "minor": 0, "medium": 0, "heavy": 0},
            )
            bucket[verdict.tier] += 1
            key = f"{verdict.field_name}.{verdict.reason_code}"
            self.by_reason[key] = self.by_reason.get(key, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        total = max(self.total_records, 1)
        return {
            "total_records": self.total_records,
            "tier_counts": {
                "clean": self.clean,
                "minor": self.minor,
                "medium": self.medium,
                "heavy": self.heavy,
            },
            "tier_rates": {
                tier: round(getattr(self, tier) / total, 6)
                for tier in ("clean", "minor", "medium", "heavy")
            },
            "corrected_records": self.corrected,
            "quarantined_records": self.quarantined,
            "low_priority_records": self.low_priority,
            "degraded": self.degraded,
            "degradation_reason": self.degradation_reason,
            "baseline_version": self.baseline_version,
            "by_field": self.by_field,
            "by_reason": dict(sorted(self.by_reason.items())),
        }
