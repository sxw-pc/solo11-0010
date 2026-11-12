"""Configuration models for the field-level semantic drift guard.

The drift guard is fully configuration driven. Every detector, severity
threshold, scoring weight and the baseline policy can be tuned from
``configs/default.yaml`` without touching code; the whole drift block is part
of the canonical :class:`~citefuse.config.AppConfig` fingerprint, so changing a
threshold changes the recorded config hash and keeps runs reproducible.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Drift severities, ordered from least to most severe. A record's overall
# tier is the maximum tier reported by any configured field detector.
TIER_ORDER: tuple[str, ...] = ("clean", "minor", "medium", "heavy")
TIER_RANK = {tier: index for index, tier in enumerate(TIER_ORDER)}

# What happens to a record at each tier.
ACTION_PASS = "pass"            # clean / auto-corrected minor -> core pipeline
ACTION_FLAG = "low_priority"    # medium -> tagged low-priority lane
ACTION_QUARANTINE = "quarantine"  # heavy -> isolated storage, no core matching
ACTION_BY_TIER = {
    "clean": ACTION_PASS,
    "minor": ACTION_PASS,
    "medium": ACTION_FLAG,
    "heavy": ACTION_QUARANTINE,
}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BaselineConfig(_Strict):
    """How historical observations are turned into a validation baseline."""

    # A field needs at least this many historical observations before the
    # distribution-based checks activate; below it only rule checks run.
    min_samples: int = Field(default=20, ge=1)
    # Update the baseline with the accepted (clean / minor) records of each
    # run. When false the baseline is only built explicitly.
    update_on_run: bool = True
    # Only records at or below this tier are folded back into the baseline,
    # so medium/heavy drift never contaminates the reference distribution.
    absorb_max_tier: Literal["clean", "minor"] = "minor"
    # Cap on distinct categorical values retained per field to bound storage.
    max_categories: int = Field(default=500, ge=10)
    # Cap on distinct character-ngram features retained per field.
    max_profile_features: int = Field(default=1000, ge=50)


class SeverityConfig(_Strict):
    """Score cut-offs mapping a continuous drift score to a tier.

    A score ``s`` resolves to:
      * ``minor``   when ``minor <= s < medium``
      * ``medium``  when ``medium <= s < heavy``
      * ``heavy``   when ``s >= heavy``
    Rule-based detectors may also assign a tier directly.
    """

    minor: float = Field(default=0.25, ge=0.0, le=1.0)
    medium: float = Field(default=0.55, ge=0.0, le=1.0)
    heavy: float = Field(default=0.80, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _ordered(self) -> "SeverityConfig":
        if not (self.minor <= self.medium <= self.heavy):
            raise ValueError("severity thresholds must satisfy minor <= medium <= heavy")
        return self

    def tier_for(self, score: float) -> str:
        if score >= self.heavy:
            return "heavy"
        if score >= self.medium:
            return "medium"
        if score >= self.minor:
            return "minor"
        return "clean"


class FieldDriftConfig(_Strict):
    """Common per-field settings."""

    enabled: bool = True
    # Contribution of this field when aggregating per-field scores into the
    # record-level drift score. Zero means the field still tags findings but
    # does not move the aggregate score.
    weight: float = Field(default=1.0, ge=0.0, le=10.0)


class YearDriftConfig(FieldDriftConfig):
    # Inclusive plausible publication-year window; outside it is anomalous.
    min_year: int = Field(default=1500, ge=1, le=9999)
    max_year: int = Field(default=2100, ge=1, le=9999)
    # Two-digit years are pivoted into a full year around this value.
    two_digit_pivot: int = Field(default=70, ge=0, le=99)
    # Number of standard deviations from the historical mean that counts as a
    # distribution-level outlier.
    outlier_sigmas: float = Field(default=3.0, gt=0.0)
    # Drift score contributed by each rule outcome.
    score_era_modern: float = Field(default=0.30, ge=0.0, le=1.0)
    score_era_imperial: float = Field(default=0.60, ge=0.0, le=1.0)
    score_era_unparsed: float = Field(default=0.70, ge=0.0, le=1.0)
    score_unrecognized: float = Field(default=0.90, ge=0.0, le=1.0)
    score_outlier: float = Field(default=0.60, ge=0.0, le=1.0)
    score_two_digit: float = Field(default=0.30, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _window(self) -> "YearDriftConfig":
        if self.min_year > self.max_year:
            raise ValueError("year min_year must be <= max_year")
        return self


class SourceDriftConfig(FieldDriftConfig):
    # Known source codes (e.g. ORBIT). Values observed in the baseline are
    # always accepted even when absent from this static list.
    known_codes: list[str] = Field(default_factory=list)
    # Static alias table: long organization / platform name -> canonical code.
    aliases: dict[str, str] = Field(default_factory=dict)
    # Score for a value that normalizes onto a known alias (auto-correctable).
    score_alias: float = Field(default=0.30, ge=0.0, le=1.0)
    # Score for a categorical value never seen in the baseline.
    score_unknown_category: float = Field(default=0.65, ge=0.0, le=1.0)
    # Score for a value whose shape is nothing like a source token.
    score_unrecognized: float = Field(default=0.90, ge=0.0, le=1.0)
    # A known code shape is an uppercase token of letters/digits this long.
    code_min_len: int = Field(default=2, ge=1, le=20)
    code_max_len: int = Field(default=20, ge=1, le=40)

    @field_validator("known_codes")
    @classmethod
    def _codes(cls, values: list[str]) -> list[str]:
        return sorted({v.strip().upper() for v in values if v.strip()})

    @field_validator("aliases")
    @classmethod
    def _alias_map(cls, values: dict[str, str]) -> dict[str, str]:
        folded: dict[str, str] = {}
        for alias, code in values.items():
            alias_key = alias.strip().casefold()
            if alias_key and code.strip():
                folded[alias_key] = code.strip().upper()
        return folded

    @model_validator(mode="after")
    def _len_window(self) -> "SourceDriftConfig":
        if self.code_min_len > self.code_max_len:
            raise ValueError("source code_min_len must be <= code_max_len")
        return self


class AuthorDriftConfig(FieldDriftConfig):
    # Substrings (case-insensitive) that mark a collective / corporate body.
    collective_markers: list[str] = Field(default_factory=list)
    score_collective_only: float = Field(default=0.65, ge=0.0, le=1.0)
    score_collective_mixed: float = Field(default=0.35, ge=0.0, le=1.0)
    score_unrecognized: float = Field(default=0.90, ge=0.0, le=1.0)
    # Above this fraction of punctuation/digit characters an entry is junk.
    junk_char_ratio: float = Field(default=0.45, gt=0.0, le=1.0)

    @field_validator("collective_markers")
    @classmethod
    def _markers(cls, values: list[str]) -> list[str]:
        seen = sorted({v.strip().casefold() for v in values if v.strip()})
        return seen


class GenericDriftConfig(FieldDriftConfig):
    """Distribution + optional regex validation for any scalar field."""

    # When set, every non-null value must fully match this regular expression.
    pattern: str | None = None
    # Categorical fields compare against the baseline value distribution;
    # numeric fields compare against mean/std. Free-text fields compare their
    # character-ngram profiles via Jaccard similarity. "shape" fields are only
    # validated against ``pattern`` (suitable for high-cardinality identifiers
    # such as DOI, where every legitimate value is inherently unique).
    kind: Literal["categorical", "numeric", "text", "shape"] = "categorical"
    score_pattern_violation: float = Field(default=0.70, ge=0.0, le=1.0)
    score_unknown_category: float = Field(default=0.60, ge=0.0, le=1.0)
    score_numeric_outlier: float = Field(default=0.60, ge=0.0, le=1.0)
    # Minimum Jaccard similarity to the baseline character profile; below it
    # the value is treated as profile drift.
    min_profile_similarity: float = Field(default=0.25, ge=0.0, le=1.0)
    score_profile_drift: float = Field(default=0.55, ge=0.0, le=1.0)
    outlier_sigmas: float = Field(default=3.0, gt=0.0)

    @field_validator("pattern")
    @classmethod
    def _pattern(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        # Fail fast at config load time rather than on the first record.
        import re

        try:
            re.compile(value)
        except re.error as exc:  # pragma: no cover - defensive
            raise ValueError(f"invalid generic pattern {value!r}: {exc}") from exc
        return value


class DriftConfig(_Strict):
    """Top-level drift-guard configuration block (``drift:`` in YAML)."""

    enabled: bool = True
    # When the guard itself raises, fail open: skip detection and let the
    # original pipeline proceed. Every degradation is audited.
    fail_open: bool = True
    severity: SeverityConfig = SeverityConfig()
    baseline: BaselineConfig = BaselineConfig()
    year: YearDriftConfig = YearDriftConfig()
    source: SourceDriftConfig = SourceDriftConfig()
    authors: AuthorDriftConfig = AuthorDriftConfig()
    # Additional generic detectors keyed by field name, e.g. ``doi``.
    generic: dict[str, GenericDriftConfig] = Field(default_factory=dict)
    # Emit a drift statistics summary with every pipeline run result.
    include_summary: bool = True
    # Persist one audit_events row per record disposition (in addition to the
    # per-run summary event).
    audit_per_record: bool = False

    @field_validator("generic")
    @classmethod
    def _generic_keys(cls, values: dict[str, GenericDriftConfig]) -> dict[str, GenericDriftConfig]:
        for key in values:
            if not key or "." in key:
                raise ValueError(f"invalid generic drift field name: {key!r}")
        return values

    def field_weight(self, field_name: str) -> float:
        section = getattr(self, field_name, None)
        if isinstance(section, FieldDriftConfig):
            return section.weight
        section = self.generic.get(field_name)
        return section.weight if section is not None else 0.0

    def is_field_enabled(self, field_name: str) -> bool:
        section = getattr(self, field_name, None)
        if isinstance(section, FieldDriftConfig):
            return bool(section.enabled)
        section = self.generic.get(field_name)
        return bool(section.enabled) if section is not None else False

    def enabled_fields(self) -> list[str]:
        names = [name for name in ("year", "source", "authors") if self.is_field_enabled(name)]
        names.extend(sorted(name for name, cfg in self.generic.items() if cfg.enabled))
        return names
