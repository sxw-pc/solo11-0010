"""Configuration loading, validation and canonical hashing.

The YAML file is parsed once at startup. Its canonical JSON representation is
hashed with SHA-256; that hash is attached to every pipeline run and metric
report so results remain auditable.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_CONFIG_PATH = Path(__file__).resolve().parent / "resources" / "default.yaml"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "default.yaml"

FIELD_NAMES = ("title", "authors", "venue", "year", "pages", "doi")
SURVIVOR_FIELDS = (
    "title", "authors", "venue", "year", "volume", "issue", "pages", "doi",
)
RULE_NAMES = {"longest", "most_frequent", "min_year", "source_priority", "non_null_concat"}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class YearVenueConfig(_Strict):
    enabled: bool = True
    venue_prefix_len: int = Field(default=5, ge=1, le=60)


class TitleSignatureConfig(_Strict):
    enabled: bool = True
    token_count: int = Field(default=5, ge=1, le=30)
    min_token_length: int = Field(default=2, ge=1, le=20)


class AuthorYearConfig(_Strict):
    enabled: bool = True


class BlockingConfig(_Strict):
    year_venue: YearVenueConfig = YearVenueConfig()
    title_signature: TitleSignatureConfig = TitleSignatureConfig()
    author_year: AuthorYearConfig = AuthorYearConfig()


class NormalizationConfig(_Strict):
    unicode_form: Literal["NFKC", "NFC"] = "NFKC"
    casefold: bool = True
    strip_punctuation: bool = True
    author_formats: list[Literal["family_given", "given_family"]] = Field(
        default_factory=lambda: ["family_given", "given_family"]
    )
    title_stopwords: list[str] = Field(default_factory=list)
    venue_aliases: dict[str, list[str]] = Field(default_factory=dict)

    @field_validator("title_stopwords")
    @classmethod
    def _fold_stopwords(cls, values: list[str]) -> list[str]:
        return sorted({value.strip().casefold() for value in values if value.strip()})


class ScoringConfig(_Strict):
    weights: dict[str, float]
    missing_policy: Literal["neutral", "exclude"] = "neutral"
    neutral_score: float = Field(default=0.5, ge=0.0, le=1.0)
    year_diff_scores: list[float] = Field(default_factory=lambda: [1.0, 0.75, 0.4])
    review_low: float = Field(default=0.68, ge=0.0, le=1.0)
    match_threshold: float = Field(default=0.80, ge=0.0, le=1.0)

    @field_validator("weights")
    @classmethod
    def _valid_weights(cls, weights: dict[str, float]) -> dict[str, float]:
        unknown = sorted(set(weights) - set(FIELD_NAMES))
        if unknown:
            raise ValueError(f"unknown scoring fields: {unknown}")
        if any(value < 0 for value in weights.values()):
            raise ValueError("weights must be non-negative")
        if sum(weights.values()) <= 0:
            raise ValueError("at least one field weight must be positive")
        return {name: float(weights.get(name, 0.0)) for name in FIELD_NAMES}

    @field_validator("year_diff_scores")
    @classmethod
    def _year_scores(cls, values: list[float]) -> list[float]:
        if not values or any(v < 0 or v > 1 for v in values):
            raise ValueError("year_diff_scores must contain scores in [0, 1]")
        return [float(v) for v in values]

    @model_validator(mode="after")
    def _thresholds(self) -> "ScoringConfig":
        if self.review_low > self.match_threshold:
            raise ValueError("review_low must be <= match_threshold")
        return self


class ReviewConfig(_Strict):
    default_page_size: int = Field(default=10, ge=1, le=100)
    max_page_size: int = Field(default=100, ge=1, le=1000)


class SurvivorshipConfig(_Strict):
    title: str = "longest"
    authors: str = "non_null_concat"
    venue: str = "most_frequent"
    year: str = "min_year"
    volume: str = "non_null_concat"
    issue: str = "non_null_concat"
    pages: str = "longest"
    doi: str = "source_priority"
    source_priority: list[str] = Field(default_factory=list)

    @field_validator("source_priority")
    @classmethod
    def _sources(cls, values: list[str]) -> list[str]:
        return [value.strip().upper() for value in values if value.strip()]

    @model_validator(mode="after")
    def _rules(self) -> "SurvivorshipConfig":
        for name in SURVIVOR_FIELDS:
            rule = getattr(self, name)
            if rule not in RULE_NAMES:
                raise ValueError(f"unsupported survivorship rule {rule!r} for {name}")
        return self


class AppConfig(_Strict):
    version: int = 1
    normalization: NormalizationConfig = NormalizationConfig()
    blocking: BlockingConfig = BlockingConfig()
    scoring: ScoringConfig
    survivorship: SurvivorshipConfig = SurvivorshipConfig()
    review: ReviewConfig = ReviewConfig()

    def canonical_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def fingerprint(self) -> str:
        payload = json.dumps(
            self.canonical_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def resolve_config_path(path: str | Path | None = None) -> Path:
    """Resolve an explicit path, the repo config, then the packaged fallback."""
    if path:
        return Path(path)
    env_path = os.environ.get("CITEFUSE_CONFIG")
    candidates = [
        Path(env_path) if env_path else None,
        DEFAULT_CONFIG_PATH,
        Path.cwd() / "configs" / "default.yaml",
        PACKAGE_CONFIG_PATH,
    ]
    for candidate in candidates:
        if candidate is not None and candidate.is_file():
            return candidate
    raise FileNotFoundError("could not locate configs/default.yaml")


def load_config(path: str | Path | None = None) -> AppConfig:
    """Load and validate a YAML config. The path defaults to configs/default.yaml."""
    config_path = resolve_config_path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")
    return AppConfig.model_validate(raw)
