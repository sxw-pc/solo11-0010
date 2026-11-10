"""Declarative, explainable bibliographic field normalization.

Every record retains its original JSON and a normalized JSON snapshot. Optional
fields that cannot be parsed become null; a title that normalizes to empty is a
hard error and therefore becomes a dead letter during ingestion.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

from .config import NormalizationConfig
from .models import RawRecord

_DOI_PREFIX_RE = re.compile(
    r"^(?:https?://(?:dx\.)?doi\.org/|doi\s*:\s*|urn:doi:)", re.IGNORECASE
)
_DIGITS_RE = re.compile(r"\d+")
_SPACE_RE = re.compile(r"\s+")


class NormalizationError(ValueError):
    """Raised when a record cannot yield the required normalized title."""


def _collapse(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


def _fold(value: str, cfg: NormalizationConfig) -> str:
    text = unicodedata.normalize(cfg.unicode_form, value)
    return text.casefold() if cfg.casefold else text


def _separate_punctuation(value: str) -> str:
    chars: list[str] = []
    for char in value:
        category = unicodedata.category(char)
        if category[0] in {"P", "S", "Z"} or char == "_":
            chars.append(" ")
        else:
            chars.append(char)
    return "".join(chars)


def normalize_text(value: str | None, cfg: NormalizationConfig) -> str | None:
    """NFKC/casefold/punctuation normalization used by titles and venues."""
    if value is None:
        return None
    text = _fold(str(value), cfg)
    if cfg.strip_punctuation:
        text = _separate_punctuation(text)
    text = _collapse(text)
    return text or None


def normalize_token(value: Any, cfg: NormalizationConfig) -> str | None:
    """Normalize volume/issue-like identifiers while retaining inner spaces."""
    if value is None:
        return None
    return normalize_text(str(value), cfg)


def normalize_year(value: Any) -> int | None:
    """Extract a plausible four-digit publication year, else None."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        candidate = value
    else:
        match = re.search(r"(?<!\d)(\d{4})(?!\d)", str(value))
        candidate = int(match.group(1)) if match else None
    if candidate is not None and 1500 <= candidate <= 2100:
        return candidate
    return None


def normalize_pages(value: Any) -> str | None:
    """Extract and canonicalize an inclusive page range such as '123-129'."""
    if value is None or isinstance(value, bool):
        return None
    numbers = [int(item) for item in _DIGITS_RE.findall(str(value))]
    if not numbers:
        return None
    if len(numbers) == 1:
        return str(numbers[0])
    start, end = numbers[0], numbers[1]
    if end < start and len(str(end)) < len(str(start)):
        end = int(str(start)[: len(str(start)) - len(str(end))] + str(end))
    if end < start:
        start, end = end, start
    return str(start) if start == end else f"{start}-{end}"


def normalize_doi(value: str | None) -> str | None:
    """Strip common resolver prefixes, whitespace and case from a DOI."""
    if not value:
        return None
    text = _SPACE_RE.sub("", str(value))
    text = _DOI_PREFIX_RE.sub("", text)
    text = text.strip(".,;()[]<>").casefold()
    return text or None


def _normalize_person_part(value: str, cfg: NormalizationConfig) -> str:
    text = _fold(value, cfg)
    if cfg.strip_punctuation:
        chars = []
        for char in text:
            category = unicodedata.category(char)
            chars.append(" " if category[0] in {"P", "S", "Z"} or char == "_" else char)
        text = "".join(chars)
    return _collapse(text)


def parse_person(value: str, cfg: NormalizationConfig) -> dict[str, str] | None:
    """Parse 'Family, Given' or 'Given Family' into a canonical author dict."""
    raw = _collapse(unicodedata.normalize(cfg.unicode_form, str(value)))
    if not raw:
        return None
    has_comma = "," in raw
    if has_comma and "family_given" in cfg.author_formats:
        family_raw, given_raw = raw.split(",", 1)
    elif not has_comma and "given_family" in cfg.author_formats:
        parts = raw.split()
        if len(parts) == 1:
            family_raw, given_raw = parts[0], ""
        else:
            family_raw, given_raw = parts[-1], " ".join(parts[:-1])
    elif "family_given" in cfg.author_formats:
        family_raw, given_raw = raw, ""
    else:
        family_raw, given_raw = raw, ""
    family = _normalize_person_part(family_raw, cfg)
    given = _normalize_person_part(given_raw, cfg)
    if not family:
        return None
    initial = given[0] if given else ""
    key = f"{family.replace(' ', '')}|{initial}"
    return {"family": family, "given": given, "key": key}


class Normalizer:
    """Reusable normalizer with a precomputed venue alias index."""

    def __init__(self, config: NormalizationConfig) -> None:
        self.config = config
        self._venue_aliases: dict[str, str] = {}
        for canonical, aliases in config.venue_aliases.items():
            canonical_norm = normalize_text(canonical, config)
            if not canonical_norm:
                continue
            for alias in [canonical, *aliases]:
                alias_norm = normalize_text(alias, config)
                if alias_norm:
                    self._venue_aliases[alias_norm] = canonical_norm

    def title(self, value: str) -> str:
        normalized = normalize_text(value, self.config)
        if not normalized:
            raise NormalizationError("title is empty after normalization")
        return normalized

    def venue(self, value: str | None) -> str | None:
        normalized = normalize_text(value, self.config)
        if not normalized:
            return None
        return self._venue_aliases.get(normalized, normalized)

    def authors(self, values: list[str]) -> list[dict[str, str]]:
        parsed = [parse_person(value, self.config) for value in values]
        return [author for author in parsed if author is not None]

    def normalize(self, record: RawRecord) -> dict[str, Any]:
        """Return the normalized snapshot for a schema-valid raw record."""
        return {
            "title": self.title(record.title),
            "authors": self.authors(record.authors),
            "venue": self.venue(record.venue),
            "year": normalize_year(record.year),
            "volume": normalize_token(record.volume, self.config),
            "issue": normalize_token(record.issue, self.config),
            "pages": normalize_pages(record.pages),
            "doi": normalize_doi(record.doi),
        }
