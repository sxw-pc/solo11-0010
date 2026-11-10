"""Field-level survivorship rules for composing a golden record."""
from __future__ import annotations

from collections import Counter
from typing import Any, Callable

from .config import AppConfig, SURVIVOR_FIELDS
from .models import Record


def _rank(record: Record, priority: dict[str, int]) -> tuple[int, str]:
    return priority.get(record.source, len(priority)), record.key


def _non_null(records: list[Record], field: str) -> list[tuple[Record, Any]]:
    return [(record, record.norm.get(field)) for record in records if record.norm.get(field)]


def _longest(records: list[Record], field: str, priority: dict[str, int]) -> tuple[Any, list[str]]:
    candidates = _non_null(records, field)
    if not candidates:
        return None, []
    candidates.sort(key=lambda pair: (-len(str(pair[1])), _rank(pair[0], priority)))
    return candidates[0][1], [candidates[0][0].key]


def _most_frequent(
    records: list[Record], field: str, priority: dict[str, int]
) -> tuple[Any, list[str]]:
    candidates = _non_null(records, field)
    if not candidates:
        return None, []
    counts = Counter(str(value) for _, value in candidates)
    by_record = {str(value): record for record, value in sorted(candidates, key=lambda p: _rank(p[0], priority))}
    winner = sorted(counts, key=lambda value: (-counts[value], _rank(by_record[value], priority), value))[0]
    chosen = [record.key for record, value in candidates if str(value) == winner]
    return winner, chosen


def _min_year(records: list[Record], field: str, priority: dict[str, int]) -> tuple[Any, list[str]]:
    candidates = _non_null(records, field)
    if not candidates:
        return None, []
    value = min(int(item[1]) for item in candidates)
    chosen = sorted((record.key for record, raw in candidates if int(raw) == value))
    return value, chosen


def _source_priority(
    records: list[Record], field: str, priority: dict[str, int]
) -> tuple[Any, list[str]]:
    candidates = _non_null(records, field)
    if not candidates:
        return None, []
    record, value = min(candidates, key=lambda pair: _rank(pair[0], priority))
    return value, [record.key]


def _non_null_concat(
    records: list[Record], field: str, priority: dict[str, int]
) -> tuple[Any, list[str]]:
    ordered = sorted(records, key=lambda record: _rank(record, priority))
    if field == "authors":
        ranked = sorted(
            (record for record in records if record.authors),
            key=lambda record: (-len(record.authors), _rank(record, priority)),
        )
        if not ranked:
            return [], []
        authors: list[dict[str, str]] = []
        seen: set[str] = set()
        provenance: list[str] = []
        for record in ranked:
            added = False
            for author in record.authors:
                key = author.get("key", "")
                if key and key not in seen:
                    seen.add(key)
                    authors.append(dict(author))
                    added = True
            if added:
                provenance.append(record.key)
        return authors, provenance
    values: list[str] = []
    provenance = []
    for record in ordered:
        value = record.norm.get(field)
        if value is None:
            continue
        text = str(value)
        if text and text not in values:
            values.append(text)
            provenance.append(record.key)
    return ("; ".join(values) if values else None), provenance


_RULES: dict[str, Callable[[list[Record], str, dict[str, int]], tuple[Any, list[str]]]] = {
    "longest": _longest,
    "most_frequent": _most_frequent,
    "min_year": _min_year,
    "source_priority": _source_priority,
    "non_null_concat": _non_null_concat,
}


def build_golden(records: list[Record], config: AppConfig) -> dict[str, Any]:
    """Compose one golden record from a non-empty cluster.

    Rules are applied independently. Deterministic tie breaking uses source
    priority then record key. Complexity is O(fields * members log members).
    """
    if not records:
        raise ValueError("cannot build a golden record from an empty cluster")
    priority = {
        source: index for index, source in enumerate(config.survivorship.source_priority)
    }
    fields: dict[str, Any] = {}
    provenance: dict[str, list[str]] = {}
    for field in SURVIVOR_FIELDS:
        rule = getattr(config.survivorship, field)
        value, sources = _RULES[rule](list(records), field, priority)
        fields[field] = value
        provenance[field] = sources
    fields["member_count"] = len(records)
    fields["provenance"] = provenance
    return fields
