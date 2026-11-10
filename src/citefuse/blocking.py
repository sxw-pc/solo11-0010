"""Deterministic candidate-pair generation using a union of blocking keys."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
from typing import Callable, Iterable

from .config import AppConfig
from .models import Record, make_pair_key
from .similarity import tokenize


@dataclass
class CandidatePair:
    pair_key: str
    left: Record
    right: Record
    strategies: tuple[str, ...]


def _group_pairs(
    records: list[Record], bucket_fn: Callable[[Record], str | None]
) -> dict[tuple[int, int], None]:
    buckets: dict[str, list[Record]] = defaultdict(list)
    for record in records:
        bucket = bucket_fn(record)
        if bucket is not None:
            buckets[bucket].append(record)
    pairs: dict[tuple[int, int], None] = {}
    for bucket in sorted(buckets):
        members = sorted(buckets[bucket], key=lambda item: item.key)
        for left, right in combinations(members, 2):
            pairs[(left.pk, right.pk)] = None
    return pairs


def _year_venue(record: Record, config: AppConfig) -> str | None:
    if record.year is None or not record.venue:
        return None
    prefix_len = config.blocking.year_venue.venue_prefix_len
    return f"{record.year}|{record.venue[:prefix_len]}"


def _title_signature(record: Record, config: AppConfig) -> str | None:
    block = config.blocking.title_signature
    stopwords = set(config.normalization.title_stopwords)
    tokens = [
        token
        for token in tokenize(record.title)
        if token not in stopwords and len(token) >= block.min_token_length
    ]
    selected = sorted(tokens[: block.token_count])
    return "|".join(selected) if selected else None


def _author_year(record: Record, config: AppConfig) -> str | None:
    if record.year is None or not record.authors:
        return None
    family = record.authors[0].get("family")
    return f"{family}|{record.year}" if family else None


STRATEGY_FUNCS: dict[str, Callable[[Record, AppConfig], str | None]] = {
    "year_venue": _year_venue,
    "title_signature": _title_signature,
    "author_year": _author_year,
}


def generate_candidates(
    records: Iterable[Record], config: AppConfig
) -> tuple[list[CandidatePair], dict[str, object]]:
    """Generate the union of enabled blocking strategies.

    Each strategy is O(sum(bucket_size^2)); the union is deduplicated by
    record primary keys. Output pairs are sorted by record keys so that every
    downstream result is independent of input order.
    """
    ordered = sorted(records, key=lambda item: item.key)
    by_pair: dict[tuple[int, int], dict[str, object]] = {}
    strategy_counts: dict[str, int] = {}
    enabled = {
        "year_venue": config.blocking.year_venue.enabled,
        "title_signature": config.blocking.title_signature.enabled,
        "author_year": config.blocking.author_year.enabled,
    }
    for name, func in STRATEGY_FUNCS.items():
        if not enabled[name]:
            strategy_counts[name] = 0
            continue
        strategy_pairs = _group_pairs(ordered, lambda record: func(record, config))
        strategy_counts[name] = len(strategy_pairs)
        for left_pk, right_pk in strategy_pairs:
            entry = by_pair.setdefault((left_pk, right_pk), {"strategies": set()})
            entry["strategies"].add(name)  # type: ignore[union-attr]

    lookup = {record.pk: record for record in ordered}
    candidates: list[CandidatePair] = []
    for (left_pk, right_pk), entry in by_pair.items():
        left, right = lookup[left_pk], lookup[right_pk]
        first, _, pair_key = make_pair_key(left.key, right.key)
        if first != left.key:
            left, right = right, left
        candidates.append(
            CandidatePair(
                pair_key=pair_key,
                left=left,
                right=right,
                strategies=tuple(sorted(entry["strategies"])),  # type: ignore[arg-type]
            )
        )
    candidates.sort(key=lambda pair: pair.pair_key)
    stats = {
        "enabled": enabled,
        "strategy_counts": strategy_counts,
        "union_count": len(candidates),
    }
    return candidates, stats
