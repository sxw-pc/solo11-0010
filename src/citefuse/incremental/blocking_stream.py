"""Memory-bounded incremental candidate generation.

The full-run :func:`citefuse.blocking.generate_candidates` loads every record
into memory and builds bucket cross-products. An incremental task must avoid
loading the whole history, so this module:

1. Loads the *new* records (the small delta) fully and indexes their blocking
   keys in memory.
2. Scans historical rows in fixed-size pages via SQLite. Only one page of
   lightweight references plus the blocking-key index is resident at a time.
3. Emits every (historical, new) pair that shares at least one enabled
   blocking key. Per-strategy counts are accumulated by the caller from the
   strategy tuple attached to each emitted pair, so the history is scanned
   exactly once.

New-versus-new pairs are generated with the unchanged
:func:`generate_candidates`, so the three strategy implementations remain
single-sourced and the two paths cannot drift.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable

from ..blocking import STRATEGY_FUNCS, generate_candidates
from ..config import AppConfig
from ..models import Record, make_pair_key

#: Default page size for the historical-record streaming scan. Kept small so
#: peak resident memory stays bounded regardless of the history size.
DEFAULT_SCAN_PAGE_SIZE = 256


@dataclass(frozen=True)
class RecordRef:
    """Minimal record projection needed for blocking, scoring and clustering.

    It exposes the same ``pk``/``key`` and normalized-field properties as the
    full :class:`~citefuse.models.Record`, so the unmodified blocking, scoring
    and clustering functions accept it directly. Raw JSON (the bulkiest part
    of a stored record) is intentionally absent.
    """

    pk: int
    source: str
    record_id: str
    norm: dict[str, Any]

    def __hash__(self) -> int:
        # Primary keys are unique within a batch; the norm dict is unhashable
        # and must not participate in hashing/equality.
        return hash(self.pk)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RecordRef) and other.pk == self.pk

    @property
    def key(self) -> str:
        return f"{self.source}::{self.record_id}"

    @property
    def title(self) -> str:
        return self.norm.get("title") or ""

    @property
    def authors(self) -> list[dict[str, str]]:
        return list(self.norm.get("authors") or [])

    @property
    def venue(self) -> str | None:
        return self.norm.get("venue")

    @property
    def year(self) -> int | None:
        return self.norm.get("year")

    @property
    def pages(self) -> str | None:
        return self.norm.get("pages")

    @property
    def doi(self) -> str | None:
        return self.norm.get("doi")


def ref_from_row(row: Any) -> RecordRef:
    return RecordRef(
        pk=int(row["record_pk"]),
        source=row["source"],
        record_id=row["record_id"],
        norm=json.loads(row["norm_json"]),
    )


def stream_record_refs(
    storage: Any,
    batch_id: str,
    page_size: int = DEFAULT_SCAN_PAGE_SIZE,
) -> Iterable[RecordRef]:
    """Yield record references of a batch in deterministic pk order.

    The scan is keyset-paginated so peak memory is one page regardless of the
    total number of historical records.
    """
    if page_size < 1:
        raise ValueError("page_size must be positive")
    cursor_pk = 0
    while True:
        rows = storage.query(
            "SELECT record_pk,source,record_id,norm_json FROM records "
            "WHERE batch_id=? AND record_pk>? ORDER BY record_pk LIMIT ?",
            (batch_id, cursor_pk, page_size),
        )
        if not rows:
            return
        for row in rows:
            ref = ref_from_row(row)
            cursor_pk = ref.pk
            yield ref


def _enabled_strategies(config: AppConfig) -> list[str]:
    blocking = config.blocking
    return [
        name
        for name, enabled in (
            ("year_venue", blocking.year_venue.enabled),
            ("title_signature", blocking.title_signature.enabled),
            ("author_year", blocking.author_year.enabled),
        )
        if enabled
    ]


class NewBlockIndex:
    """Blocking-key index over the new (delta) records."""

    def __init__(self, new_records: list[RecordRef], config: AppConfig) -> None:
        self.config = config
        self.strategy_names = _enabled_strategies(config)
        # strategy -> blocking key -> list of new-record refs
        self.buckets: dict[str, dict[str, list[RecordRef]]] = {
            name: {} for name in self.strategy_names
        }
        for record in new_records:
            for name in self.strategy_names:
                key = STRATEGY_FUNCS[name](record, config)
                if key is None:
                    continue
                self.buckets[name].setdefault(key, []).append(record)
        for per_strategy in self.buckets.values():
            for members in per_strategy.values():
                members.sort(key=lambda item: item.key)

    def strategies_for(
        self, historical: RecordRef
    ) -> dict[RecordRef, tuple[str, ...]]:
        """Map each matching new record to the strategies that connect it."""
        per_new: dict[int, set[str]] = {}
        refs_by_pk: dict[int, RecordRef] = {}
        for name in self.strategy_names:
            key = STRATEGY_FUNCS[name](historical, config=self.config)
            if key is None:
                continue
            for member in self.buckets[name].get(key, ()):
                per_new.setdefault(member.pk, set()).add(name)
                refs_by_pk[member.pk] = member
        return {
            refs_by_pk[pk]: tuple(sorted(names))
            for pk, names in per_new.items()
        }

    def cross_counts(self, historical: RecordRef) -> dict[str, int]:
        """Full-run strategy counts contributed by one historical record.

        For every shared blocking key the historical record forms one pair
        with *each* new record in that bucket, so the contribution is the
        bucket size; pairs spanning several strategies count once per
        strategy, exactly as the full-run blocker counts them.
        """
        counts = {name: 0 for name in self.strategy_names}
        for name in self.strategy_names:
            key = STRATEGY_FUNCS[name](historical, config=self.config)
            if key is None:
                continue
            counts[name] += len(self.buckets[name].get(key, ()))
        return counts


def new_new_candidates(
    new_records: list[RecordRef], config: AppConfig
) -> tuple[list, dict[str, int]]:
    """Candidate pairs entirely inside the delta, via the unmodified blocker."""
    candidates, stats = generate_candidates(list(new_records), config)
    return candidates, dict(stats["strategy_counts"])


def scan_cross_matches(
    storage: Any,
    batch_id: str,
    new_records: list[RecordRef],
    config: AppConfig,
    historical_pks: set[int],
    page_size: int = DEFAULT_SCAN_PAGE_SIZE,
) -> tuple[
    list[tuple[RecordRef, RecordRef, tuple[str, ...]]], dict[str, int]
]:
    """Single streaming scan returning cross pairs and per-strategy counts.

    The history is read exactly once. Each unordered pair appears once with
    the full tuple of connecting strategies; ``strategy_counts`` follows the
    full-run rule (one count per strategy whose bucket holds both endpoints),
    so summing baseline old-old counts with these delta counts reproduces the
    full-run blocking statistics exactly.
    """
    index = NewBlockIndex(new_records, config)
    strategy_counts = {name: 0 for name in index.strategy_names}
    pairs: list[tuple[RecordRef, RecordRef, tuple[str, ...]]] = []
    seen: set[tuple[int, int]] = set()
    for historical in stream_record_refs(storage, batch_id, page_size):
        if historical.pk not in historical_pks:
            continue
        for name, count in index.cross_counts(historical).items():
            strategy_counts[name] += count
        matches = index.strategies_for(historical)
        for new_ref in sorted(matches, key=lambda item: item.key):
            marker = (historical.pk, new_ref.pk)
            if marker in seen:
                continue
            seen.add(marker)
            pairs.append((historical, new_ref, matches[new_ref]))
    return pairs, strategy_counts


def orient_pair(left: Record, right: Record) -> tuple[Record, Record, str]:
    """Return canonical ``(first, second, pair_key)`` ordered like a full run."""
    first, _, pair_key = make_pair_key(left.key, right.key)
    if first != left.key:
        return right, left, pair_key
    return left, right, pair_key
