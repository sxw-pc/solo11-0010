"""In-memory baseline statistics and their deterministic serialization.

A baseline captures the *accepted* historical convention per field. Three
statistic kinds are supported, matching the detector needs:

* ``numeric``     -- running count / sum / sum of squares (mean, std, range);
* ``categorical`` -- value -> observation count, bounded by a configured cap;
* ``text``        -- character n-gram -> count profile, likewise bounded.

Every statistic supports merging two independent observation sets so that
concurrent batches can fold into a new versioned snapshot without losing
samples. Feature capping is deterministic (highest count, then lexical key) so
two runs over the same data always produce byte-identical baselines.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

KIND_NUMERIC = "numeric"
KIND_CATEGORICAL = "categorical"
KIND_TEXT = "text"


@dataclass
class NumericStats:
    count: int = 0
    total: float = 0.0
    total_sq: float = 0.0
    minimum: float | None = None
    maximum: float | None = None

    def observe(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.total_sq += value * value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)

    def merge(self, other: "NumericStats") -> None:
        if other.count == 0:
            return
        self.count += other.count
        self.total += other.total
        self.total_sq += other.total_sq
        self.minimum = other.minimum if self.minimum is None else min(self.minimum, other.minimum)
        self.maximum = other.maximum if self.maximum is None else max(self.maximum, other.maximum)

    @property
    def mean(self) -> float | None:
        return self.total / self.count if self.count else None

    def stddev(self) -> float:
        """Population standard deviation; 0.0 when fewer than two samples."""
        if self.count < 2:
            return 0.0
        variance = max(self.total_sq / self.count - (self.total / self.count) ** 2, 0.0)
        return variance ** 0.5

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": KIND_NUMERIC,
            "count": self.count,
            "sum": round(self.total, 9),
            "sum_sq": round(self.total_sq, 9),
            "min": self.minimum,
            "max": self.maximum,
            "mean": round(self.mean, 9) if self.mean is not None else None,
            "stddev": round(self.stddev(), 9),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NumericStats":
        return cls(
            count=int(payload["count"]),
            total=float(payload["sum"]),
            total_sq=float(payload["sum_sq"]),
            minimum=payload.get("min"),
            maximum=payload.get("max"),
        )


@dataclass
class CountStats:
    """Categorical value or n-gram frequency table with a deterministic cap."""

    kind: str = KIND_CATEGORICAL
    count: int = 0
    counts: dict[str, int] = field(default_factory=dict)

    def observe(self, value: str, cap: int) -> None:
        self.count += 1
        self.counts[value] = self.counts.get(value, 0) + 1
        self._enforce_cap(cap)

    def merge(self, other: "CountStats", cap: int) -> None:
        self.count += other.count
        for value, occurrences in other.counts.items():
            self.counts[value] = self.counts.get(value, 0) + occurrences
        self._enforce_cap(cap)

    def _enforce_cap(self, cap: int) -> None:
        if len(self.counts) <= cap:
            return
        # Keep the most frequent features; ties break lexically for stability.
        ranked = sorted(self.counts.items(), key=lambda item: (-item[1], item[0]))
        self.counts = dict(ranked[:cap])

    def frequency(self, value: str) -> int:
        return self.counts.get(value, 0)

    def features(self) -> set[str]:
        return set(self.counts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "count": self.count,
            "counts": dict(sorted(self.counts.items())),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CountStats":
        return cls(
            kind=str(payload.get("kind", KIND_CATEGORICAL)),
            count=int(payload["count"]),
            counts={str(k): int(v) for k, v in payload["counts"].items()},
        )


FieldStat = NumericStats | CountStats


class Baseline:
    """An immutable-ish snapshot of per-field statistics at one version."""

    def __init__(self, version: int, stats: dict[str, FieldStat] | None = None) -> None:
        self.version = version
        self.stats: dict[str, FieldStat] = stats or {}

    # -- reads used by detectors ---------------------------------------------
    def numeric(self, field_name: str) -> NumericStats | None:
        stat = self.stats.get(field_name)
        return stat if isinstance(stat, NumericStats) else None

    def categorical(self, field_name: str) -> CountStats | None:
        stat = self.stats.get(field_name)
        if isinstance(stat, CountStats) and stat.kind == KIND_CATEGORICAL:
            return stat
        return None

    def text_profile(self, field_name: str) -> CountStats | None:
        stat = self.stats.get(field_name)
        if isinstance(stat, CountStats) and stat.kind == KIND_TEXT:
            return stat
        return None

    def sample_count(self, field_name: str) -> int:
        stat = self.stats.get(field_name)
        return stat.count if stat is not None else 0

    # -- (de)serialization ----------------------------------------------------
    def to_json(self) -> str:
        return json.dumps(
            {name: stat.to_dict() for name, stat in sorted(self.stats.items())},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, version: int, raw: str) -> "Baseline":
        payload = json.loads(raw)
        stats: dict[str, FieldStat] = {}
        for name, body in payload.items():
            if body.get("kind") == KIND_NUMERIC:
                stats[name] = NumericStats.from_dict(body)
            else:
                stats[name] = CountStats.from_dict(body)
        return cls(version, stats)


def char_ngrams(value: str, size: int) -> set[str]:
    """Character n-gram set over a whitespace-collapsed string."""
    text = " ".join(str(value).casefold().split())
    if len(text) < size:
        return {text} if text else set()
    return {text[index:index + size] for index in range(len(text) - size + 1)}
