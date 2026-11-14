"""Injectable clocks and the persisted-timestamp contract.

All persisted timestamps and measured durations come from a Clock so tests can
freeze time and obtain byte-stable output.

Timestamp contract
------------------
Every timestamp persisted to SQLite (``batches.created_at``/``updated_at``,
``runs.created_at``, ``run_attempts.started_at``/``finished_at``,
``decisions.submitted_at``, ``audit_events.created_at``) is stored as a
canonical UTC string: ``YYYY-MM-DDTHH:MM:SSZ`` — ISO 8601, second precision,
``Z`` suffix, always exactly 20 characters. Because the width and precision
are fixed, lexicographic ordering equals chronological ordering, so SQL
``ORDER BY`` and plain string comparison stay meaningful, and golden exports
remain byte-stable.

``datetime.utcnow()`` (deprecated since Python 3.12) is never used; aware
``datetime.now(timezone.utc)`` is the only wall-clock source. Legacy rows
written by pre-contract versions (naive ``utcnow().isoformat()`` strings,
``+00:00`` offsets, fractional seconds, space separators) are normalized to
the canonical form once by the startup migration in ``storage.py``; naive
values are interpreted as UTC, matching the legacy ``utcnow()`` semantics.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Protocol

#: The canonical persisted form: exactly ``YYYY-MM-DDTHH:MM:SSZ``.
CANONICAL_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def is_canonical_timestamp(value: str) -> bool:
    """Return True if *value* already satisfies the persisted contract."""
    return CANONICAL_TIMESTAMP_RE.fullmatch(value) is not None


def format_timestamp(moment: datetime) -> str:
    """Format *moment* in the canonical persisted form (UTC, seconds, ``Z``).

    Naive input is interpreted as UTC; sub-second precision is truncated,
    matching ``isoformat(timespec="seconds")`` semantics.
    """
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return (
        moment.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def normalize_timestamp(value: str) -> str | None:
    """Best-effort conversion of a legacy persisted timestamp to the contract.

    Returns the canonical form, or ``None`` when *value* is not a parseable
    ISO 8601 timestamp (callers must leave such values untouched). Already
    canonical values are returned unchanged, which keeps the storage-layer
    migration idempotent.
    """
    if is_canonical_timestamp(value):
        return value
    try:
        parsed = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
    return format_timestamp(parsed)


class Clock(Protocol):
    def now(self) -> datetime: ...

    def now_iso(self) -> str: ...

    def monotonic(self) -> float: ...


class SystemClock:
    """Wall clock plus a monotonic timer used for stage durations."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def now_iso(self) -> str:
        return format_timestamp(self.now())

    def monotonic(self) -> float:
        import time

        return time.perf_counter()


class FixedClock:
    """Deterministic clock. The monotonic value can be advanced explicitly."""

    def __init__(self, moment: datetime, monotonic: float = 0.0) -> None:
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        self._moment = moment.astimezone(timezone.utc)
        self._monotonic = monotonic

    def now(self) -> datetime:
        return self._moment

    def now_iso(self) -> str:
        return format_timestamp(self._moment)

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        self._monotonic += seconds
