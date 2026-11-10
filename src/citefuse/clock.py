"""Injectable clocks.

All persisted timestamps and measured durations come from a Clock so tests can
freeze time and obtain byte-stable output.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...

    def now_iso(self) -> str: ...

    def monotonic(self) -> float: ...


class SystemClock:
    """Wall clock plus a monotonic timer used for stage durations."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def now_iso(self) -> str:
        return self.now().isoformat(timespec="seconds").replace("+00:00", "Z")

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
        return self._moment.isoformat(timespec="seconds").replace("+00:00", "Z")

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        self._monotonic += seconds
