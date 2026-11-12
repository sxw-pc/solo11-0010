"""Execution context shared by every detector for one guard run."""
from __future__ import annotations

from dataclasses import dataclass

from .baseline import Baseline


@dataclass(frozen=True)
class DetectionContext:
    """The frozen baseline snapshot and thresholds used within one run."""

    baseline: Baseline | None = None
    min_samples: int = 20

    def has_baseline(self, field_name: str) -> bool:
        if self.baseline is None:
            return False
        return self.baseline.sample_count(field_name) >= self.min_samples
