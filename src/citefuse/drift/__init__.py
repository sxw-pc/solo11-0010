"""Field-level semantic drift guard for the citefuse ingestion pipeline.

The guard sits between record loading and blocking in the core pipeline. It
learns per-field conventions from historically accepted records, validates
every incoming field against that baseline plus explicit semantic rules, and
disposes of drift in three tiers:

* minor  -- deterministic auto-correction, then pass;
* medium -- tagged and routed to a low-priority lane;
* heavy  -- isolated in dedicated quarantine storage, never clustered.

All rules and thresholds are YAML-driven; every decision is audited; any
internal failure degrades to "detection skipped" without blocking the engine.
"""
from __future__ import annotations

from .config import (
    ACTION_BY_TIER, TIER_ORDER, TIER_RANK, AuthorDriftConfig, BaselineConfig,
    DriftConfig, GenericDriftConfig, SeverityConfig, SourceDriftConfig,
    YearDriftConfig,
)
from .context import DetectionContext
from .detectors import (
    AuthorDetector, GenericDetector, SourceDetector, YearDetector, build_detectors,
)
from .guard import DriftGuard
from .models import DriftSummary, FieldVerdict, RecordDriftResult
from .store import DriftStore

__all__ = [
    "ACTION_BY_TIER", "TIER_ORDER", "TIER_RANK",
    "AuthorDriftConfig", "BaselineConfig", "DriftConfig", "GenericDriftConfig",
    "SeverityConfig", "SourceDriftConfig", "YearDriftConfig",
    "DetectionContext",
    "AuthorDetector", "GenericDetector", "SourceDetector", "YearDetector",
    "build_detectors",
    "DriftGuard", "DriftStore",
    "DriftSummary", "FieldVerdict", "RecordDriftResult",
]
