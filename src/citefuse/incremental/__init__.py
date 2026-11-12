"""Incremental fusion module.

A standalone add-on that fuses newly ingested records against the results of a
previous (full or incremental) run without touching the existing full-run core
logic. Public entry points:

- :func:`run_incremental` / :class:`IncrementalEngine` -- one incremental task.
- :class:`IncrementalError` -- validation/precondition failures.
- :class:`IncrementalBusy` -- another incremental task is currently running.

The module only *reads* historical runs and *appends* new runs; every row
belonging to a previous run is immutable and remains independently
reproducible.
"""
from __future__ import annotations

from .engine import IncrementalBusy, IncrementalEngine, IncrementalError, run_incremental
from .schema import ensure_incremental_schema

__all__ = [
    "IncrementalBusy",
    "IncrementalEngine",
    "IncrementalError",
    "ensure_incremental_schema",
    "run_incremental",
]
