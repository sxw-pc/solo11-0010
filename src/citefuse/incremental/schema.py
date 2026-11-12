"""Storage additions used exclusively by the incremental module.

The tables created here are *additive*: the existing full-run schema in
``storage.py`` is not modified, so historical runs and their snapshots are
untouched. All tables are created with ``CREATE TABLE IF NOT EXISTS`` which is
idempotent across processes and re-runs.

Tables
------
incremental_tasks
    One row per submitted incremental task: its reproducible task id, the
    baseline run it was fused against, the produced run id, delta statistics
    (same metric vocabulary as ``runs``) and lifecycle status.

incremental_locks
    A single-row advisory lock. A ``UNIQUE`` constraint makes concurrent
    incremental tasks fail at the database layer (works across processes),
    while an in-process lock additionally serializes threads inside one app.
"""
from __future__ import annotations

from typing import Any

INCREMENTAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS incremental_tasks (
  task_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  base_run_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  new_records INTEGER NOT NULL DEFAULT 0,
  new_pairs INTEGER NOT NULL DEFAULT 0,
  new_model_edges INTEGER NOT NULL DEFAULT 0,
  reused_pairs INTEGER NOT NULL DEFAULT 0,
  reused_clusters INTEGER NOT NULL DEFAULT 0,
  rebuilt_clusters INTEGER NOT NULL DEFAULT 0,
  idempotent_replay INTEGER NOT NULL DEFAULT 0,
  timings_json TEXT NOT NULL,
  blocking_stats_json TEXT NOT NULL,
  error TEXT,
  created_at TEXT NOT NULL,
  finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_incr_tasks_batch ON incremental_tasks(batch_id);
CREATE TABLE IF NOT EXISTS incremental_locks (
  lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1),
  task_id TEXT NOT NULL,
  acquired_at TEXT NOT NULL
);
"""


def ensure_incremental_schema(storage: Any) -> None:
    """Idempotently create the incremental-only tables."""
    with storage.transaction() as conn:
        conn.executescript(INCREMENTAL_SCHEMA)
