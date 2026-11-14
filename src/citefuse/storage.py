"""SQLite persistence core: connections, idempotent DDL and record storage.

All SQL is parameterized. The single app-wide connection is guarded by a
re-entrant lock so FastAPI's worker threads can share it safely. Run, pair,
cluster and review queries live in repository.py.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .repository import RepositoryMixin

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY, name TEXT, input_hash TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  total_lines INTEGER NOT NULL DEFAULT 0, valid_lines INTEGER NOT NULL DEFAULT 0,
  bad_lines INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'ingested'
);
CREATE INDEX IF NOT EXISTS idx_batches_hash ON batches(input_hash);
CREATE TABLE IF NOT EXISTS records (
  record_pk INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  source TEXT NOT NULL, record_id TEXT NOT NULL, line_no INTEGER NOT NULL,
  raw_json TEXT NOT NULL, norm_json TEXT NOT NULL,
  title_norm TEXT NOT NULL, authors_norm TEXT NOT NULL,
  venue_norm TEXT, year_norm INTEGER, volume_norm TEXT, issue_norm TEXT,
  pages_norm TEXT, doi_norm TEXT, gold_cluster TEXT,
  UNIQUE(batch_id, source, record_id)
);
CREATE INDEX IF NOT EXISTS idx_records_batch ON records(batch_id);
CREATE TABLE IF NOT EXISTS dead_letters (
  dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  line_no INTEGER NOT NULL, reason TEXT NOT NULL, raw_text TEXT NOT NULL,
  UNIQUE(batch_id, line_no, reason, raw_text)
);
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  config_hash TEXT NOT NULL, input_rows INTEGER NOT NULL,
  bad_rows INTEGER NOT NULL, candidate_pairs INTEGER NOT NULL,
  matched_edges INTEGER NOT NULL, cluster_count INTEGER NOT NULL,
  golden_count INTEGER NOT NULL, timings_json TEXT NOT NULL,
  blocking_stats_json TEXT NOT NULL, status TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_batch ON runs(batch_id);
CREATE TABLE IF NOT EXISTS run_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  batch_id TEXT NOT NULL, config_hash TEXT NOT NULL,
  input_rows INTEGER NOT NULL, bad_rows INTEGER NOT NULL,
  candidate_pairs INTEGER NOT NULL, matched_edges INTEGER NOT NULL,
  cluster_count INTEGER NOT NULL, golden_count INTEGER NOT NULL,
  timings_json TEXT NOT NULL, status TEXT NOT NULL,
  started_at TEXT NOT NULL, finished_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pairs (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL, rec_a_pk INTEGER NOT NULL, rec_b_pk INTEGER NOT NULL,
  score REAL NOT NULL, zone TEXT NOT NULL, strategies_json TEXT NOT NULL,
  details_json TEXT NOT NULL, PRIMARY KEY(run_id, pair_key)
);
CREATE INDEX IF NOT EXISTS idx_pairs_zone ON pairs(run_id, zone);
CREATE TABLE IF NOT EXISTS edges (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL, rec_a_pk INTEGER NOT NULL, rec_b_pk INTEGER NOT NULL,
  score REAL NOT NULL, kind TEXT NOT NULL, details_json TEXT NOT NULL,
  PRIMARY KEY(run_id, pair_key)
);
CREATE TABLE IF NOT EXISTS clusters (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  cluster_id TEXT NOT NULL, batch_id TEXT NOT NULL,
  member_keys_json TEXT NOT NULL, member_pks_json TEXT NOT NULL,
  golden_json TEXT NOT NULL, PRIMARY KEY(run_id, cluster_id)
);
CREATE TABLE IF NOT EXISTS decisions (
  decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL, rec_a_key TEXT NOT NULL, rec_b_key TEXT NOT NULL,
  decision TEXT NOT NULL, reviewer TEXT NOT NULL, note TEXT NOT NULL,
  submitted_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_batch ON decisions(batch_id, pair_key);
CREATE TABLE IF NOT EXISTS audit_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT, batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  event_type TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_run ON audit_events(run_id);
"""


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Storage(RepositoryMixin):
    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.RLock()
        self._tx_depth = 0
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        # Wait for a competing writer (e.g. a second process) instead of
        # failing immediately with "database is locked".
        self.conn.execute("PRAGMA busy_timeout=5000")
        with self.transaction() as conn:
            conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Re-entrant unit of work guarded by the app-wide lock.

        Nested ``transaction()``/``execute()`` calls join the outer unit of
        work: only the outermost level commits on success or rolls back on
        error. An exception escaping a nested level propagates without touching
        the transaction state, so callers may catch it (SQLite has already
        rolled the failed statement back) and continue the outer unit.
        """
        with self._lock:
            self._tx_depth += 1
            try:
                yield self.conn
                if self._tx_depth == 1:
                    self.conn.commit()
            except Exception:
                if self._tx_depth == 1:
                    self.conn.rollback()
                raise
            finally:
                self._tx_depth -= 1

    def execute(self, sql: str, args: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        with self.transaction() as conn:
            return conn.execute(sql, args)

    def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(sql, args).fetchall())

    def query_one(self, sql: str, args: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(sql, args).fetchone()

    # -- batches and records -------------------------------------------------
    def save_batch(self, batch: dict[str, Any]) -> None:
        self.execute(
            """INSERT INTO batches(batch_id,name,input_hash,created_at,updated_at,
                 total_lines,valid_lines,bad_lines,status)
               VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(batch_id) DO UPDATE SET
                 name=COALESCE(excluded.name,batches.name),
                 input_hash=excluded.input_hash, updated_at=excluded.updated_at,
                 total_lines=excluded.total_lines, valid_lines=excluded.valid_lines,
                 bad_lines=excluded.bad_lines, status=excluded.status""",
            (batch["batch_id"], batch.get("name"), batch["input_hash"],
             batch["created_at"], batch["updated_at"], batch["total_lines"],
             batch["valid_lines"], batch["bad_lines"], batch.get("status", "ingested")),
        )

    def get_batch(self, batch_id: str) -> sqlite3.Row | None:
        return self.query_one("SELECT * FROM batches WHERE batch_id=?", (batch_id,))

    def get_batch_by_hash(self, input_hash: str) -> sqlite3.Row | None:
        """Return the batch stored for identical content, if one exists."""
        return self.query_one(
            "SELECT * FROM batches WHERE input_hash=?"
            " ORDER BY created_at, batch_id LIMIT 1",
            (input_hash,),
        )

    def list_batches(self) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM batches ORDER BY created_at, batch_id")

    def insert_record(self, batch_id: str, row: dict[str, Any]) -> bool:
        cursor = self.execute(
            """INSERT OR IGNORE INTO records(batch_id,source,record_id,line_no,
                 raw_json,norm_json,title_norm,authors_norm,venue_norm,year_norm,
                 volume_norm,issue_norm,pages_norm,doi_norm,gold_cluster)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (batch_id, row["source"], row["record_id"], row["line_no"],
             json_dumps(row["raw"]), json_dumps(row["norm"]), row["title"],
             json_dumps(row["authors"]), row["venue"], row["year"], row["volume"],
             row["issue"], row["pages"], row["doi"], row["gold_cluster"]),
        )
        return cursor.rowcount == 1

    def list_records(self, batch_id: str) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM records WHERE batch_id=? ORDER BY source,record_id", (batch_id,)
        )

    def count_records(self, batch_id: str) -> int:
        row = self.query_one("SELECT COUNT(*) AS n FROM records WHERE batch_id=?", (batch_id,))
        return int(row["n"]) if row else 0

    def insert_dead_letter(self, batch_id: str, line_no: int, reason: str, raw: str) -> None:
        self.execute(
            """INSERT OR IGNORE INTO dead_letters(batch_id,line_no,reason,raw_text)
               VALUES(?,?,?,?)""",
            (batch_id, line_no, reason, raw),
        )

    def count_dead_letters(self, batch_id: str) -> int:
        row = self.query_one(
            "SELECT COUNT(*) AS n FROM dead_letters WHERE batch_id=?", (batch_id,)
        )
        return int(row["n"]) if row else 0

    def list_dead_letters(self, batch_id: str, limit: int = 100) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM dead_letters WHERE batch_id=? ORDER BY line_no LIMIT ?",
            (batch_id, limit),
        )

    def delete_batch(self, batch_id: str) -> bool:
        cursor = self.execute("DELETE FROM batches WHERE batch_id=?", (batch_id,))
        return cursor.rowcount > 0
