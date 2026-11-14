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
  batch_id TEXT PRIMARY KEY, name TEXT, input_hash TEXT NOT NULL,
  ingest_hash TEXT, revision INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  total_lines INTEGER NOT NULL DEFAULT 0, valid_lines INTEGER NOT NULL DEFAULT 0,
  bad_lines INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'ingested'
);
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
-- Frozen record set observed by one run. A merge into the batch must never
-- change what an already-finished run reports, so each run stores the exact
-- record_pks it consumed. The run_snapshots row also exists for runs over an
-- empty record set, distinguishing "snapshot of zero records" from "run
-- created before snapshots were introduced" (legacy fallback to batch rows).
CREATE TABLE IF NOT EXISTS run_snapshots (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
  record_count INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run_records (
  run_id TEXT NOT NULL REFERENCES run_snapshots(run_id) ON DELETE CASCADE,
  record_pk INTEGER NOT NULL,
  PRIMARY KEY(run_id, record_pk)
);
CREATE INDEX IF NOT EXISTS idx_run_records_run ON run_records(run_id);
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
-- One row per successful batch merge. Kept separate from batches so the
-- existing /batches response shape is unchanged.
CREATE TABLE IF NOT EXISTS batch_merges (
  merge_id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  input_hash_before TEXT, input_hash_after TEXT NOT NULL,
  inserted INTEGER NOT NULL, duplicates INTEGER NOT NULL,
  dead_added INTEGER NOT NULL, total_lines INTEGER NOT NULL,
  valid_lines INTEGER NOT NULL, bad_lines INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_merges_batch ON batch_merges(batch_id);
"""


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class MergeInProgress(RuntimeError):
    """Raised when a second merge targets a batch that is already merging."""

    def __init__(self, batch_id: str) -> None:
        self.batch_id = batch_id
        super().__init__(f"a merge into batch {batch_id!r} is already in progress")


class Storage(RepositoryMixin):
    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.RLock()
        # Serializes batch merges per batch within this process. A merge holds
        # this guard for its whole read-check-write transaction so a concurrent
        # merge of the same batch fails fast with an explicit conflict instead
        # of blocking and silently applying a second time.
        self._merge_guard = threading.Lock()
        self._active_merges: set[str] = set()
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        with self.transaction() as conn:
            conn.executescript(SCHEMA)
            # Forward migration for databases created before batch merges.
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(batches)")}
            if "ingest_hash" not in columns:
                conn.execute("ALTER TABLE batches ADD COLUMN ingest_hash TEXT")
            if "revision" not in columns:
                conn.execute(
                    "ALTER TABLE batches ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
                )
            # Old batches: their input_hash was the payload hash, use it as the
            # remembered last-ingest hash so re-posting stays a no-op.
            conn.execute(
                "UPDATE batches SET ingest_hash=input_hash WHERE ingest_hash IS NULL"
            )
        # Runs from before snapshots existed predate any merge, so the batch's
        # current records are exactly what they saw. Freeze that now so a later
        # merge cannot change their answers.
        self.backfill_run_snapshots()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    @contextmanager
    def merge_transaction(self) -> Iterator[sqlite3.Connection]:
        """One write transaction for a whole batch merge, or nothing.

        ``BEGIN IMMEDIATE`` takes the write lock up front so a second writer
        (thread or another process on the same file) cannot read-then-write
        around us. Any exception -- including a validation conflict detected
        after rows have been staged -- rolls the entire merge back. A process
        killed mid-merge leaves only an uncommitted WAL transaction that
        SQLite discards on the next open, so records and batch counters revert
        to their pre-merge values.
        """
        with self._lock:
            if self.conn.in_transaction:
                # Defensive: never nest BEGIN. Should be unreachable because
                # every other write path commits before returning.
                self.conn.commit()
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def execute(self, sql: str, args: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        with self.transaction() as conn:
            return conn.execute(sql, args)

    def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(sql, args).fetchall())

    def query_one(self, sql: str, args: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(sql, args).fetchone()

    @contextmanager
    def batch_merge_guard(self, batch_id: str) -> Iterator[None]:
        """Fail fast if another merge for the same batch is in flight.

        The guard is acquired before the merge reads existing rows and is held
        until its single write transaction commits (or rolls back). A second
        concurrent merge therefore gets MergeInProgress instead of interleaving
        reads/writes and leaving a half-applied file behind. This is
        process-local by design: citefuse is a single-process local service.
        """
        with self._merge_guard:
            if batch_id in self._active_merges:
                raise MergeInProgress(batch_id)
            self._active_merges.add(batch_id)
        try:
            yield
        finally:
            with self._merge_guard:
                self._active_merges.discard(batch_id)

    # -- batches and records -------------------------------------------------
    def save_batch(self, batch: dict[str, Any], conn: sqlite3.Connection | None = None) -> None:
        sql = """INSERT INTO batches(batch_id,name,input_hash,ingest_hash,revision,
                 created_at,updated_at,total_lines,valid_lines,bad_lines,status)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(batch_id) DO UPDATE SET
                 name=COALESCE(excluded.name,batches.name),
                 input_hash=excluded.input_hash,
                 ingest_hash=COALESCE(excluded.ingest_hash,batches.ingest_hash),
                 revision=excluded.revision,
                 updated_at=excluded.updated_at,
                 total_lines=excluded.total_lines, valid_lines=excluded.valid_lines,
                 bad_lines=excluded.bad_lines, status=excluded.status"""
        args = (batch["batch_id"], batch.get("name"), batch["input_hash"],
                batch.get("ingest_hash"), int(batch.get("revision", 0)),
                batch["created_at"], batch["updated_at"],
                batch["total_lines"], batch["valid_lines"], batch["bad_lines"],
                batch.get("status", "ingested"))
        if conn is not None:
            conn.execute(sql, args)
            return
        self.execute(sql, args)

    def get_batch(self, batch_id: str) -> sqlite3.Row | None:
        return self.query_one("SELECT * FROM batches WHERE batch_id=?", (batch_id,))

    def load_batch_inputs(
        self, batch_id: str
    ) -> tuple[sqlite3.Row, list[sqlite3.Row], list[sqlite3.Row]] | None:
        """Read batch, records and decisions atomically for one pipeline run.

        A merge holds ``_lock`` for its whole write transaction, so this read
        cannot observe the batch hash from before a merge and the records from
        after it (or vice versa). That keeps a run's ``input_hash``, its
        in-memory record set and the snapshot it persists mutually consistent.
        """
        with self._lock:
            batch = self.conn.execute(
                "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if batch is None:
                return None
            records = list(
                self.conn.execute(
                    "SELECT * FROM records WHERE batch_id=? ORDER BY source,record_id",
                    (batch_id,),
                ).fetchall()
            )
            decisions = list(
                self.conn.execute(
                    """SELECT * FROM decisions WHERE batch_id=?
                       ORDER BY submitted_at,decision_id""",
                    (batch_id,),
                ).fetchall()
            )
            return batch, records, decisions

    def list_batches(self) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM batches ORDER BY created_at, batch_id")

    def insert_record(
        self, batch_id: str, row: dict[str, Any], conn: sqlite3.Connection | None = None
    ) -> bool:
        sql = """INSERT OR IGNORE INTO records(batch_id,source,record_id,line_no,
                 raw_json,norm_json,title_norm,authors_norm,venue_norm,year_norm,
                 volume_norm,issue_norm,pages_norm,doi_norm,gold_cluster)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        args = (batch_id, row["source"], row["record_id"], row["line_no"],
                json_dumps(row["raw"]), json_dumps(row["norm"]), row["title"],
                json_dumps(row["authors"]), row["venue"], row["year"], row["volume"],
                row["issue"], row["pages"], row["doi"], row["gold_cluster"])
        if conn is not None:
            return conn.execute(sql, args).rowcount == 1
        return self.execute(sql, args).rowcount == 1

    def list_records(self, batch_id: str) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM records WHERE batch_id=? ORDER BY source,record_id", (batch_id,)
        )

    def count_records(self, batch_id: str, conn: sqlite3.Connection | None = None) -> int:
        sql = "SELECT COUNT(*) AS n FROM records WHERE batch_id=?"
        row = (conn or self.conn).execute(sql, (batch_id,)).fetchone()
        return int(row["n"]) if row else 0

    def insert_dead_letter(
        self, batch_id: str, line_no: int, reason: str, raw: str,
        conn: sqlite3.Connection | None = None,
    ) -> bool:
        sql = """INSERT OR IGNORE INTO dead_letters(batch_id,line_no,reason,raw_text)
               VALUES(?,?,?,?)"""
        args = (batch_id, line_no, reason, raw)
        if conn is not None:
            return conn.execute(sql, args).rowcount == 1
        return self.execute(sql, args).rowcount == 1

    def count_dead_letters(
        self, batch_id: str, conn: sqlite3.Connection | None = None
    ) -> int:
        sql = "SELECT COUNT(*) AS n FROM dead_letters WHERE batch_id=?"
        row = (conn or self.conn).execute(sql, (batch_id,)).fetchone()
        return int(row["n"]) if row else 0

    def max_line_no(
        self, batch_id: str, conn: sqlite3.Connection | None = None
    ) -> int:
        sql = """SELECT MAX(line_no) AS n FROM (
                   SELECT line_no FROM records WHERE batch_id=?
                   UNION ALL
                   SELECT line_no FROM dead_letters WHERE batch_id=?)"""
        row = (conn or self.conn).execute(sql, (batch_id, batch_id)).fetchone()
        return int(row["n"] or 0) if row else 0

    def list_dead_letters(self, batch_id: str, limit: int = 100) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM dead_letters WHERE batch_id=? ORDER BY line_no LIMIT ?",
            (batch_id, limit),
        )

    def delete_batch(self, batch_id: str) -> bool:
        cursor = self.execute("DELETE FROM batches WHERE batch_id=?", (batch_id,))
        return cursor.rowcount > 0
