"""SQLite persistence for drift baselines, dispositions and quarantine.

The drift tables live alongside the engine tables in the same SQLite file but
are created and managed solely by this module, keeping the feature separable.

Concurrency model
-----------------
The app-wide connection already serializes Python access with an RLock. To
also stay correct across *processes* (e.g. two batch workers pointing at the
same file), baseline promotion takes an explicit ``BEGIN IMMEDIATE`` write
transaction. Detection reads the latest committed snapshot, so a reader never
observes a half-merged baseline and concurrent promotions are applied one at a
time against the freshest version. Every promotion appends a new immutable
version row; audit trails can always reproduce which version produced a
decision.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from ..storage import json_dumps
from .baseline import Baseline, FieldStat

DRIFT_SCHEMA = """
CREATE TABLE IF NOT EXISTS drift_baselines (
  baseline_id INTEGER PRIMARY KEY AUTOINCREMENT,
  version INTEGER NOT NULL UNIQUE,
  stats_json TEXT NOT NULL,
  source_batch_id TEXT,
  absorbed_records INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS drift_runs (
  drift_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  baseline_version INTEGER NOT NULL,
  summary_json TEXT NOT NULL,
  degraded INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  UNIQUE(run_id)
);
CREATE INDEX IF NOT EXISTS idx_drift_runs_batch ON drift_runs(batch_id);
CREATE TABLE IF NOT EXISTS drift_dispositions (
  disposition_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  record_key TEXT NOT NULL,
  record_pk INTEGER,
  tier TEXT NOT NULL,
  score REAL NOT NULL,
  action TEXT NOT NULL,
  baseline_version INTEGER NOT NULL,
  corrected_json TEXT NOT NULL,
  verdicts_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(run_id, record_key)
);
CREATE INDEX IF NOT EXISTS idx_disp_batch_tier ON drift_dispositions(batch_id, tier);
CREATE TABLE IF NOT EXISTS drift_quarantine (
  quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  record_key TEXT NOT NULL,
  record_pk INTEGER,
  tier TEXT NOT NULL,
  score REAL NOT NULL,
  reasons_json TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  norm_json TEXT NOT NULL,
  baseline_version INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(run_id, record_key)
);
CREATE INDEX IF NOT EXISTS idx_quar_batch ON drift_quarantine(batch_id);
"""


class DriftStore:
    """Versioned baseline + disposition persistence over an engine Storage."""

    def __init__(self, storage: Any) -> None:
        self.storage = storage
        with storage.transaction() as conn:
            conn.executescript(DRIFT_SCHEMA)

    # -- baseline reads -------------------------------------------------------
    def latest_baseline_row(self) -> sqlite3.Row | None:
        return self.storage.query_one(
            "SELECT * FROM drift_baselines ORDER BY version DESC LIMIT 1"
        )

    def load_latest_baseline(self) -> Baseline | None:
        row = self.latest_baseline_row()
        if row is None:
            return None
        return Baseline.from_json(int(row["version"]), row["stats_json"])

    def list_baseline_versions(self) -> list[dict[str, Any]]:
        rows = self.storage.query(
            "SELECT version,source_batch_id,absorbed_records,created_at "
            "FROM drift_baselines ORDER BY version"
        )
        return [dict(row) for row in rows]

    # -- baseline promotion (concurrency safe) --------------------------------
    def promote_baseline(
        self,
        stats: dict[str, FieldStat],
        source_batch_id: str | None,
        absorbed_records: int,
        created_at: str,
    ) -> int:
        """Atomically append a new baseline version.

        Takes an immediate write lock and recomputes the next version from the
        freshest committed row inside the same transaction, so two concurrent
        promoters can never allocate the same version or clobber each other.
        """
        with self.storage._lock:
            conn = self.storage.conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT COALESCE(MAX(version), 0) AS v FROM drift_baselines"
                ).fetchone()
                next_version = int(row["v"]) + 1
                snapshot = Baseline(next_version, stats)
                conn.execute(
                    """INSERT INTO drift_baselines(version,stats_json,source_batch_id,
                         absorbed_records,created_at) VALUES(?,?,?,?,?)""",
                    (next_version, snapshot.to_json(), source_batch_id,
                     absorbed_records, created_at),
                )
                conn.commit()
                return next_version
            except Exception:
                conn.rollback()
                raise

    def reset_baseline(self) -> None:
        """Remove every baseline version (explicit operator action)."""
        self.storage.execute("DELETE FROM drift_baselines")

    # -- run summaries --------------------------------------------------------
    def save_drift_run(
        self, run_id: str, batch_id: str, baseline_version: int,
        summary: dict[str, Any], degraded: bool, created_at: str,
    ) -> None:
        self.storage.execute(
            """INSERT INTO drift_runs(run_id,batch_id,baseline_version,summary_json,
                 degraded,created_at) VALUES(?,?,?,?,?,?)
               ON CONFLICT(run_id) DO UPDATE SET
                 baseline_version=excluded.baseline_version,
                 summary_json=excluded.summary_json,
                 degraded=excluded.degraded""",
            (run_id, batch_id, baseline_version, json_dumps(summary),
             1 if degraded else 0, created_at),
        )

    def persist_run_artifacts(
        self,
        run_id: str,
        batch_id: str,
        disposition_rows: list[dict[str, Any]],
        quarantine_items: list[dict[str, Any]],
        summary: dict[str, Any],
        baseline_version: int,
        degraded: bool,
        created_at: str,
    ) -> None:
        """Persist dispositions, quarantine and the run summary in ONE txn.

        Collapsing the writes into a single commit avoids repeated fsync cost
        while keeping every artifact atomic: a reader sees all of them or none.
        """
        with self.storage.transaction() as conn:
            conn.execute(
                "DELETE FROM drift_dispositions WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO drift_dispositions(run_id,batch_id,record_key,record_pk,
                     tier,score,action,baseline_version,corrected_json,verdicts_json,
                     created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (run_id, batch_id, item["record_key"], item.get("record_pk"),
                     item["tier"], item["score"], item["action"],
                     item["baseline_version"], json_dumps(item["corrected_fields"]),
                     json_dumps(item["verdicts"]), created_at)
                    for item in disposition_rows
                ],
            )
            conn.execute("DELETE FROM drift_quarantine WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO drift_quarantine(run_id,batch_id,record_key,record_pk,
                     tier,score,reasons_json,raw_json,norm_json,baseline_version,
                     created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (run_id, batch_id, item["record_key"], item.get("record_pk"),
                     item["tier"], item["score"], json_dumps(item["reasons"]),
                     json_dumps(item["raw"]), json_dumps(item["norm"]),
                     item["baseline_version"], created_at)
                    for item in quarantine_items
                ],
            )
            conn.execute(
                """INSERT INTO drift_runs(run_id,batch_id,baseline_version,summary_json,
                     degraded,created_at) VALUES(?,?,?,?,?,?)
                   ON CONFLICT(run_id) DO UPDATE SET
                     baseline_version=excluded.baseline_version,
                     summary_json=excluded.summary_json,
                     degraded=excluded.degraded""",
                (run_id, batch_id, baseline_version, json_dumps(summary),
                 1 if degraded else 0, created_at),
            )

    def get_drift_run(self, run_id: str) -> dict[str, Any] | None:
        row = self.storage.query_one(
            "SELECT * FROM drift_runs WHERE run_id=?", (run_id,)
        )
        if row is None:
            return None
        import json

        result = dict(row)
        result["summary"] = json.loads(result.pop("summary_json"))
        result["degraded"] = bool(result["degraded"])
        return result

    def list_drift_runs(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id:
            rows = self.storage.query(
                "SELECT * FROM drift_runs WHERE batch_id=? ORDER BY drift_run_id",
                (batch_id,),
            )
        else:
            rows = self.storage.query("SELECT * FROM drift_runs ORDER BY drift_run_id")
        import json

        results = []
        for row in rows:
            item = dict(row)
            item["summary"] = json.loads(item.pop("summary_json"))
            item["degraded"] = bool(item["degraded"])
            results.append(item)
        return results

    # -- dispositions ----------------------------------------------------------
    def replace_dispositions(
        self, run_id: str, batch_id: str, results: list[dict[str, Any]], created_at: str
    ) -> None:
        with self.storage.transaction() as conn:
            conn.execute("DELETE FROM drift_dispositions WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO drift_dispositions(run_id,batch_id,record_key,record_pk,
                     tier,score,action,baseline_version,corrected_json,verdicts_json,
                     created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (run_id, batch_id, item["record_key"], item.get("record_pk"),
                     item["tier"], item["score"], item["action"],
                     item["baseline_version"], json_dumps(item["corrected_fields"]),
                     json_dumps(item["verdicts"]), created_at)
                    for item in results
                ],
            )

    def list_dispositions(
        self, batch_id: str, tier: str | None = None, limit: int = 1000
    ) -> list[dict[str, Any]]:
        import json

        if tier:
            rows = self.storage.query(
                "SELECT * FROM drift_dispositions WHERE batch_id=? AND tier=? "
                "ORDER BY disposition_id LIMIT ?",
                (batch_id, tier, limit),
            )
        else:
            rows = self.storage.query(
                "SELECT * FROM drift_dispositions WHERE batch_id=? "
                "ORDER BY disposition_id LIMIT ?",
                (batch_id, limit),
            )
        output = []
        for row in rows:
            item = dict(row)
            item["corrected_fields"] = json.loads(item.pop("corrected_json"))
            item["verdicts"] = json.loads(item.pop("verdicts_json"))
            output.append(item)
        return output

    def disposition_keys_for_run(self, run_id: str) -> set[str]:
        rows = self.storage.query(
            "SELECT record_key FROM drift_dispositions WHERE run_id=?", (run_id,)
        )
        return {row["record_key"] for row in rows}

    # -- quarantine -------------------------------------------------------------
    def replace_quarantine(
        self, run_id: str, batch_id: str, items: list[dict[str, Any]], created_at: str
    ) -> None:
        with self.storage.transaction() as conn:
            conn.execute("DELETE FROM drift_quarantine WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO drift_quarantine(run_id,batch_id,record_key,record_pk,
                     tier,score,reasons_json,raw_json,norm_json,baseline_version,
                     created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (run_id, batch_id, item["record_key"], item.get("record_pk"),
                     item["tier"], item["score"], json_dumps(item["reasons"]),
                     json_dumps(item["raw"]), json_dumps(item["norm"]),
                     item["baseline_version"], created_at)
                    for item in items
                ],
            )

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        import json

        if batch_id:
            rows = self.storage.query(
                "SELECT * FROM drift_quarantine WHERE batch_id=? ORDER BY quarantine_id",
                (batch_id,),
            )
        else:
            rows = self.storage.query(
                "SELECT * FROM drift_quarantine ORDER BY quarantine_id"
            )
        output = []
        for row in rows:
            item = dict(row)
            item["reasons"] = json.loads(item.pop("reasons_json"))
            item["raw"] = json.loads(item.pop("raw_json"))
            item["norm"] = json.loads(item.pop("norm_json"))
            output.append(item)
        return output

    def quarantine_keys_for_run(self, run_id: str) -> set[str]:
        rows = self.storage.query(
            "SELECT record_key FROM drift_quarantine WHERE run_id=?", (run_id,)
        )
        return {row["record_key"] for row in rows}
