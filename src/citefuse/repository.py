"""Repository mixin for pipeline runs, candidate pairs, clusters and reviews."""
from __future__ import annotations

import json
import sqlite3
from typing import Any


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class RepositoryMixin:
    """Data-access methods composed into Storage.

    The mixin relies on Storage.execute/query/query_one/transaction but does
    not import Storage, keeping the split free of circular imports.
    """

    def save_run(self, run: dict[str, Any]) -> None:
        self.execute(
            """INSERT INTO runs(run_id,batch_id,config_hash,input_rows,bad_rows,
                 candidate_pairs,matched_edges,cluster_count,golden_count,
                 timings_json,blocking_stats_json,status,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id) DO UPDATE SET
                 input_rows=excluded.input_rows,bad_rows=excluded.bad_rows,
                 candidate_pairs=excluded.candidate_pairs,
                 matched_edges=excluded.matched_edges,
                 cluster_count=excluded.cluster_count,golden_count=excluded.golden_count,
                 timings_json=excluded.timings_json,
                 blocking_stats_json=excluded.blocking_stats_json,
                 status=excluded.status""",
            (run["run_id"], run["batch_id"], run["config_hash"], run["input_rows"],
             run["bad_rows"], run["candidate_pairs"], run["matched_edges"],
             run["cluster_count"], run["golden_count"], _dumps(run["timings"]),
             _dumps(run["blocking_stats"]), run["status"], run["created_at"]),
        )

    def add_attempt(self, attempt: dict[str, Any]) -> None:
        self.execute(
            """INSERT INTO run_attempts(run_id,batch_id,config_hash,input_rows,
                 bad_rows,candidate_pairs,matched_edges,cluster_count,golden_count,
                 timings_json,status,started_at,finished_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (attempt["run_id"], attempt["batch_id"], attempt["config_hash"],
             attempt["input_rows"], attempt["bad_rows"], attempt["candidate_pairs"],
             attempt["matched_edges"], attempt["cluster_count"], attempt["golden_count"],
             _dumps(attempt["timings"]), attempt["status"], attempt["started_at"],
             attempt["finished_at"]),
        )

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        return self.query_one("SELECT * FROM runs WHERE run_id=?", (run_id,))

    def latest_attempt(self, run_id: str) -> sqlite3.Row | None:
        return self.query_one(
            "SELECT * FROM run_attempts WHERE run_id=? ORDER BY attempt_id DESC LIMIT 1",
            (run_id,),
        )

    def list_runs(self, batch_id: str | None = None) -> list[sqlite3.Row]:
        if batch_id:
            return self.query(
                "SELECT * FROM runs WHERE batch_id=? ORDER BY created_at,run_id", (batch_id,)
            )
        return self.query("SELECT * FROM runs ORDER BY created_at,run_id")

    def latest_run_id(self, batch_id: str) -> str | None:
        row = self.query_one(
            "SELECT run_id FROM run_attempts WHERE batch_id=? ORDER BY attempt_id DESC LIMIT 1",
            (batch_id,),
        )
        return str(row["run_id"]) if row else None

    def add_audit_event(
        self, batch_id: str, run_id: str | None, event_type: str, detail: Any, created_at: str
    ) -> None:
        self.execute(
            "INSERT INTO audit_events(run_id,batch_id,event_type,detail_json,created_at)"
            " VALUES(?,?,?,?,?)",
            (run_id, batch_id, event_type, _dumps(detail), created_at),
        )

    def list_audit_events(self, run_id: str) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM audit_events WHERE run_id=? ORDER BY event_id", (run_id,)
        )

    def replace_pairs(self, run_id: str, pairs: list[dict[str, Any]]) -> None:
        with self.transaction() as conn:
            conn.execute("DELETE FROM pairs WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO pairs(run_id,pair_key,rec_a_pk,rec_b_pk,score,zone,
                     strategies_json,details_json) VALUES(?,?,?,?,?,?,?,?)""",
                [(run_id, p["pair_key"], p["rec_a_pk"], p["rec_b_pk"], p["score"],
                  p["zone"], _dumps(p["strategies"]), _dumps(p["details"]))
                 for p in pairs],
            )

    def list_pairs(self, run_id: str, zone: str | None = None) -> list[sqlite3.Row]:
        if zone:
            return self.query(
                "SELECT * FROM pairs WHERE run_id=? AND zone=? ORDER BY pair_key",
                (run_id, zone),
            )
        return self.query("SELECT * FROM pairs WHERE run_id=? ORDER BY pair_key", (run_id,))

    def get_pair(self, run_id: str, pair_key: str) -> sqlite3.Row | None:
        return self.query_one(
            "SELECT * FROM pairs WHERE run_id=? AND pair_key=?", (run_id, pair_key)
        )

    def replace_edges(self, run_id: str, edges: list[dict[str, Any]]) -> None:
        with self.transaction() as conn:
            conn.execute("DELETE FROM edges WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO edges(run_id,pair_key,rec_a_pk,rec_b_pk,score,kind,
                     details_json) VALUES(?,?,?,?,?,?,?)""",
                [(run_id, e["pair_key"], e["rec_a_pk"], e["rec_b_pk"], e["score"],
                  e["kind"], _dumps(e.get("details", {}))) for e in edges],
            )

    def list_edges(self, run_id: str) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM edges WHERE run_id=? ORDER BY pair_key", (run_id,)
        )

    def replace_clusters(self, run_id: str, batch_id: str, clusters: list[dict[str, Any]]) -> None:
        with self.transaction() as conn:
            conn.execute("DELETE FROM clusters WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO clusters(run_id,cluster_id,batch_id,member_keys_json,
                     member_pks_json,golden_json) VALUES(?,?,?,?,?,?)""",
                [(run_id, c["cluster_id"], batch_id, _dumps(c["member_keys"]),
                  _dumps(c["member_pks"]), _dumps(c["golden"])) for c in clusters],
            )

    def list_clusters(self, run_id: str) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM clusters WHERE run_id=? ORDER BY cluster_id", (run_id,)
        )

    def get_cluster(self, run_id: str, cluster_id: str) -> sqlite3.Row | None:
        return self.query_one(
            "SELECT * FROM clusters WHERE run_id=? AND cluster_id=?", (run_id, cluster_id)
        )

    # -- frozen run record snapshots ----------------------------------------
    def replace_run_snapshot(
        self, run_id: str, record_pks: list[int], created_at: str
    ) -> None:
        """Persist the exact record set a run consumed, keyed by run_id.

        Re-running the same logical run_id (same batch content, config and
        decisions) rewrites an identical snapshot. A merge into the batch later
        cannot change it.
        """
        with self.transaction() as conn:
            self._write_run_snapshot(conn, run_id, record_pks, created_at)

    def _write_run_snapshot(
        self, conn: Any, run_id: str, record_pks: list[int], created_at: str
    ) -> None:
        conn.execute("DELETE FROM run_records WHERE run_id=?", (run_id,))
        conn.execute(
            """INSERT INTO run_snapshots(run_id,record_count,created_at)
               VALUES(?,?,?)
               ON CONFLICT(run_id) DO UPDATE SET
                 record_count=excluded.record_count, created_at=excluded.created_at""",
            (run_id, len(record_pks), created_at),
        )
        conn.executemany(
            "INSERT OR IGNORE INTO run_records(run_id,record_pk) VALUES(?,?)",
            [(run_id, int(pk)) for pk in record_pks],
        )

    def persist_run_outputs(
        self,
        run_id: str,
        batch_id: str,
        pairs: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        clusters: list[dict[str, Any]],
        record_pks: list[int],
        created_at: str,
    ) -> None:
        """Write every derived row for a run in one transaction.

        Pairs, edges, clusters and the record snapshot become visible together.
        A crash can therefore never leave a run whose clusters exist but whose
        snapshot does not -- which would make it fall back to the live batch and
        leak records merged after the run.
        """
        with self.transaction() as conn:
            conn.execute("DELETE FROM pairs WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO pairs(run_id,pair_key,rec_a_pk,rec_b_pk,score,zone,
                     strategies_json,details_json) VALUES(?,?,?,?,?,?,?,?)""",
                [(run_id, p["pair_key"], p["rec_a_pk"], p["rec_b_pk"], p["score"],
                  p["zone"], _dumps(p["strategies"]), _dumps(p["details"]))
                 for p in pairs],
            )
            conn.execute("DELETE FROM edges WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO edges(run_id,pair_key,rec_a_pk,rec_b_pk,score,kind,
                     details_json) VALUES(?,?,?,?,?,?,?)""",
                [(run_id, e["pair_key"], e["rec_a_pk"], e["rec_b_pk"], e["score"],
                  e["kind"], _dumps(e.get("details", {}))) for e in edges],
            )
            conn.execute("DELETE FROM clusters WHERE run_id=?", (run_id,))
            conn.executemany(
                """INSERT INTO clusters(run_id,cluster_id,batch_id,member_keys_json,
                     member_pks_json,golden_json) VALUES(?,?,?,?,?,?)""",
                [(run_id, c["cluster_id"], batch_id, _dumps(c["member_keys"]),
                  _dumps(c["member_pks"]), _dumps(c["golden"])) for c in clusters],
            )
            self._write_run_snapshot(conn, run_id, record_pks, created_at)

    def backfill_run_snapshots(self) -> int:
        """Snapshot runs created before run_snapshots existed.

        Runs this only while the database has no snapshots for a run. Since the
        merge feature is new, any such run predates every merge, so the batch's
        current records are exactly what that run saw.
        """
        rows = self.query(
            """SELECT r.run_id, r.batch_id, r.created_at FROM runs r
               LEFT JOIN run_snapshots s ON s.run_id = r.run_id
               WHERE s.run_id IS NULL"""
        )
        for row in rows:
            pks = [
                int(record["record_pk"])
                for record in self.query(
                    "SELECT record_pk FROM records WHERE batch_id=?", (row["batch_id"],)
                )
            ]
            self.replace_run_snapshot(row["run_id"], pks, row["created_at"])
        return len(rows)

    def list_run_record_pks(self, run_id: str) -> list[int] | None:
        """Snapshot pks for a run, or None for legacy runs without a snapshot."""
        snapshot = self.query_one(
            "SELECT run_id FROM run_snapshots WHERE run_id=?", (run_id,)
        )
        if snapshot is None:
            return None
        return [
            int(row["record_pk"])
            for row in self.query(
                "SELECT record_pk FROM run_records WHERE run_id=? ORDER BY record_pk",
                (run_id,),
            )
        ]

    def list_run_records(self, run_id: str, batch_id: str) -> list[sqlite3.Row]:
        """Records visible to a run: its frozen set, or the batch for legacy runs."""
        snapshot = self.query_one(
            "SELECT run_id FROM run_snapshots WHERE run_id=?", (run_id,)
        )
        if snapshot is None:
            return self.list_records(batch_id)
        return self.query(
            """SELECT r.* FROM run_records rr
               JOIN records r ON r.record_pk = rr.record_pk
               WHERE rr.run_id=? ORDER BY r.source,r.record_id""",
            (run_id,),
        )

    def add_decision(
        self, batch_id: str, pair_key: str, rec_a_key: str, rec_b_key: str,
        decision: str, reviewer: str, note: str, submitted_at: str,
    ) -> int:
        cursor = self.execute(
            """INSERT INTO decisions(batch_id,pair_key,rec_a_key,rec_b_key,decision,
                 reviewer,note,submitted_at) VALUES(?,?,?,?,?,?,?,?)""",
            (batch_id, pair_key, rec_a_key, rec_b_key, decision, reviewer, note, submitted_at),
        )
        return int(cursor.lastrowid)

    def decision_history(self, batch_id: str, pair_key: str | None = None) -> list[sqlite3.Row]:
        if pair_key:
            return self.query(
                """SELECT * FROM decisions WHERE batch_id=? AND pair_key=?
                   ORDER BY submitted_at,decision_id""", (batch_id, pair_key),
            )
        return self.query(
            "SELECT * FROM decisions WHERE batch_id=? ORDER BY submitted_at,decision_id",
            (batch_id,),
        )
