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
                 timings_json,blocking_stats_json,status,decision_view_version,
                 created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id) DO UPDATE SET
                 input_rows=excluded.input_rows,bad_rows=excluded.bad_rows,
                 candidate_pairs=excluded.candidate_pairs,
                 matched_edges=excluded.matched_edges,
                 cluster_count=excluded.cluster_count,golden_count=excluded.golden_count,
                 timings_json=excluded.timings_json,
                 blocking_stats_json=excluded.blocking_stats_json,
                 status=excluded.status,
                 decision_view_version=excluded.decision_view_version""",
            (run["run_id"], run["batch_id"], run["config_hash"], run["input_rows"],
             run["bad_rows"], run["candidate_pairs"], run["matched_edges"],
             run["cluster_count"], run["golden_count"], _dumps(run["timings"]),
             _dumps(run["blocking_stats"]), run["status"],
             run.get("decision_view_version"), run["created_at"]),
        )

    def add_attempt(self, attempt: dict[str, Any]) -> None:
        self.execute(
            """INSERT INTO run_attempts(run_id,batch_id,config_hash,input_rows,
                 bad_rows,candidate_pairs,matched_edges,cluster_count,golden_count,
                 timings_json,status,decision_view_version,started_at,finished_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (attempt["run_id"], attempt["batch_id"], attempt["config_hash"],
             attempt["input_rows"], attempt["bad_rows"], attempt["candidate_pairs"],
             attempt["matched_edges"], attempt["cluster_count"], attempt["golden_count"],
             _dumps(attempt["timings"]), attempt["status"],
             attempt.get("decision_view_version"), attempt["started_at"],
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
                     strategies_json,details_json,excluded_by_decision)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                [(run_id, p["pair_key"], p["rec_a_pk"], p["rec_b_pk"], p["score"],
                  p["zone"], _dumps(p["strategies"]), _dumps(p["details"]),
                  1 if p.get("excluded_by_decision") else 0)
                 for p in pairs],
            )

    def list_pairs(
        self, run_id: str, zone: str | None = None, include_excluded: bool = True
    ) -> list[sqlite3.Row]:
        sql = "SELECT * FROM pairs WHERE run_id=?"
        args: list[Any] = [run_id]
        if not include_excluded:
            sql += " AND excluded_by_decision=0"
        if zone:
            sql += " AND zone=?"
            args.append(zone)
        sql += " ORDER BY pair_key"
        return self.query(sql, tuple(args))

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

    # -- versioned, materialized batch decision view (instant reflow) -------
    def decision_state_rows(self, batch_id: str) -> list[sqlite3.Row]:
        """Return the whole effective decision view, ordered deterministically."""
        return self.query(
            """SELECT * FROM decision_state WHERE batch_id=?
               ORDER BY rec_a_key,rec_b_key""",
            (batch_id,),
        )

    def decision_state_version(self, batch_id: str) -> int | None:
        """Current view version, or None when the view has never been written."""
        row = self.query_one(
            "SELECT MAX(view_version) AS v FROM decision_state WHERE batch_id=?",
            (batch_id,),
        )
        if row is None or row["v"] is None:
            return None
        return int(row["v"])

    def decision_state_for_pair(
        self, batch_id: str, rec_a_key: str, rec_b_key: str
    ) -> sqlite3.Row | None:
        return self.query_one(
            """SELECT * FROM decision_state
               WHERE batch_id=? AND rec_a_key=? AND rec_b_key=?""",
            (batch_id, rec_a_key, rec_b_key),
        )

    def upsert_decision_state(
        self,
        conn: sqlite3.Connection,
        batch_id: str,
        pair_key: str,
        rec_a_key: str,
        rec_b_key: str,
        decision: str,
        reviewer: str,
        note: str,
        decision_id: int,
        submitted_at: str,
        view_version: int,
    ) -> None:
        """Idempotent upsert of the single effective decision for a pair.

        The (batch_id, rec_a_key, rec_b_key) PRIMARY KEY plus this UPSERT make
        concurrent, mutually exclusive submissions collapse onto one row: the
        last committed writer wins, and no two same/different rows can coexist.
        """
        conn.execute(
            """INSERT INTO decision_state(batch_id,pair_key,rec_a_key,rec_b_key,
                 decision,reviewer,note,decision_id,submitted_at,view_version)
               VALUES(?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(batch_id,rec_a_key,rec_b_key) DO UPDATE SET
                 pair_key=excluded.pair_key, decision=excluded.decision,
                 reviewer=excluded.reviewer, note=excluded.note,
                 decision_id=excluded.decision_id,
                 submitted_at=excluded.submitted_at,
                 view_version=excluded.view_version""",
            (batch_id, pair_key, rec_a_key, rec_b_key, decision, reviewer, note,
             decision_id, submitted_at, view_version),
        )

    def replace_decision_state(
        self, conn: sqlite3.Connection, batch_id: str, rows: list[dict[str, Any]]
    ) -> int:
        """Rebuild the whole batch view from decision history; new version = 1."""
        conn.execute("DELETE FROM decision_state WHERE batch_id=?", (batch_id,))
        conn.executemany(
            """INSERT INTO decision_state(batch_id,pair_key,rec_a_key,rec_b_key,
                 decision,reviewer,note,decision_id,submitted_at,view_version)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            [(batch_id, row["pair_key"], row["rec_a_key"], row["rec_b_key"],
              row["decision"], row["reviewer"], row["note"], row["decision_id"],
              row["submitted_at"], 1) for row in rows],
        )
        return 1
