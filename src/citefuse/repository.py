"""Repository mixin for pipeline runs, candidate pairs, clusters and reviews."""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable


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

    # -- versioned per-batch decision view ("instant reflux view") ----------
    def current_decision_version(self, batch_id: str) -> int | None:
        row = self.query_one(
            "SELECT current_version FROM decision_view_versions WHERE batch_id=?",
            (batch_id,),
        )
        return int(row["current_version"]) if row else None

    def list_decision_view(
        self, batch_id: str, view_version: int
    ) -> list[sqlite3.Row]:
        return self.query(
            """SELECT * FROM decision_views WHERE batch_id=? AND view_version=?
               ORDER BY pair_key""",
            (batch_id, view_version),
        )

    def count_decision_view(self, batch_id: str, view_version: int) -> int:
        row = self.query_one(
            "SELECT COUNT(*) AS n FROM decision_views WHERE batch_id=? AND view_version=?",
            (batch_id, view_version),
        )
        return int(row["n"]) if row else 0

    def replace_decision_view(
        self,
        batch_id: str,
        view_version: int,
        rows: list[dict[str, Any]],
        connection: sqlite3.Connection | None = None,
    ) -> None:
        """Persist one immutable, versioned effective-decision snapshot.

        The (batch_id, view_version, pair_key) primary key guarantees that a
        pair appears at most once per version even under concurrent writers.
        """
        sql = (
            "INSERT INTO decision_views(batch_id,view_version,pair_key,rec_a_key,"
            "rec_b_key,decision,decision_id,reviewer,note,submitted_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)"
        )
        payload = [
            (batch_id, view_version, item["pair_key"], item["rec_a_key"],
             item["rec_b_key"], item["decision"], item["decision_id"],
             item["reviewer"], item["note"], item["submitted_at"])
            for item in rows
        ]
        if connection is not None:
            connection.execute(
                "INSERT INTO decision_view_versions(batch_id,current_version)"
                " VALUES(?,?) ON CONFLICT(batch_id) DO UPDATE SET"
                " current_version=excluded.current_version",
                (batch_id, view_version),
            )
            connection.executemany(sql, payload)
            return
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO decision_view_versions(batch_id,current_version)"
                " VALUES(?,?) ON CONFLICT(batch_id) DO UPDATE SET"
                " current_version=excluded.current_version",
                (batch_id, view_version),
            )
            conn.executemany(sql, payload)

    def insert_decision_and_rebuild_view(
        self,
        batch_id: str,
        decision: dict[str, Any],
        view_version: int,
        view_builder: Callable[[], list[dict[str, Any]]],
    ) -> int:
        """Atomically append a decision and publish the next view version.

        One transaction means reviewers can never observe a decision without
        its view or a view whose version pointer points at missing rows. The
        ``view_builder`` runs inside the transaction and must re-read the
        history (which then includes the just-inserted row).
        """
        with self.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO decisions(batch_id,pair_key,rec_a_key,rec_b_key,
                     decision,reviewer,note,submitted_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (batch_id, decision["pair_key"], decision["rec_a_key"],
                 decision["rec_b_key"], decision["decision"], decision["reviewer"],
                 decision["note"], decision["submitted_at"]),
            )
            decision_id = int(cursor.lastrowid)
            view_rows = view_builder()
            conn.execute(
                "INSERT INTO decision_view_versions(batch_id,current_version)"
                " VALUES(?,?) ON CONFLICT(batch_id) DO UPDATE SET"
                " current_version=excluded.current_version",
                (batch_id, view_version),
            )
            conn.executemany(
                "INSERT INTO decision_views(batch_id,view_version,pair_key,rec_a_key,"
                "rec_b_key,decision,decision_id,reviewer,note,submitted_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                [(batch_id, view_version, item["pair_key"], item["rec_a_key"],
                  item["rec_b_key"], item["decision"], item["decision_id"],
                  item["reviewer"], item["note"], item["submitted_at"])
                 for item in view_rows],
            )
        return decision_id
