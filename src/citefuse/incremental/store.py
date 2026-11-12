"""Persistence helpers for incremental task metadata and the serial task lock."""
from __future__ import annotations

import sqlite3
from typing import Any

from ..storage import json_dumps


class IncrementalTaskStore:
    """Data access for ``incremental_tasks`` / ``incremental_locks``.

    The class holds no state of its own; it composes the shared
    :class:`~citefuse.storage.Storage` connection and its re-entrant lock.
    """

    def __init__(self, storage: Any) -> None:
        self.storage = storage

    # -- task rows -----------------------------------------------------------
    def insert_task(
        self, conn: sqlite3.Connection, task: dict[str, Any]
    ) -> None:
        conn.execute(
            """INSERT INTO incremental_tasks(task_id,batch_id,base_run_id,run_id,
                 config_hash,status,new_records,new_pairs,new_model_edges,
                 reused_pairs,reused_clusters,rebuilt_clusters,idempotent_replay,
                 timings_json,blocking_stats_json,error,created_at,finished_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                task["task_id"], task["batch_id"], task["base_run_id"],
                task["run_id"], task["config_hash"], task["status"],
                int(task.get("new_records", 0)), int(task.get("new_pairs", 0)),
                int(task.get("new_model_edges", 0)),
                int(task.get("reused_pairs", 0)),
                int(task.get("reused_clusters", 0)),
                int(task.get("rebuilt_clusters", 0)),
                1 if task.get("idempotent_replay") else 0,
                json_dumps(task.get("timings", {})),
                json_dumps(task.get("blocking_stats", {})),
                task.get("error"), task["created_at"], task.get("finished_at"),
            ),
        )

    def update_task_finish(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        status: str,
        timings: dict[str, Any],
        blocking_stats: dict[str, Any],
        counters: dict[str, int],
        error: str | None,
        finished_at: str,
    ) -> None:
        conn.execute(
            """UPDATE incremental_tasks SET status=?, timings_json=?,
                 blocking_stats_json=?, new_records=?, new_pairs=?,
                 new_model_edges=?, reused_pairs=?, reused_clusters=?,
                 rebuilt_clusters=?, error=?, finished_at=?
               WHERE task_id=?""",
            (
                status, json_dumps(timings), json_dumps(blocking_stats),
                int(counters.get("new_records", 0)),
                int(counters.get("new_pairs", 0)),
                int(counters.get("new_model_edges", 0)),
                int(counters.get("reused_pairs", 0)),
                int(counters.get("reused_clusters", 0)),
                int(counters.get("rebuilt_clusters", 0)),
                error, finished_at, task_id,
            ),
        )

    def get_task(self, task_id: str) -> sqlite3.Row | None:
        return self.storage.query_one(
            "SELECT * FROM incremental_tasks WHERE task_id=?", (task_id,)
        )

    def find_task(
        self, batch_id: str, base_run_id: str, run_id: str
    ) -> sqlite3.Row | None:
        return self.storage.query_one(
            """SELECT * FROM incremental_tasks
               WHERE batch_id=? AND base_run_id=? AND run_id=?
               ORDER BY rowid LIMIT 1""",
            (batch_id, base_run_id, run_id),
        )

    def list_tasks(self, batch_id: str | None = None) -> list[sqlite3.Row]:
        if batch_id:
            return self.storage.query(
                "SELECT * FROM incremental_tasks WHERE batch_id=? "
                "ORDER BY rowid",
                (batch_id,),
            )
        return self.storage.query(
            "SELECT * FROM incremental_tasks ORDER BY rowid"
        )

    # -- advisory lock -------------------------------------------------------
    def try_acquire_lock(
        self, task_id: str, acquired_at: str
    ) -> bool:
        """Atomically take the single-row lock. True on success."""
        try:
            self.storage.execute(
                "INSERT INTO incremental_locks(lock_id,task_id,acquired_at) "
                "VALUES(1,?,?)",
                (task_id, acquired_at),
            )
        except sqlite3.IntegrityError:
            return False
        return True

    def release_lock(self, task_id: str) -> None:
        # Only the owner can release it; a stale row is never silently stolen.
        self.storage.execute(
            "DELETE FROM incremental_locks WHERE lock_id=1 AND task_id=?",
            (task_id,),
        )

    def current_lock(self) -> sqlite3.Row | None:
        return self.storage.query_one(
            "SELECT * FROM incremental_locks WHERE lock_id=1"
        )
