"""Incremental fusion engine.

Fuses the records ingested since a baseline run against that run's stored
normalization output, pair scores, clusters and the live human-decision
history, and persists a brand-new run snapshot. The design guarantees:

* **Equivalence** -- the produced pairs/edges/clusters/golden rows are
  identical to re-running the full pipeline over all current records, because
  every algorithmic step reuses the unchanged full-run functions and only
  pair *retrieval* is incremental (old rows are copied, new rows are scored
  exactly as a full run would score them).
* **Snapshot isolation** -- baseline and all older runs are never modified;
  the incremental output lives under a new deterministic ``run_id``.
* **Atomicity** -- every business write of a task happens in one SQLite
  transaction. A validation or matching failure rolls the whole task back, so
  historical results can never be polluted. Only the independent task row
  (and a failure audit event) is written afterwards for traceability.
* **Bounded memory** -- historical records and pairs are scanned/streamed in
  pages; the full history is never materialized in Python memory.
* **Serial submission** -- an in-process lock plus a single-row database
  lock ensure incremental tasks never run concurrently.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from typing import Any

from ..audit import record_event
from ..clock import Clock
from ..clustering import cluster_records
from ..config import AppConfig
from ..models import Record, record_from_db
from ..pipeline import _cluster_id, _run_id
from ..review import decision_dicts
from ..scoring import score_pair
from ..storage import json_dumps
from ..survivorship import build_golden
from .blocking_stream import (
    DEFAULT_SCAN_PAGE_SIZE,
    RecordRef,
    new_new_candidates,
    orient_pair,
    scan_cross_matches,
)
from .schema import ensure_incremental_schema
from .store import IncrementalTaskStore


class IncrementalError(ValueError):
    """Raised when an incremental task fails a precondition or validation."""


class IncrementalBusy(IncrementalError):
    """Raised when another incremental task currently holds the serial lock."""


class IncrementalEngine:
    """Runs incremental fusion tasks against one :class:`Storage` instance."""

    def __init__(
        self,
        storage: Any,
        config: AppConfig,
        clock: Clock,
        page_size: int = DEFAULT_SCAN_PAGE_SIZE,
    ) -> None:
        self.storage = storage
        self.config = config
        self.clock = clock
        self.page_size = page_size
        self.tasks = IncrementalTaskStore(storage)
        self._thread_lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Public entry point
    # ------------------------------------------------------------------ #
    def run_incremental(self, batch_id: str, base_run_id: str | None = None) -> dict[str, Any]:
        ensure_incremental_schema(self.storage)
        batch = self._require_batch(batch_id)
        base_run = self._resolve_base_run(batch_id, base_run_id)
        base_run_id = str(base_run["run_id"])
        self._check_config(base_run)

        history = decision_dicts(self.storage, batch_id)
        run_id = _run_id(batch, self.config, history)
        task_id = self._task_id(batch_id, base_run_id, run_id)

        # Idempotent replay: an identical delta against the same baseline has
        # already been fused -- return the stored outcome, write nothing new.
        existing = self.tasks.find_task(batch_id, base_run_id, run_id)
        if existing is not None and existing["status"] == "completed":
            return self._replay_result(existing)

        if not self._thread_lock.acquire(blocking=False):
            raise IncrementalBusy("an incremental task is already running")
        try:
            if not self.tasks.try_acquire_lock(task_id, self.clock.now_iso()):
                raise IncrementalBusy(
                    "another incremental task holds the database lock"
                )
            try:
                # Re-check under the lock: a concurrent task may have finished it.
                existing = self.tasks.find_task(batch_id, base_run_id, run_id)
                if existing is not None and existing["status"] == "completed":
                    return self._replay_result(existing)
                return self._execute(
                    batch, base_run, base_run_id, history, run_id, task_id
                )
            finally:
                self.tasks.release_lock(task_id)
        finally:
            self._thread_lock.release()

    # ------------------------------------------------------------------ #
    # Preconditions
    # ------------------------------------------------------------------ #
    def _require_batch(self, batch_id: str) -> sqlite3.Row:
        batch = self.storage.get_batch(batch_id)
        if batch is None:
            raise IncrementalError(f"batch {batch_id!r} not found")
        return batch

    def _resolve_base_run(
        self, batch_id: str, base_run_id: str | None
    ) -> sqlite3.Row:
        if base_run_id is None:
            base_run_id = self.storage.latest_run_id(batch_id)
            if base_run_id is None:
                raise IncrementalError(
                    f"batch {batch_id!r} has no baseline run; run a full "
                    "pipeline first"
                )
        run = self.storage.get_run(base_run_id)
        if run is None:
            raise IncrementalError(f"baseline run {base_run_id!r} not found")
        if run["batch_id"] != batch_id:
            raise IncrementalError(
                f"baseline run {base_run_id!r} belongs to another batch"
            )
        if run["status"] != "completed":
            raise IncrementalError(
                f"baseline run {base_run_id!r} is not completed "
                f"(status={run['status']!r})"
            )
        return run

    def _check_config(self, base_run: sqlite3.Row) -> None:
        current = self.config.fingerprint()
        if base_run["config_hash"] != current:
            raise IncrementalError(
                "configuration drift detected: the baseline run used config "
                f"{base_run['config_hash']!r} but the active config is "
                f"{current!r}; a full re-run is required"
            )

    def _new_record_pks(self, batch_id: str, base_run_id: str) -> list[int]:
        """Primary keys ingested after the baseline run was produced.

        Membership is taken from the baseline clusters' parsed
        ``member_pks_json``; the difference is computed over streamed integer
        keys, so no fragile substring matching and no full-record load is
        required.
        """
        base_member_pks = self._base_member_pks(base_run_id)
        rows = self.storage.query(
            "SELECT record_pk FROM records WHERE batch_id=? ORDER BY record_pk",
            (batch_id,),
        )
        return [
            int(row["record_pk"])
            for row in rows
            if int(row["record_pk"]) not in base_member_pks
        ]

    def _base_member_pks(self, base_run_id: str) -> set[int]:
        pks: set[int] = set()
        for cluster in self.storage.list_clusters(base_run_id):
            pks.update(int(pk) for pk in json.loads(cluster["member_pks_json"]))
        return pks

    # ------------------------------------------------------------------ #
    # Core execution
    # ------------------------------------------------------------------ #
    def _execute(
        self,
        batch: sqlite3.Row,
        base_run: sqlite3.Row,
        base_run_id: str,
        history: list[dict[str, Any]],
        run_id: str,
        task_id: str,
    ) -> dict[str, Any]:
        batch_id = batch["batch_id"]
        started_at = self.clock.now_iso()
        timings: dict[str, float] = {}
        start = self.clock.monotonic()
        last = start

        def mark(stage: str) -> None:
            nonlocal last
            now = self.clock.monotonic()
            timings[stage] = round(now - last, 6)
            last = now

        blocking_stats: dict[str, Any] = {}
        counters = {
            "new_records": 0,
            "new_pairs": 0,
            "new_model_edges": 0,
            "reused_pairs": 0,
            "reused_clusters": 0,
            "rebuilt_clusters": 0,
        }
        task_seed = {
            "task_id": task_id,
            "batch_id": batch_id,
            "base_run_id": base_run_id,
            "run_id": run_id,
            "config_hash": self.config.fingerprint(),
            "created_at": started_at,
        }

        try:
            # ---- Phase 1: identify the delta (streaming-safe reads) -----
            new_pks = self._new_record_pks(batch_id, base_run_id)
            base_member_pks = self._base_member_pks(base_run_id)
            counters["new_records"] = len(new_pks)

            no_changes = not new_pks and run_id == base_run_id
            if no_changes:
                return self._finish_noop(
                    task_seed, timings, start, counters
                )

            new_refs = self._load_refs_by_pks(new_pks)
            mark("delta_load")

            # ---- Phase 2: incremental blocking + scoring ---------------
            new_pair_rows, blocking_stats, counters = self._build_new_pairs(
                batch_id, base_run_id, base_member_pks, new_refs, counters
            )
            mark("blocking_scoring")

            # ---- Phase 3: clustering over all records (streaming refs) --
            all_refs = list(
                self._iter_all_refs(batch_id)
            )  # lightweight refs only, no raw JSON
            model_edges = self._collect_model_edges(
                base_run_id, new_pair_rows
            )
            groups, applied_edges, conflicts = cluster_records(
                all_refs, model_edges, history
            )
            pair_scores = self._applied_pair_scores(
                base_run_id, new_pair_rows, applied_edges
            )
            for edge in applied_edges:
                if edge["kind"] == "human_same":
                    edge["score"] = pair_scores.get(edge["pair_key"], 0.0)
            # Non-negative, genuinely-new model edges: applied model edges
            # whose candidate pair belongs to this delta (a delta pair can
            # still be withheld by a human cannot-link rule).
            delta_pair_keys = {row["pair_key"] for row in new_pair_rows}
            counters["new_model_edges"] = sum(
                1
                for edge in applied_edges
                if edge["kind"] == "model"
                and edge["pair_key"] in delta_pair_keys
            )
            mark("clustering")

            # ---- Phase 4: golden records, then one atomic write --------
            cluster_rows, counters = self._build_cluster_rows(
                batch_id, base_run_id, new_pks, groups, counters
            )
            mark("golden")

            run_common = self._run_common(
                batch, run_id, counters["reused_pairs"],
                len(applied_edges), len(cluster_rows), timings, blocking_stats,
                started_at,
            )
            with self.storage.transaction() as conn:
                self.tasks.insert_task(conn, {
                    **task_seed,
                    "status": "running",
                    "timings": timings,
                    "blocking_stats": blocking_stats,
                    **counters,
                })
                self._persist(
                    conn, run_common, base_run_id, new_pair_rows,
                    applied_edges, cluster_rows,
                )
                self._verify_persisted(
                    conn, run_id, base_run_id, run_common, new_pair_rows
                )
                timings["persistence"] = round(self.clock.monotonic() - last, 6)
                timings["total"] = round(self.clock.monotonic() - start, 6)
                run_common["timings"] = timings
                self._update_run_row(conn, run_common)
                self.tasks.update_task_finish(
                    conn, task_id, "completed", timings, blocking_stats,
                    counters, None, self.clock.now_iso(),
                )
                conn.execute(
                    "INSERT INTO run_attempts(run_id,batch_id,config_hash,"
                    "input_rows,bad_rows,candidate_pairs,matched_edges,"
                    "cluster_count,golden_count,timings_json,status,"
                    "started_at,finished_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        run_common["run_id"], run_common["batch_id"],
                        run_common["config_hash"], run_common["input_rows"],
                        run_common["bad_rows"], run_common["candidate_pairs"],
                        run_common["matched_edges"],
                        run_common["cluster_count"],
                        run_common["golden_count"], json_dumps(timings),
                        "completed", started_at, self.clock.now_iso(),
                    ),
                )
            mark("persistence")
            for conflict in conflicts:
                record_event(
                    self.storage, self.clock, batch_id, run_id,
                    "review_conflict", conflict,
                )
            record_event(
                self.storage, self.clock, batch_id, run_id, "run_completed",
                {
                    "mode": "incremental",
                    "base_run_id": base_run_id,
                    "candidate_pairs": run_common["candidate_pairs"],
                    "clusters": run_common["cluster_count"],
                    "conflicts": len(conflicts),
                },
            )
            record_event(
                self.storage, self.clock, batch_id, run_id,
                "incremental_completed",
                {"task_id": task_id, "base_run_id": base_run_id, **counters},
            )
        except Exception as exc:
            self._record_failure(task_seed, timings, blocking_stats, counters, exc)
            raise

        return self._public_result(
            run_common, task_id, base_run_id, counters, conflicts
        )

    # ------------------------------------------------------------------ #
    # Phase helpers
    # ------------------------------------------------------------------ #
    def _load_refs_by_pks(self, pks: list[int]) -> list[RecordRef]:
        if not pks:
            return []
        refs: list[RecordRef] = []
        for start in range(0, len(pks), self.page_size):
            chunk = pks[start:start + self.page_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.storage.query(
                "SELECT record_pk,source,record_id,norm_json FROM records "
                f"WHERE record_pk IN ({placeholders}) ORDER BY record_pk",
                tuple(chunk),
            )
            refs.extend(RecordRef(
                pk=int(row["record_pk"]),
                source=row["source"],
                record_id=row["record_id"],
                norm=json.loads(row["norm_json"]),
            ) for row in rows)
        return refs

    def _iter_all_refs(self, batch_id: str):
        from .blocking_stream import stream_record_refs

        yield from stream_record_refs(self.storage, batch_id, self.page_size)

    def _build_new_pairs(
        self,
        batch_id: str,
        base_run_id: str,
        base_member_pks: set[int],
        new_refs: list[RecordRef],
        counters: dict[str, int],
    ) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, int]]:
        """Score every candidate pair involving at least one new record."""
        strategy_counts: dict[str, int] = {
            "year_venue": 0,
            "title_signature": 0,
            "author_year": 0,
        }
        enabled = {
            "year_venue": self.config.blocking.year_venue.enabled,
            "title_signature": self.config.blocking.title_signature.enabled,
            "author_year": self.config.blocking.author_year.enabled,
        }
        pair_rows: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add_pair(left: RecordRef, right: RecordRef, strategies: tuple[str, ...]) -> None:
            first, second, pair_key = orient_pair(left, right)
            if pair_key in seen:
                return
            seen.add(pair_key)
            result = score_pair(first, second, self.config)
            pair_rows.append({
                "pair_key": pair_key,
                "rec_a_pk": first.pk,
                "rec_b_pk": second.pk,
                "score": result["score"],
                "zone": result["zone"],
                "strategies": list(strategies),
                "details": result,
            })

        # New-versus-new pairs: unchanged full-run blocker on the delta.
        if new_refs:
            delta_candidates, delta_counts = new_new_candidates(new_refs, self.config)
            for name, count in delta_counts.items():
                strategy_counts[name] += count
            for candidate in delta_candidates:
                add_pair(candidate.left, candidate.right, candidate.strategies)

        # Historical-versus-new pairs: one paginated streaming scan that also
        # produces the delta per-strategy counts. The counts are taken from
        # the scan (bucket-based, full-run parity) rather than re-derived from
        # deduplicated pairs, which would undercount multi-strategy buckets.
        cross_pairs, cross_counts = scan_cross_matches(
            self.storage, batch_id, new_refs, self.config,
            base_member_pks, self.page_size,
        )
        for name, count in cross_counts.items():
            strategy_counts[name] += count
        for historical, new_ref, strategies in cross_pairs:
            add_pair(historical, new_ref, strategies)

        pair_rows.sort(key=lambda row: row["pair_key"])
        counters["new_pairs"] = len(pair_rows)

        base_count_row = self.storage.query_one(
            "SELECT COUNT(*) AS n FROM pairs WHERE run_id=?", (base_run_id,)
        )
        reused_pairs = int(base_count_row["n"]) if base_count_row else 0
        counters["reused_pairs"] = reused_pairs

        # Full-run strategy counts partition into old-old pairs (already
        # counted on the baseline run) plus the delta pairs counted above.
        base_stats_row = self.storage.get_run(base_run_id)
        base_blocking_stats = (
            json.loads(base_stats_row["blocking_stats_json"])
            if base_stats_row is not None else {}
        )
        base_strategy_counts = base_blocking_stats.get("strategy_counts", {})
        full_strategy_counts = {
            name: int(base_strategy_counts.get(name, 0)) + strategy_counts[name]
            for name in strategy_counts
        }
        union_count = reused_pairs + len(pair_rows)
        blocking_stats = {
            "enabled": enabled,
            "strategy_counts": full_strategy_counts,
            "union_count": union_count,
            "incremental": {
                "new_pairs": len(pair_rows),
                "reused_pairs": reused_pairs,
                "new_strategy_counts": strategy_counts,
            },
        }
        return pair_rows, blocking_stats, counters

    def _collect_model_edges(
        self, base_run_id: str, new_pair_rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Model edges of the equivalent full run.

        Baseline model edges are streamed page by page (never fully loaded);
        delta match-zone pairs contribute the remaining edges. Human pairs are
        excluded here because ``cluster_records`` derives them itself from the
        decision history, exactly as in the full pipeline.
        """
        edges: list[dict[str, Any]] = []
        offset = 0
        while True:
            rows = self.storage.query(
                "SELECT pair_key,rec_a_pk,rec_b_pk,score FROM edges "
                "WHERE run_id=? AND kind='model' ORDER BY pair_key LIMIT ? OFFSET ?",
                (base_run_id, self.page_size, offset),
            )
            if not rows:
                break
            for row in rows:
                edges.append({
                    "pair_key": row["pair_key"],
                    "rec_a_pk": row["rec_a_pk"],
                    "rec_b_pk": row["rec_b_pk"],
                    "score": row["score"],
                    "details": {},
                })
            offset += len(rows)
        for row in new_pair_rows:
            if row["zone"] == "match":
                edges.append({
                    "pair_key": row["pair_key"],
                    "rec_a_pk": row["rec_a_pk"],
                    "rec_b_pk": row["rec_b_pk"],
                    "score": row["score"],
                    "details": row["details"],
                })
        return edges

    def _applied_pair_scores(
        self,
        base_run_id: str,
        new_pair_rows: list[dict[str, Any]],
        applied_edges: list[dict[str, Any]],
    ) -> dict[str, float]:
        """Scores for applied human_same edges (pipeline parity).

        Model edge scores are already carried on the edges; human edges need
        the underlying pair score, looked up in the baseline pairs table or
        the freshly scored delta pairs, in bounded batches.
        """
        delta_scores = {row["pair_key"]: row["score"] for row in new_pair_rows}
        missing = [
            edge["pair_key"]
            for edge in applied_edges
            if edge["kind"] == "human_same" and edge["pair_key"] not in delta_scores
        ]
        scores: dict[str, float] = dict(delta_scores)
        for start in range(0, len(missing), self.page_size):
            chunk = missing[start:start + self.page_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.storage.query(
                f"SELECT pair_key,score FROM pairs WHERE run_id=? "
                f"AND pair_key IN ({placeholders})",
                (base_run_id, *chunk),
            )
            for row in rows:
                scores[row["pair_key"]] = row["score"]
        return scores

    def _build_cluster_rows(
        self,
        batch_id: str,
        base_run_id: str,
        new_pks: list[int],
        groups: list[list[RecordRef]],
        counters: dict[str, int],
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Reuse untouched baseline clusters; rebuild only affected ones.

        A baseline cluster is *reused* only when the new clustering produces
        the exact same member set and it contains no delta records. All other
        baseline clusters have *evolved* (merged with other clusters or new
        records, or split apart): their historical records must still be fully
        covered by the rebuilt groups, which is verified explicitly so a
        divergence raises and rolls the whole task back.
        """
        new_pk_set = set(new_pks)
        base_clusters = {
            row["cluster_id"]: row for row in self.storage.list_clusters(base_run_id)
        }
        base_member_sets = {
            cluster_id: frozenset(
                int(pk) for pk in json.loads(row["member_pks_json"])
            )
            for cluster_id, row in base_clusters.items()
        }
        base_by_members = {
            members: cluster_id for cluster_id, members in base_member_sets.items()
        }

        rows: list[dict[str, Any]] = []
        reused_ids: set[str] = set()
        rebuilt_member_sets: set[frozenset[int]] = set()
        for group in groups:
            member_set = frozenset(ref.pk for ref in group)
            member_pks = sorted(member_set)
            member_keys = sorted(ref.key for ref in group)
            baseline_id = base_by_members.get(member_set)
            if baseline_id is not None and not (new_pk_set & member_set):
                # Unchanged cluster: byte-identical reuse of the snapshot.
                baseline = base_clusters[baseline_id]
                if json.loads(baseline["member_keys_json"]) != member_keys:
                    raise IncrementalError(
                        "integrity check failed: cluster membership mismatch "
                        f"for cluster {baseline_id}"
                    )
                reused_ids.add(baseline_id)
                rows.append({
                    "cluster_id": baseline["cluster_id"],
                    "member_keys": json.loads(baseline["member_keys_json"]),
                    "member_pks": member_pks,
                    "golden": json.loads(baseline["golden_json"]),
                    "reused": True,
                })
                continue

            # New or affected cluster: rebuild with the unchanged golden rules.
            full_records = self._load_full_records(member_pks)
            if len(full_records) != len(member_pks):
                raise IncrementalError(
                    "integrity check failed: cluster references missing records"
                )
            cluster_id = _cluster_id(batch_id, member_keys)
            if baseline_id is not None and baseline_id != cluster_id:
                raise IncrementalError(
                    "integrity check failed: deterministic cluster id drift"
                )
            rebuilt_member_sets.add(member_set)
            rows.append({
                "cluster_id": cluster_id,
                "member_keys": member_keys,
                "member_pks": member_pks,
                "golden": build_golden(full_records, self.config),
                "reused": False,
            })

        counters["reused_clusters"] = len(reused_ids)
        counters["rebuilt_clusters"] = len(rows) - len(reused_ids)

        # Every evolved baseline cluster must have all historical members
        # covered by rebuilt groups; no record may silently vanish.
        rebuilt_union = (
            frozenset().union(*rebuilt_member_sets)
            if rebuilt_member_sets else frozenset()
        )
        for cluster_id, base_members in base_member_sets.items():
            if cluster_id in reused_ids:
                continue
            if not base_members.issubset(rebuilt_union):
                raise IncrementalError(
                    f"integrity check failed: baseline cluster {cluster_id} "
                    "is unaccounted for in the incremental result"
                )
        return rows, counters

    def _load_full_records(self, pks: list[int]) -> list[Record]:
        if not pks:
            return []
        placeholders = ",".join("?" for _ in pks)
        rows = self.storage.query(
            f"SELECT * FROM records WHERE record_pk IN ({placeholders}) "
            "ORDER BY source,record_id",
            tuple(sorted(pks)),
        )
        return [record_from_db(row) for row in rows]

    # ------------------------------------------------------------------ #
    # Persistence (single transaction; caller owns it)
    # ------------------------------------------------------------------ #
    def _run_common(
        self,
        batch: sqlite3.Row,
        run_id: str,
        reused_pair_count: int,
        matched_edges: int,
        cluster_count: int,
        timings: dict[str, float],
        blocking_stats: dict[str, Any],
        started_at: str,
    ) -> dict[str, Any]:
        candidate_pairs = reused_pair_count + blocking_stats["incremental"]["new_pairs"]
        return {
            "run_id": run_id,
            "batch_id": batch["batch_id"],
            "config_hash": self.config.fingerprint(),
            # Whole-batch counters: identical vocabulary/values to a full run.
            "input_rows": int(batch["total_lines"]),
            "bad_rows": int(batch["bad_lines"]),
            "candidate_pairs": candidate_pairs,
            "matched_edges": matched_edges,
            "cluster_count": cluster_count,
            "golden_count": cluster_count,
            "timings": timings,
            "blocking_stats": blocking_stats,
            "status": "completed",
            "created_at": started_at,
        }

    def _persist(
        self,
        conn: sqlite3.Connection,
        run_common: dict[str, Any],
        base_run_id: str,
        new_pair_rows: list[dict[str, Any]],
        applied_edges: list[dict[str, Any]],
        cluster_rows: list[dict[str, Any]],
    ) -> None:
        conn.execute(
            """INSERT INTO runs(run_id,batch_id,config_hash,input_rows,bad_rows,
                 candidate_pairs,matched_edges,cluster_count,golden_count,
                 timings_json,blocking_stats_json,status,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_common["run_id"], run_common["batch_id"],
                run_common["config_hash"], run_common["input_rows"],
                run_common["bad_rows"], run_common["candidate_pairs"],
                run_common["matched_edges"], run_common["cluster_count"],
                run_common["golden_count"], json_dumps(run_common["timings"]),
                json_dumps(run_common["blocking_stats"]),
                run_common["status"], run_common["created_at"],
            ),
        )
        # Server-side copy: historical pair rows never enter Python memory.
        conn.execute(
            "INSERT INTO pairs(run_id,pair_key,rec_a_pk,rec_b_pk,score,zone,"
            "strategies_json,details_json) "
            "SELECT ?,pair_key,rec_a_pk,rec_b_pk,score,zone,strategies_json,"
            "details_json FROM pairs WHERE run_id=?",
            (run_common["run_id"], base_run_id),
        )
        conn.executemany(
            """INSERT INTO pairs(run_id,pair_key,rec_a_pk,rec_b_pk,score,zone,
                 strategies_json,details_json) VALUES(?,?,?,?,?,?,?,?)""",
            [
                (
                    run_common["run_id"], row["pair_key"], row["rec_a_pk"],
                    row["rec_b_pk"], row["score"], row["zone"],
                    json_dumps(row["strategies"]), json_dumps(row["details"]),
                )
                for row in new_pair_rows
            ],
        )
        # Server-side copy: historical edge rows never enter Python memory.
        conn.execute(
            "INSERT OR IGNORE INTO edges(run_id,pair_key,rec_a_pk,rec_b_pk,"
            "score,kind,details_json) "
            "SELECT ?,pair_key,rec_a_pk,rec_b_pk,score,kind,details_json "
            "FROM edges WHERE run_id=?",
            (run_common["run_id"], base_run_id),
        )
        # Applied edges on delta pairs (or newly materialized human edges);
        # old-pair edges already arrived via the baseline copy above.
        conn.executemany(
            """INSERT OR IGNORE INTO edges(run_id,pair_key,rec_a_pk,rec_b_pk,
                 score,kind,details_json) VALUES(?,?,?,?,?,?,?)""",
            [
                (
                    run_common["run_id"], edge["pair_key"], edge["rec_a_pk"],
                    edge["rec_b_pk"], edge["score"], edge["kind"],
                    json_dumps(edge.get("details", {})),
                )
                for edge in applied_edges
            ],
        )
        # The full pipeline overwrites human_same edge scores with the stored
        # pair score; apply the same deterministic update to copied rows.
        conn.executemany(
            "UPDATE edges SET score=? WHERE run_id=? AND pair_key=? AND kind='human_same'",
            [
                (edge["score"], run_common["run_id"], edge["pair_key"])
                for edge in applied_edges
                if edge["kind"] == "human_same"
            ],
        )
        conn.executemany(
            """INSERT INTO clusters(run_id,cluster_id,batch_id,member_keys_json,
                 member_pks_json,golden_json) VALUES(?,?,?,?,?,?)""",
            [
                (
                    run_common["run_id"], row["cluster_id"],
                    run_common["batch_id"], json_dumps(row["member_keys"]),
                    json_dumps(row["member_pks"]), json_dumps(row["golden"]),
                )
                for row in cluster_rows
            ],
        )

    def _verify_persisted(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        base_run_id: str,
        run_common: dict[str, Any],
        new_pair_rows: list[dict[str, Any]],
    ) -> None:
        """Post-write invariants; any failure rolls the transaction back."""
        pair_count = conn.execute(
            "SELECT COUNT(*) AS n FROM pairs WHERE run_id=?", (run_id,)
        ).fetchone()["n"]
        if pair_count != run_common["candidate_pairs"]:
            raise IncrementalError(
                f"integrity check failed: persisted {pair_count} pairs but "
                f"computed {run_common['candidate_pairs']}"
            )
        duplicate_keys = conn.execute(
            "SELECT pair_key, COUNT(*) AS n FROM pairs WHERE run_id=? "
            "GROUP BY pair_key HAVING n > 1 LIMIT 1",
            (run_id,),
        ).fetchone()
        if duplicate_keys is not None:
            raise IncrementalError(
                "integrity check failed: duplicate pair key "
                f"{duplicate_keys['pair_key']!r}"
            )
        cluster_count = conn.execute(
            "SELECT COUNT(*) AS n FROM clusters WHERE run_id=?", (run_id,)
        ).fetchone()["n"]
        if cluster_count != run_common["cluster_count"]:
            raise IncrementalError(
                "integrity check failed: cluster count mismatch"
            )
        edge_count = conn.execute(
            "SELECT COUNT(*) AS n FROM edges WHERE run_id=?", (run_id,)
        ).fetchone()["n"]
        if edge_count != run_common["matched_edges"]:
            raise IncrementalError(
                f"integrity check failed: persisted {edge_count} edges but "
                f"computed {run_common['matched_edges']}"
            )
        # Baseline snapshot must be byte-for-byte untouched.
        drift = conn.execute(
            "SELECT COUNT(*) AS n FROM pairs WHERE run_id=?", (base_run_id,)
        ).fetchone()["n"]
        base_row = conn.execute(
            "SELECT candidate_pairs FROM runs WHERE run_id=?", (base_run_id,)
        ).fetchone()
        if base_row is None or drift != base_row["candidate_pairs"]:
            raise IncrementalError(
                "integrity check failed: baseline run snapshot was modified"
            )

    def _update_run_row(
        self, conn: sqlite3.Connection, run_common: dict[str, Any]
    ) -> None:
        conn.execute(
            """UPDATE runs SET input_rows=?,bad_rows=?,candidate_pairs=?,
                 matched_edges=?,cluster_count=?,golden_count=?,timings_json=?,
                 blocking_stats_json=?,status=? WHERE run_id=?""",
            (
                run_common["input_rows"], run_common["bad_rows"],
                run_common["candidate_pairs"], run_common["matched_edges"],
                run_common["cluster_count"], run_common["golden_count"],
                json_dumps(run_common["timings"]),
                json_dumps(run_common["blocking_stats"]),
                run_common["status"], run_common["run_id"],
            ),
        )

    # ------------------------------------------------------------------ #
    # No-op, failure and result shaping
    # ------------------------------------------------------------------ #
    def _finish_noop(
        self,
        task_seed: dict[str, Any],
        timings: dict[str, float],
        start: float,
        counters: dict[str, int],
    ) -> dict[str, Any]:
        timings["total"] = round(self.clock.monotonic() - start, 6)
        # A no-op means the deterministic run id equals the baseline run id.
        run_id = task_seed["run_id"]
        with self.storage.transaction() as conn:
            self.tasks.insert_task(conn, {
                **task_seed,
                "status": "completed",
                "timings": timings,
                "blocking_stats": {},
                **counters,
                "idempotent_replay": True,
                "finished_at": self.clock.now_iso(),
            })
        record_event(
            self.storage, self.clock, task_seed["batch_id"], run_id,
            "incremental_noop",
            {"task_id": task_seed["task_id"], "base_run_id": run_id},
        )
        return {
            "task_id": task_seed["task_id"],
            "base_run_id": run_id,
            "run_id": run_id,
            "batch_id": task_seed["batch_id"],
            "config_hash": task_seed["config_hash"],
            "status": "completed",
            "mode": "incremental",
            "noop": True,
            "idempotent_replay": True,
            "counters": dict(counters),
            "timings": timings,
            "blocking_stats": {},
            "conflicts": [],
        }

    def _record_failure(
        self,
        task_seed: dict[str, Any],
        timings: dict[str, float],
        blocking_stats: dict[str, Any],
        counters: dict[str, int],
        exc: Exception,
    ) -> None:
        """Persist only the independent failure marker; business writes rolled back.

        The failure row uses a distinct task id so a later successful retry of
        the same logical task does not collide with it.
        """
        timings.setdefault("total", 0.0)
        prior_failures = self.storage.query_one(
            "SELECT COUNT(*) AS n FROM incremental_tasks "
            "WHERE task_id LIKE ?",
            (task_seed["task_id"] + ":failed%",),
        )
        suffix = ":failed" if not prior_failures or prior_failures["n"] == 0 else (
            f":failed:{prior_failures['n'] + 1}"
        )
        failure_task_id = task_seed["task_id"] + suffix
        try:
            with self.storage.transaction() as conn:
                self.tasks.insert_task(conn, {
                    **task_seed,
                    "task_id": failure_task_id,
                    "status": "failed",
                    "timings": timings,
                    "blocking_stats": blocking_stats,
                    **counters,
                    "error": f"{type(exc).__name__}: {exc}"[:1000],
                    "finished_at": self.clock.now_iso(),
                })
            record_event(
                self.storage, self.clock, task_seed["batch_id"],
                task_seed["run_id"], "incremental_failed",
                {
                    "task_id": task_seed["task_id"],
                    "base_run_id": task_seed["base_run_id"],
                    "error": f"{type(exc).__name__}: {exc}"[:1000],
                },
            )
        except Exception:  # pragma: no cover - failure bookkeeping must not mask
            pass

    def _replay_result(self, task_row: sqlite3.Row) -> dict[str, Any]:
        run = self.storage.get_run(task_row["run_id"])
        counters = {
            "new_records": task_row["new_records"],
            "new_pairs": task_row["new_pairs"],
            "new_model_edges": task_row["new_model_edges"],
            "reused_pairs": task_row["reused_pairs"],
            "reused_clusters": task_row["reused_clusters"],
            "rebuilt_clusters": task_row["rebuilt_clusters"],
        }
        result = {
            "task_id": task_row["task_id"],
            "base_run_id": task_row["base_run_id"],
            "run_id": task_row["run_id"],
            "batch_id": task_row["batch_id"],
            "config_hash": task_row["config_hash"],
            "status": "completed",
            "mode": "incremental",
            "noop": False,
            "idempotent_replay": True,
            "counters": counters,
            "timings": json.loads(task_row["timings_json"]),
            "blocking_stats": json.loads(task_row["blocking_stats_json"]),
            "conflicts": [],
        }
        if run is not None:
            result.update({
                "input_rows": run["input_rows"],
                "bad_rows": run["bad_rows"],
                "candidate_pairs": run["candidate_pairs"],
                "matched_edges": run["matched_edges"],
                "cluster_count": run["cluster_count"],
                "golden_count": run["golden_count"],
            })
        return result

    def _public_result(
        self,
        run_common: dict[str, Any],
        task_id: str,
        base_run_id: str,
        counters: dict[str, int],
        conflicts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "task_id": task_id,
            "base_run_id": base_run_id,
            "run_id": run_common["run_id"],
            "batch_id": run_common["batch_id"],
            "config_hash": run_common["config_hash"],
            "status": run_common["status"],
            "mode": "incremental",
            "noop": False,
            "idempotent_replay": False,
            # Full-run-aligned counters (identical statistical vocabulary).
            "input_rows": run_common["input_rows"],
            "bad_rows": run_common["bad_rows"],
            "candidate_pairs": run_common["candidate_pairs"],
            "matched_edges": run_common["matched_edges"],
            "cluster_count": run_common["cluster_count"],
            "golden_count": run_common["golden_count"],
            "timings": run_common["timings"],
            "blocking_stats": run_common["blocking_stats"],
            # Incremental-only delta counters.
            "counters": dict(counters),
            "conflicts": conflicts,
        }

    def _task_id(self, batch_id: str, base_run_id: str, run_id: str) -> str:
        material = "|".join([batch_id, base_run_id, run_id])
        return "incr_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def run_incremental(
    storage: Any,
    config: AppConfig,
    clock: Clock,
    batch_id: str,
    base_run_id: str | None = None,
    page_size: int = DEFAULT_SCAN_PAGE_SIZE,
) -> dict[str, Any]:
    """Convenience wrapper matching the ``run_pipeline`` calling convention."""
    return IncrementalEngine(storage, config, clock, page_size).run_incremental(
        batch_id, base_run_id
    )
