"""Batch ingestion with per-line validation and a dead-letter queue.

Besides creating batches, this module supports *merging* a JSONL payload into
an existing batch. Merging is keyed by the business identifier
``(source, record_id)``: identical identifiers with identical content are
skipped, identical identifiers with different content reject the whole merge
with a conflict report, and new identifiers are appended. All merge writes
happen in one transaction, so a failure (including a process kill) leaves both
records and batch counters exactly as they were before the merge.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from pydantic import ValidationError

from .clock import Clock
from .config import AppConfig
from .models import BATCH_ID_RE, RawRecord
from .normalize import NormalizationError, Normalizer
from .storage import MergeInProgress, Storage, json_dumps

_BATCH_ID_RE = re.compile(BATCH_ID_RE)


class IngestError(ValueError):
    """Raised for request-level ingestion failures (not per-line failures)."""


class MergeConflictError(IngestError):
    """A merge would overwrite a differently-shaped existing record.

    The merge is rejected wholesale; ``conflicts`` lists every offending
    identifier so a caller can reconcile the source before retrying.
    """

    def __init__(self, batch_id: str, conflicts: list[dict[str, Any]]) -> None:
        self.batch_id = batch_id
        self.conflicts = conflicts
        super().__init__(
            f"merge into batch {batch_id!r} rejected: "
            f"{len(conflicts)} identifier conflict(s)"
        )


def derive_batch_id(payload: bytes) -> str:
    """Content-derived default ID: re-posting identical JSONL is idempotent."""
    return "batch_" + hashlib.sha256(payload).hexdigest()[:16]


def _validation_reason(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        pieces = []
        for error in exc.errors():
            location = ".".join(str(item) for item in error.get("loc", ()))
            pieces.append(f"{location or 'record'}: {error.get('msg', 'invalid')}")
        return "schema: " + "; ".join(pieces)[:500]
    if isinstance(exc, json.JSONDecodeError):
        return f"json: line {exc.lineno} column {exc.colno}: {exc.msg}"[:500]
    if isinstance(exc, NormalizationError):
        return f"normalize: {exc}"[:500]
    return f"{type(exc).__name__}: {exc}"[:500]


def _record_row(raw: RawRecord, norm: dict[str, Any], line_no: int) -> dict[str, Any]:
    """The persistence row shared by first ingest and merge append."""
    return {
        "source": raw.source.upper(),
        "record_id": raw.record_id,
        "line_no": line_no,
        "raw": raw.model_dump(mode="json"),
        "norm": norm,
        "title": norm["title"],
        "authors": norm["authors"],
        "venue": norm["venue"],
        "year": norm["year"],
        "volume": norm["volume"],
        "issue": norm["issue"],
        "pages": norm["pages"],
        "doi": norm["doi"],
        "gold_cluster": raw.gold_cluster,
    }


def _record_identity(row: Any) -> tuple[str, str]:
    """Business key used for merge idempotency: (source, record_id)."""
    return str(row["source"]).upper(), str(row["record_id"])


def batch_content_hash(storage: Storage, batch_id: str, conn: Any = None) -> str:
    """Hash the batch's current full content, independent of file boundaries.

    A merge may arrive as several payloads (or out of order), so the batch
    identity cannot be the hash of the last request body. This canonicalizes
    every currently stored record and distinct dead letter, then hashes them.
    Line numbers and auto-increment pks are deliberately excluded: they are
    physical artifacts and would make an otherwise identical batch hash
    differently after a no-op merge.
    """
    connection = conn if conn is not None else storage.conn
    records = connection.execute(
        """SELECT source, record_id, raw_json FROM records
           WHERE batch_id=? ORDER BY source, record_id""",
        (batch_id,),
    ).fetchall()
    dead = connection.execute(
        """SELECT reason, raw_text FROM dead_letters WHERE batch_id=?
           ORDER BY reason, raw_text""",
        (batch_id,),
    ).fetchall()
    material = json.dumps(
        {
            "records": [[row["source"], row["record_id"], row["raw_json"]] for row in records],
            "dead_letters": [[row["reason"], row["raw_text"]] for row in dead],
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def ingest_jsonl(
    storage: Storage,
    config: AppConfig,
    clock: Clock,
    payload: str | bytes,
    batch_id: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """Validate, normalize and persist a JSONL batch.

    Bad lines never abort the batch; each becomes a dead letter. Re-posting an
    identical payload returns the original counts without writing new records.
    """
    if isinstance(payload, str):
        raw_bytes = payload.encode("utf-8")
        text = payload
    else:
        raw_bytes = bytes(payload)
        try:
            text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise IngestError(f"payload must be UTF-8: {exc}") from exc
    if not text.strip():
        raise IngestError("payload is empty")

    chosen_id = (batch_id or "").strip() or derive_batch_id(raw_bytes)
    if not _BATCH_ID_RE.fullmatch(chosen_id):
        raise IngestError("batch_id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")

    input_hash = hashlib.sha256(raw_bytes).hexdigest()
    # Share the per-batch guard with merge_jsonl: a standalone re-ingest into an
    # existing batch and a merge must not interleave their read-modify-write of
    # batch counters and input_hash.
    with storage.batch_merge_guard(chosen_id):
        existing = storage.get_batch(chosen_id)
        if existing is not None and existing["input_hash"] == input_hash:
            # Legacy idempotency: the batch's stored content hash still equals
            # this standalone payload's hash, so nothing can have been merged
            # since.
            return _unchanged_result(existing)
        if existing is not None and existing["ingest_hash"] == input_hash:
            # Same starting payload re-posted after a merge. The batch's
            # input_hash now covers the merged content, but records are still
            # keyed by (source, record_id), so this call is a no-op too.
            return _unchanged_result(existing)

        normalizer = Normalizer(config.normalization)
        now = clock.now_iso()
        inserted = duplicates = dead_added = 0
        physical_lines = 0
        bootstrap = {
            "batch_id": chosen_id,
            "name": name,
            "input_hash": input_hash,
            "ingest_hash": input_hash,
            "revision": int(existing["revision"]) if existing else 0,
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
            "total_lines": int(existing["total_lines"]) if existing else 0,
            "valid_lines": int(existing["valid_lines"]) if existing else 0,
            "bad_lines": int(existing["bad_lines"]) if existing else 0,
            "status": "ingested",
        }
        storage.save_batch(bootstrap)
        for line_no, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            physical_lines += 1
            try:
                parsed = json.loads(line)
                if not isinstance(parsed, dict):
                    raise ValueError("top-level JSON value must be an object")
                raw = RawRecord.model_validate(parsed)
                norm = normalizer.normalize(raw)
            except (json.JSONDecodeError, ValidationError, ValueError, NormalizationError) as exc:
                storage.insert_dead_letter(chosen_id, line_no, _validation_reason(exc), line)
                dead_added += 1
                continue

            row = _record_row(raw, norm, line_no)
            if storage.insert_record(chosen_id, row):
                inserted += 1
            else:
                duplicates += 1

        valid_count = storage.count_records(chosen_id)
        bad_count = storage.count_dead_letters(chosen_id)
        total_lines = (int(existing["total_lines"]) if existing else 0) + physical_lines
        batch = {
            "batch_id": chosen_id,
            "name": name if name is not None else (existing["name"] if existing else None),
            "input_hash": input_hash,
            "ingest_hash": input_hash,
            "revision": (int(existing["revision"]) if existing else 0) + 1,
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
            "total_lines": total_lines,
            "valid_lines": valid_count,
            "bad_lines": bad_count,
            "status": "ingested",
        }
        storage.save_batch(batch)
    return _ingest_result(batch, created=existing is None, inserted=inserted,
                          duplicates=duplicates, dead_added=dead_added)


# Columns added for merge bookkeeping; they must never leak into the public
# /batches response, whose field set is part of the stable API contract.
_INTERNAL_BATCH_FIELDS = ("ingest_hash", "revision")


def _public_batch(batch: dict[str, Any] | Any) -> dict[str, Any]:
    payload = dict(batch)
    for field in _INTERNAL_BATCH_FIELDS:
        payload.pop(field, None)
    return payload


def _unchanged_result(existing: Any) -> dict[str, Any]:
    result = _public_batch(existing)
    result.update({"created": False, "inserted": 0, "duplicates": 0, "dead_added": 0})
    return result


def _ingest_result(
    batch: dict[str, Any], *, created: bool, inserted: int,
    duplicates: int, dead_added: int,
) -> dict[str, Any]:
    result = _public_batch(batch)
    result.update({
        "created": created,
        "inserted": inserted,
        "duplicates": duplicates,
        "dead_added": dead_added,
    })
    return result


def merge_jsonl(
    storage: Storage,
    config: AppConfig,
    clock: Clock,
    payload: str | bytes,
    batch_id: str,
) -> dict[str, Any]:
    """Merge a JSONL payload into an existing batch, idempotently.

    Identity is ``(source, record_id)``. A row whose identifier already exists
    with byte-identical canonical raw JSON is a duplicate and is skipped; the
    same identifier with different content rejects the entire merge (the
    caller gets every conflict in ``MergeConflictError.conflicts``). Rows with
    new identifiers are appended.

    All writes happen inside one ``BEGIN IMMEDIATE`` transaction. The
    read-compare-write sequence is therefore atomic: an exception rolls
    everything back, and a process killed mid-merge leaves only an uncommitted
    WAL transaction that SQLite discards. The per-batch guard makes a second
    in-flight merge of the same batch fail fast with ``MergeInProgress``.
    """
    if isinstance(payload, str):
        text = payload
    else:
        try:
            text = bytes(payload).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise IngestError(f"payload must be UTF-8: {exc}") from exc
    if not text.strip():
        raise IngestError("payload is empty")
    # The guard is taken before the batch state is read and held until the
    # write transaction commits. Two overlapping merges for the same batch
    # therefore cannot both observe the same starting revision: the loser gets
    # an explicit conflict (or MergeInProgress), never a partial write.
    with storage.batch_merge_guard(batch_id):
        opening = storage.get_batch(batch_id)
        if opening is None:
            raise LookupError(f"batch {batch_id!r} not found")
        opening_revision = int(opening["revision"])

        normalizer = Normalizer(config.normalization)
        # Parse and normalize before opening the write transaction. Nothing is
        # persisted here; a validation-heavy payload only extends this batch's
        # critical section, never the global DB lock.
        parsed_rows: list[tuple[int, dict[str, Any]]] = []  # (physical index, row)
        dead_lines: list[tuple[int, str, str]] = []  # (physical index, reason, text)
        physical_index = 0
        for line_no, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            physical_index += 1
            try:
                parsed = json.loads(line)
                if not isinstance(parsed, dict):
                    raise ValueError("top-level JSON value must be an object")
                raw = RawRecord.model_validate(parsed)
                norm = normalizer.normalize(raw)
            except (json.JSONDecodeError, ValidationError, ValueError, NormalizationError) as exc:
                dead_lines.append((physical_index, _validation_reason(exc), line))
                continue
            parsed_rows.append((physical_index, _record_row(raw, norm, line_no)))

        now = clock.now_iso()
        with storage.merge_transaction() as conn:
            # Recheck under the write lock. The guard already excludes merges
            # within this process; this catches a changed revision from any
            # other writer and turns it into an explicit conflict.
            existing_batch = conn.execute(
                "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if existing_batch is None:
                raise LookupError(f"batch {batch_id!r} not found")
            if int(existing_batch["revision"]) != opening_revision:
                raise MergeConflictError(
                    batch_id,
                    [{
                        "reason": "concurrent_merge",
                        "expected_revision": opening_revision,
                        "current_revision": int(existing_batch["revision"]),
                    }],
                )
            previous_input_hash = existing_batch["input_hash"]

            existing_by_key: dict[tuple[str, str], str] = {
                _record_identity(row): row["raw_json"]
                for row in conn.execute(
                    "SELECT source, record_id, raw_json FROM records WHERE batch_id=?",
                    (batch_id,),
                ).fetchall()
            }
            existing_dead = {
                (row["reason"], row["raw_text"])
                for row in conn.execute(
                    "SELECT reason, raw_text FROM dead_letters WHERE batch_id=?",
                    (batch_id,),
                ).fetchall()
            }

            inserted = duplicates = 0
            pending: list[tuple[dict[str, Any], int]] = []  # (row, physical index)
            conflicts: list[dict[str, Any]] = []
            for physical, row in parsed_rows:
                key = _record_identity(row)
                raw_json = json_dumps(row["raw"])
                if key in existing_by_key:
                    if existing_by_key[key] == raw_json:
                        duplicates += 1
                        continue
                    conflicts.append({
                        "source": key[0],
                        "record_id": key[1],
                        "line_no": row["line_no"],
                        "reason": "identifier_conflict",
                        "existing": json.loads(existing_by_key[key]),
                        "incoming": row["raw"],
                    })
                    continue
                pending.append((row, physical))
                existing_by_key[key] = raw_json

            if conflicts:
                raise MergeConflictError(batch_id, conflicts)

            base_line = storage.max_line_no(batch_id, conn)
            dead_added = 0
            for row, physical in pending:
                storage.insert_record(
                    batch_id, {**row, "line_no": base_line + physical}, conn=conn
                )
                inserted += 1
            for physical, reason, raw_text in dead_lines:
                if (reason, raw_text) in existing_dead:
                    continue  # content-idempotent: re-merging the same bad line
                storage.insert_dead_letter(
                    batch_id, base_line + physical, reason, raw_text, conn=conn
                )
                existing_dead.add((reason, raw_text))
                dead_added += 1

            updated_at = now
            total_lines = int(existing_batch["total_lines"]) + physical_index
            valid_lines = storage.count_records(batch_id, conn)
            bad_lines = storage.count_dead_letters(batch_id, conn)
            input_hash = batch_content_hash(storage, batch_id, conn)
            batch = {
                "batch_id": batch_id,
                "name": None,
                "input_hash": input_hash,
                "ingest_hash": None,
                "revision": int(existing_batch["revision"]) + 1,
                "created_at": existing_batch["created_at"],
                "updated_at": updated_at,
                "total_lines": total_lines,
                "valid_lines": valid_lines,
                "bad_lines": bad_lines,
                "status": existing_batch["status"],
            }
            storage.save_batch(batch, conn=conn)
            conn.execute(
                """INSERT INTO batch_merges(batch_id,input_hash_before,input_hash_after,
                     inserted,duplicates,dead_added,total_lines,valid_lines,bad_lines,
                     created_at) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (batch_id, previous_input_hash, input_hash, inserted,
                 duplicates, dead_added, total_lines, valid_lines, bad_lines, now),
            )
            conn.execute(
                """INSERT INTO audit_events(run_id,batch_id,event_type,detail_json,created_at)
                   VALUES(NULL,?,?,?,?)""",
                (batch_id, "batch_merged", json_dumps({
                    "inserted": inserted, "duplicates": duplicates,
                    "dead_added": dead_added, "total_lines": total_lines,
                    "valid_lines": valid_lines, "bad_lines": bad_lines,
                    "input_hash_before": previous_input_hash,
                    "input_hash_after": input_hash,
                }), now),
            )

    result = {
        "batch_id": batch_id,
        "name": existing_batch["name"],
        "input_hash": input_hash,
        "created_at": existing_batch["created_at"],
        "updated_at": updated_at,
        "total_lines": total_lines,
        "valid_lines": valid_lines,
        "bad_lines": bad_lines,
        "status": existing_batch["status"],
        "created": False,
        "merged": True,
        "inserted": inserted,
        "duplicates": duplicates,
        "dead_added": dead_added,
        "conflicts": [],
    }
    return result